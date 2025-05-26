package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.f4b6a3.uuid.UuidCreator
import io.github.gabrielshanahan.structmess.coroutine.CancellationRequestedException
import io.github.gabrielshanahan.structmess.coroutine.CooperationContext
import io.github.gabrielshanahan.structmess.coroutine.CooperationScope
import io.github.gabrielshanahan.structmess.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.structmess.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.structmess.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.structmess.coroutine.EventLoop
import io.github.gabrielshanahan.structmess.coroutine.RollbackRequestedException
import io.github.gabrielshanahan.structmess.coroutine.RootCooperationScopeIdentifier
import io.github.gabrielshanahan.structmess.coroutine.emptyContext
import io.github.gabrielshanahan.structmess.coroutine.renderAsString
import io.github.gabrielshanahan.structmess.domain.CooperationFailure
import io.github.gabrielshanahan.structmess.domain.Message
import io.smallrye.mutiny.Uni
import io.vertx.mutiny.sqlclient.SqlConnection
import io.vertx.pgclient.pubsub.PgChannel
import io.vertx.pgclient.pubsub.PgSubscriber
import jakarta.enterprise.context.ApplicationScoped
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import org.jboss.logging.Logger

@ApplicationScoped
class PostgresMessageQueue(
    private val pgSubscriber: PgSubscriber,
    private val objectMapper: ObjectMapper,
    private val messageRepository: MessageRepository,
    private val messageEventRepository: MessageEventRepository,
    private val eventLoop: EventLoop,
) : MessageQueue, HandlerRegistry {

    private val topicsToCoroutines =
        ConcurrentHashMap.newKeySet<Pair<String, DistributedCoroutineIdentifier>>()

    companion object {
        private val logger = Logger.getLogger(PostgresMessageQueue::class.java)
    }

    override fun fetch(connection: SqlConnection, messageId: UUID): Uni<Message?> =
        messageRepository.fetchMessage(connection, messageId).map { message ->
            message?.also { logger.info("Fetched message: id=${it.id}, topic=${it.topic}") }
        }

    override fun launchOnGlobalScope(
        connection: SqlConnection,
        topic: String,
        payload: JsonNode,
        context: CooperationContext?,
    ): Uni<MessageQueue.CooperationRoot> =
        messageRepository
            .insertMessage(connection, topic, payload)
            .invoke { message ->
                message.also {
                    logger.info("Published message: id='${it.id}', topic='${it.topic}'")
                }
            }
            .flatMap { message ->
                val cooperationId = UuidCreator.getTimeOrderedEpoch()
                val cooperationLineage = listOf(cooperationId)
                messageEventRepository
                    .insertGlobalEmittedEvent(connection, message.id, cooperationLineage, context)
                    .replaceWith(
                        MessageQueue.CooperationRoot(
                            RootCooperationScopeIdentifier(cooperationId),
                            message,
                        )
                    )
            }

    override fun launch(
        scope: CooperationScope,
        topic: String,
        payload: JsonNode,
        additionalContext: CooperationContext?,
    ): Uni<Message> =
        messageRepository
            .insertMessage(scope.connection, topic, payload)
            .invoke { message ->
                logger.info("Published message: id='${message.id}', topic='${message.topic}'")
            }
            .flatMap { message ->
                messageEventRepository
                    .insertScopedEmittedEvent(
                        scope.connection,
                        message.id,
                        scope.continuation.continuationIdentifier.distributedCoroutineIdentifier
                            .name,
                        scope.continuation.continuationIdentifier.distributedCoroutineIdentifier
                            .instance,
                        scope.continuation.continuationIdentifier.stepName,
                        scope.cooperationScopeIdentifier.cooperationLineage,
                        scope.context + (additionalContext ?: emptyContext()),
                    )
                    .invoke { _ -> scope.emitted(message) }
                    .replaceWith(message)
            }

    override fun cancel(
        connection: SqlConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    ): Uni<Unit> {
        val exception = CancellationRequestedException(reason)
        val cooperationFailure = CooperationFailure.fromThrowable(exception, source)
        val exceptionJson = objectMapper.writeValueAsString(cooperationFailure)

        return messageEventRepository.insertCancellationRequestedEvent(
            connection,
            null,
            null,
            null,
            cooperationScopeIdentifier.cooperationLineage,
            exceptionJson,
        )
    }

    override fun cancel(scope: CooperationScope, reason: String): Uni<Nothing> {
        val exception = CancellationRequestedException(reason)
        val cooperationFailure =
            CooperationFailure.fromThrowable(
                exception,
                scope.continuation.continuationIdentifier.distributedCoroutineIdentifier
                    .renderAsString(),
            )
        val exceptionJson = objectMapper.writeValueAsString(cooperationFailure)

        return messageEventRepository
            .insertCancellationRequestedEvent(
                scope.connection,
                scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.name,
                scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.instance,
                scope.continuation.continuationIdentifier.stepName,
                scope.cooperationScopeIdentifier.cooperationLineage,
                exceptionJson,
            )
            .map { _ -> throw exception }
    }

    override fun rollback(
        connection: SqlConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    ): Uni<Unit> {
        val exception = RollbackRequestedException(reason)
        val cooperationFailure = CooperationFailure.fromThrowable(exception, source)
        val exceptionJson = objectMapper.writeValueAsString(cooperationFailure)

        return messageEventRepository.insertRollbackEmittedEvent(
            connection,
            cooperationScopeIdentifier.cooperationLineage,
            exceptionJson,
        )
    }

    override fun listenersByTopic(): Map<String, List<String>> =
        topicsToCoroutines
            .map { (topic, identifier) -> topic to identifier.name }
            .distinct()
            .groupBy({ it.first }, { it.second })

    override fun subscribe(
        topic: String,
        distributedCoroutine: DistributedCoroutine,
    ): MessageQueue.Subscription {
        val channel: PgChannel = pgSubscriber.channel(topic)
        topicsToCoroutines.add(topic to distributedCoroutine.identifier)

        channel.handler {
            eventLoop
                .tick(topic, distributedCoroutine)
                .subscribe()
                .with(
                    {},
                    { e ->
                        logger.error(
                            "Event loop for ${distributedCoroutine.identifier} failed when triggered from LISTEN handler",
                            e,
                        )
                    },
                )
        }

        val subscription =
            eventLoop.tickPeriodically(
                topic,
                distributedCoroutine,
                // TODO: configuration
                Duration.ofMillis(50),
            )

        return MessageQueue.Subscription {
            topicsToCoroutines.remove(topic to distributedCoroutine.identifier)
            subscription.close()
            channel.handler(null) // Executes UNLISTEN
        }
    }
}
