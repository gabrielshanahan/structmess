package io.github.gabrielshanahan.structmess.messaging

import io.github.gabrielshanahan.structmess.coroutine.CooperationContext
import io.github.gabrielshanahan.structmess.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.structmess.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.structmess.coroutine.EventLoop
import io.github.gabrielshanahan.structmess.coroutine.SLEEP_TOPIC
import io.github.gabrielshanahan.structmess.domain.Message
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonObject
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
    private val structuredCooperationManager: StructuredCooperationManager,
    private val messageRepository: MessageRepository,
    private val eventLoop: EventLoop,
) : MessageQueue, HandlerRegistry {

    private val topicsToCoroutines =
        ConcurrentHashMap.newKeySet<Pair<String, DistributedCoroutineIdentifier>>().apply {
            add(SLEEP_TOPIC to DistributedCoroutineIdentifier("ephemeral-sleep-handler"))
        }

    companion object {
        private val logger = Logger.getLogger(PostgresMessageQueue::class.java)
    }

    override fun fetch(connection: SqlConnection, messageId: UUID): Uni<Message?> =
        messageRepository.fetchMessage(connection, messageId).map { message ->
            message?.also { logger.info("Fetched message: id=${it.id}, topic=${it.topic}") }
        }

    override fun launch(
        connection: SqlConnection,
        topic: String,
        payload: JsonObject,
        context: CooperationContext?,
    ): Uni<CooperationRoot> =
        structuredCooperationManager.launchOnGlobalScope(connection, topic, payload, context)

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
