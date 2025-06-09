package io.github.gabrielshanahan.scoop.blocking.messaging

import io.github.gabrielshanahan.scoop.blocking.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.blocking.coroutine.EventLoop
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.SLEEP_TOPIC
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.SleepEventLoopStrategy
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.StructuredCooperationManager
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.messaging.Subscription
import io.vertx.pgclient.pubsub.PgChannel
import io.vertx.pgclient.pubsub.PgSubscriber
import jakarta.enterprise.context.ApplicationScoped
import java.sql.Connection
import java.time.Duration
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import org.jboss.logging.Logger
import org.postgresql.util.PGobject

// TODO: Fix detekt
// TODO: Setup github actions
// TODO: Rewrite to non-reactive
@ApplicationScoped
class PostgresMessageQueue(
    private val pgSubscriber: PgSubscriber,
    private val structuredCooperationManager: StructuredCooperationManager,
    private val messageRepository: MessageRepository,
    private val eventLoop: EventLoop,
) : MessageQueue, HandlerRegistry {

    private val topicsToCoroutines =
        ConcurrentHashMap.newKeySet<Pair<String, DistributedCoroutineIdentifier>>()

    init {
        subscribe(
            SLEEP_TOPIC,
            saga("sleep-handler", SleepEventLoopStrategy(OffsetDateTime.now())) {
                step("sleep") { _, _ -> }
            },
        )
    }

    companion object {
        private val logger = Logger.getLogger(PostgresMessageQueue::class.java)
    }

    override fun fetch(connection: Connection, messageId: UUID): Message? =
        messageRepository.fetchMessage(connection, messageId)?.also {
            logger.info("Fetched message: id=${it.id}, topic=${it.topic}")
        }

    override fun launch(
        connection: Connection,
        topic: String,
        payload: PGobject,
        context: CooperationContext?,
    ): CooperationRoot =
        structuredCooperationManager.launchOnGlobalScope(connection, topic, payload, context)

    override fun listenersByTopic(): Map<String, List<String>> =
        topicsToCoroutines
            .map { (topic, identifier) -> topic to identifier.name }
            .distinct()
            .groupBy({ it.first }, { it.second })

    override fun subscribe(
        topic: String,
        distributedCoroutine: DistributedCoroutine,
    ): Subscription {
        val channel: PgChannel = pgSubscriber.channel(topic)
        topicsToCoroutines.add(topic to distributedCoroutine.identifier)

        channel.handler { eventLoop.tick(topic, distributedCoroutine) }

        val subscription =
            eventLoop.tickPeriodically(
                topic,
                distributedCoroutine,
                // TODO: configuration
                Duration.ofMillis(50),
            )

        return Subscription {
            topicsToCoroutines.remove(topic to distributedCoroutine.identifier)
            subscription.close()
            channel.handler(null) // Executes UNLISTEN
        }
    }
}
