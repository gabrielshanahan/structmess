package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.domain.Message
import io.smallrye.mutiny.Uni
import io.vertx.mutiny.sqlclient.Pool
import io.vertx.mutiny.sqlclient.SqlConnection
import io.vertx.mutiny.sqlclient.Tuple
import io.vertx.pgclient.pubsub.PgChannel
import io.vertx.pgclient.pubsub.PgSubscriber
import jakarta.enterprise.context.ApplicationScoped
import java.time.ZoneId
import java.util.*
import org.jboss.logging.Logger

@ApplicationScoped
class PostgresMessageQueue(
    private val client: Pool,
    private val pgSubscriber: PgSubscriber,
    private val objectMapper: ObjectMapper,
) : MessageQueue {

    companion object {
        private val logger = Logger.getLogger(PostgresMessageQueue::class.java)
        private const val TABLE_NAME = "messages"
    }

    override fun fetch(connection: SqlConnection, messageId: UUID): Uni<Message?> =
        connection
            .preparedQuery("SELECT id, topic, payload, created_at FROM $TABLE_NAME WHERE id = $1")
            .execute(Tuple.of(messageId))
            .onItem()
            .transform { rowSet ->
                rowSet
                    .firstOrNull()
                    ?.let { row ->
                        Message(
                            id = row.getUUID("id"),
                            topic = row.getString("topic"),
                            payload = objectMapper.readTree(row.getString("payload")),
                            createdAt =
                                row.getLocalDateTime("created_at").atZone(ZoneId.systemDefault()),
                        )
                    }
                    ?.also { logger.info("Fetched message: id=$it.id, topic=$it.topic") }
            }

    override fun publish(
        connection: SqlConnection,
        topic: String,
        payload: JsonNode,
    ): Uni<Message> =
        connection
            .preparedQuery(
                "INSERT INTO $TABLE_NAME (topic, payload) VALUES ($1, $2::jsonb) RETURNING id, created_at"
            )
            .execute(Tuple.of(topic, payload.toString()))
            .onItem()
            .transform { rowSet ->
                val row =
                    checkNotNull(rowSet.firstOrNull()) {
                        "Unable to publish message for topic $topic"
                    }
                Message(
                        id = row.getUUID("id"),
                        topic = topic,
                        payload = payload,
                        createdAt =
                            row.getLocalDateTime("created_at").atZone(ZoneId.systemDefault()),
                    )
                    .also { logger.info("Published message: id='${it.id}', topic='${it.topic}'") }
            }

    override fun subscribe(
        topic: String,
        handler: (SqlConnection, Message) -> Uni<Unit>,
    ): MessageQueue.Subscription {
        val channel: PgChannel = pgSubscriber.channel(topic)
        channel.handler { id ->
            client
                .withTransaction { connection ->
                    fetch(connection, UUID.fromString(id)).flatMap { handler(connection, it!!) }
                }
                .subscribe()
                .with({}, { e -> logger.error("Failed processing message $id", e) })
        }

        return MessageQueue.Subscription {
            channel.handler(null) // Executes UNLISTEN
        }
    }
}
