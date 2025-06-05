package io.github.gabrielshanahan.structmess.messaging

import io.github.gabrielshanahan.structmess.domain.Message
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.SqlConnection
import io.vertx.mutiny.sqlclient.Tuple
import jakarta.enterprise.context.ApplicationScoped
import java.time.ZoneId
import java.util.UUID

@ApplicationScoped
class MessageRepository {

    fun fetchMessage(connection: SqlConnection, messageId: UUID): Uni<Message?> =
        connection
            .preparedQuery("SELECT id, topic, payload, created_at FROM message WHERE id = $1")
            .execute(Tuple.of(messageId))
            .map { rowSet ->
                rowSet.firstOrNull()?.let { row ->
                    Message(
                        id = row.getUUID("id"),
                        topic = row.getString("topic"),
                        payload = row.getJsonObject("payload"),
                        createdAt =
                            row.getLocalDateTime("created_at").atZone(ZoneId.systemDefault()),
                    )
                }
            }

    fun insertMessage(connection: SqlConnection, topic: String, payload: JsonObject): Uni<Message> =
        connection
            .preparedQuery(
                "INSERT INTO message (topic, payload) VALUES ($1, $2) RETURNING id, created_at"
            )
            .execute(Tuple.of(topic, payload))
            .map { rowSet ->
                val row =
                    checkNotNull(rowSet.firstOrNull()) {
                        "Unable to publish message on topic $topic"
                    }
                Message(
                    id = row.getUUID("id"),
                    topic = topic,
                    payload = payload,
                    createdAt = row.getLocalDateTime("created_at").atZone(ZoneId.systemDefault()),
                )
            }
}
