package io.github.gabrielshanahan.scoop.blocking.messaging

import jakarta.enterprise.context.ApplicationScoped
import java.sql.Connection
import java.time.ZoneOffset
import java.util.UUID
import org.codejargon.fluentjdbc.api.FluentJdbc
import org.postgresql.util.PGobject

@ApplicationScoped
class MessageRepository(private val fluentJdbc: FluentJdbc) {

    fun fetchMessage(connection: Connection, messageId: UUID): Message? =
        fluentJdbc
            .queryOn(connection)
            .select("SELECT id, topic, payload, created_at FROM message WHERE id = :messageId")
            .namedParam("messageId", messageId)
            .firstResult { resultSet ->
                Message(
                    id = resultSet.getObject("id", UUID::class.java),
                    topic = resultSet.getString("topic"),
                    payload = resultSet.getObject("payload") as PGobject,
                    createdAt =
                        resultSet
                            .getTimestamp("created_at")
                            .toLocalDateTime()
                            .atOffset(ZoneOffset.UTC),
                )
            }
            .orElse(null)

    fun insertMessage(connection: Connection, topic: String, payload: PGobject): Message =
        // For PostgreSQL RETURNING clause, we need to use a regular select query
        fluentJdbc
            .queryOn(connection)
            .select(
                "INSERT INTO message (topic, payload) VALUES (:topic, :payload) RETURNING id, created_at"
            )
            .namedParam("topic", topic)
            .namedParam("payload", payload)
            .singleResult { resultSet ->
                Message(
                    id = resultSet.getObject("id", UUID::class.java),
                    topic = topic,
                    payload = payload,
                    createdAt =
                        resultSet
                            .getTimestamp("created_at")
                            .toLocalDateTime()
                            .atOffset(ZoneOffset.UTC),
                )
            }
}
