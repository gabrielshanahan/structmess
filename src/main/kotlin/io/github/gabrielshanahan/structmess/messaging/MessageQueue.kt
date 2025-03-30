package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.JsonNode
import io.github.gabrielshanahan.structmess.domain.Message
import io.smallrye.mutiny.Uni
import io.vertx.mutiny.sqlclient.SqlConnection
import java.util.UUID

/**
 * Interface for a message queue implementation that supports publish-subscribe pattern and message
 * claiming for work queue pattern.
 */
interface MessageQueue {
    /**
     * Fetch message from a specific topic
     *
     * @param messageId The id of the message
     */
    fun fetch(connection: SqlConnection, messageId: UUID): Uni<Message?>

    /**
     * Publish a message to a specific topic.
     *
     * @param topic The topic to publish to
     * @param payload The message payload as a JSON object
     * @return The published message
     */
    fun publish(connection: SqlConnection, topic: String, payload: JsonNode): Uni<Message>

    /**
     * Subscribe to messages on a specific topic.
     *
     * @param topic The topic to subscribe to
     * @return A stream of messages
     */
    fun subscribe(topic: String, handler: (SqlConnection, Message) -> Uni<Unit>): Subscription

    fun interface Subscription : AutoCloseable
}
