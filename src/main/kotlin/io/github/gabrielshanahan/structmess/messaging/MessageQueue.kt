package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.JsonNode
import io.github.gabrielshanahan.structmess.coroutine.CooperationContext
import io.github.gabrielshanahan.structmess.coroutine.CooperationRoot
import io.github.gabrielshanahan.structmess.coroutine.CooperationScope
import io.github.gabrielshanahan.structmess.coroutine.Coroutine
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
    fun launchOnGlobalScope(
        connection: SqlConnection,
        topic: String,
        payload: JsonNode,
        context: CooperationContext? = null,
    ): Uni<CooperationRoot>

    /**
     * Publish a message to a specific topic.
     *
     * @param topic The topic to publish to
     * @param payload The message payload as a JSON object
     * @return The published message
     */
    fun launch(
        scope: CooperationScope,
        topic: String,
        payload: JsonNode,
        additionalContext: CooperationContext? = null,
    ): Uni<Message>

    fun cancel(scope: CooperationScope, reason: String): Uni<Nothing>

    fun cancel(
        connection: SqlConnection,
        cooperationRoot: CooperationRoot,
        source: String,
        reason: String,
    ): Uni<Unit>

    fun rollback(
        connection: SqlConnection,
        cooperationRoot: CooperationRoot,
        source: String,
        reason: String,
    ): Uni<Unit>

    /**
     * Subscribe to messages on a specific topic.
     *
     * @param topic The topic to subscribe to
     * @return A stream of messages
     */
    fun subscribe(topic: String, coroutine: Coroutine): Subscription

    fun interface Subscription : AutoCloseable
}
