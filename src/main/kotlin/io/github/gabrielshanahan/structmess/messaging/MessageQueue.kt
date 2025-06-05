package io.github.gabrielshanahan.structmess.messaging

import io.github.gabrielshanahan.structmess.coroutine.CooperationContext
import io.github.gabrielshanahan.structmess.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.structmess.domain.Message
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonObject
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

    fun launch(
        connection: SqlConnection,
        topic: String,
        payload: JsonObject,
        context: CooperationContext? = null,
    ): Uni<CooperationRoot>

    /**
     * Subscribe to messages on a specific topic.
     *
     * @param topic The topic to subscribe to
     * @return A stream of messages
     */
    fun subscribe(topic: String, distributedCoroutine: DistributedCoroutine): Subscription

    fun interface Subscription : AutoCloseable
}
