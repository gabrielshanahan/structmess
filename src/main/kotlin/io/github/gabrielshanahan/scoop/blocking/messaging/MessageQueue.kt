package io.github.gabrielshanahan.scoop.blocking.messaging

import io.github.gabrielshanahan.scoop.blocking.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.messaging.Subscription
import java.sql.Connection
import java.util.UUID
import org.postgresql.util.PGobject

interface MessageQueue {

    fun fetch(connection: Connection, messageId: UUID): Message?

    fun launch(
        connection: Connection,
        topic: String,
        payload: PGobject,
        context: CooperationContext? = null,
    ): CooperationRoot

    fun subscribe(topic: String, distributedCoroutine: DistributedCoroutine): Subscription
}
