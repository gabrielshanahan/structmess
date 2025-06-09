package io.github.gabrielshanahan.scoop.reactive.messaging

import io.github.gabrielshanahan.scoop.reactive.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.messaging.Subscription
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.SqlConnection
import java.util.UUID

interface MessageQueue {

    fun fetch(connection: SqlConnection, messageId: UUID): Uni<Message?>

    fun launch(
        connection: SqlConnection,
        topic: String,
        payload: JsonObject,
        context: CooperationContext? = null,
    ): Uni<CooperationRoot>

    fun subscribe(topic: String, distributedCoroutine: DistributedCoroutine): Subscription
}
