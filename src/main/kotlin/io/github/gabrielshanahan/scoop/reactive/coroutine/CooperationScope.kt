package io.github.gabrielshanahan.scoop.reactive.coroutine

import io.github.gabrielshanahan.scoop.reactive.coroutine.continuation.Continuation
import io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.reactive.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.SqlConnection

// TODO: Doc that the scope stays the same for entire coroutine run! Even including ROLLING_BACK
interface CooperationScope {

    val scopeIdentifier: CooperationScopeIdentifier.Child

    var context: CooperationContext

    val continuation: Continuation

    val connection: SqlConnection
    val emittedMessages: List<Message>

    fun emitted(message: Message)

    fun launch(
        topic: String,
        payload: JsonObject,
        additionalContext: CooperationContext? = null,
    ): Uni<Message>

    fun launchOnGlobalScope(
        topic: String,
        payload: JsonObject,
        context: CooperationContext? = null,
    ): Uni<CooperationRoot>

    // TODO: DOC that Definition of this should actually be part of step, since
    //  want to be able to create an uncancellable step. Actually, per-coroutine
    //  should be enough, especially for a POC. IF you need it for a specific step,
    //  you can always create one that just emits a message that's consumed by a
    //  non-cancellable coroutine
    fun giveUpIfNecessary(): Uni<Unit>
}
