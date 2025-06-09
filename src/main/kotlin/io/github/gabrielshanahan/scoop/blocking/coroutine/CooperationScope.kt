package io.github.gabrielshanahan.scoop.blocking.coroutine

import io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.Continuation
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import java.sql.Connection
import org.postgresql.util.PGobject

// TODO: Doc that the scope stays the same for entire coroutine run! Even including ROLLING_BACK
interface CooperationScope {

    val scopeIdentifier: CooperationScopeIdentifier.Child

    var context: CooperationContext

    val continuation: Continuation

    val connection: Connection
    val emittedMessages: List<Message>

    fun emitted(message: Message)

    fun launch(
        topic: String,
        payload: PGobject,
        additionalContext: CooperationContext? = null,
    ): Message

    fun launchOnGlobalScope(
        topic: String,
        payload: PGobject,
        context: CooperationContext? = null,
    ): CooperationRoot

    // TODO: DOC that Definition of this should actually be part of step, since
    //  want to be able to create an uncancellable step. Actually, per-coroutine
    //  should be enough, especially for a POC. IF you need it for a specific step,
    //  you can always create one that just emits a message that's consumed by a
    //  non-cancellable coroutine
    fun giveUpIfNecessary()
}
