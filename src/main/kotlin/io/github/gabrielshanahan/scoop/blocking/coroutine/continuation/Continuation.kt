package io.github.gabrielshanahan.scoop.blocking.coroutine.continuation

import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.continuation.ContinuationIdentifier

// TODO: Doc that the suspension points are just after the last step finished, but before the new
// one started!
interface Continuation {

    val continuationIdentifier: ContinuationIdentifier

    fun resumeWith(lastStepResult: LastStepResult): ContinuationResult

    sealed interface LastStepResult {
        val message: Message

        data class SuccessfullyInvoked(override val message: Message) : LastStepResult

        data class SuccessfullyRolledBack(override val message: Message, val throwable: Throwable) :
            LastStepResult

        data class Failure(override val message: Message, val throwable: Throwable) :
            LastStepResult
    }

    sealed interface ContinuationResult {
        data class Suspend(val emittedMessages: List<Message>) :
            ContinuationResult // TODO: Doc that Emitted at the end of each step no matter what

        data object Success : ContinuationResult

        data class Failure(val exception: Throwable) : ContinuationResult
    }
}
