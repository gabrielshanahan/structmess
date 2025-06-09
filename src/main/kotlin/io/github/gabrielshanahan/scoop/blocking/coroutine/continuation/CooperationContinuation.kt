package io.github.gabrielshanahan.scoop.blocking.coroutine.continuation

import io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.blocking.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.blocking.coroutine.TransactionalStep
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.StructuredCooperationManager
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.continuation.ContinuationIdentifier
import java.sql.Connection
import org.postgresql.util.PGobject

// TODO: doc that the continuation is equivalent to the scope (demonstrate graphically?)
interface CooperationContinuation : Continuation, CooperationScope {

    override val continuation
        get() = this
}

sealed interface SuspensionPoint {
    data class BeforeFirstStep(val firstStep: TransactionalStep) : SuspensionPoint

    data class BetweenSteps(val previousStep: TransactionalStep, val nextStep: TransactionalStep) :
        SuspensionPoint

    data class AfterLastStep(val lastStep: TransactionalStep) : SuspensionPoint
}

abstract class BaseCooperationContinuation(
    override val connection: Connection,
    override var context: CooperationContext,
    override val scopeIdentifier: CooperationScopeIdentifier.Child,
    private val suspensionPoint: SuspensionPoint,
    protected val distributedCoroutine: DistributedCoroutine,
    private val structuredCooperationManager: StructuredCooperationManager,
) : CooperationContinuation {

    // TODO: Doc that this is so that emitted & rollback correctly mark steps
    private lateinit var currentStep: TransactionalStep

    override val emittedMessages: MutableList<Message> = mutableListOf()

    override val continuationIdentifier: ContinuationIdentifier
        get() = ContinuationIdentifier(currentStep.name, distributedCoroutine.identifier)

    abstract fun giveUpStrategy(seen: String): String

    override fun emitted(message: Message) {
        emittedMessages.add(message)
    }

    override fun launch(
        topic: String,
        payload: PGobject,
        additionalContext: CooperationContext?,
    ): Message = structuredCooperationManager.launch(this, topic, payload, additionalContext)

    override fun launchOnGlobalScope(
        topic: String,
        payload: PGobject,
        context: CooperationContext?,
    ): CooperationRoot =
        structuredCooperationManager.launchOnGlobalScope(connection, topic, payload, context)

    override fun giveUpIfNecessary(): Unit =
        structuredCooperationManager.giveUpIfNecessary(this, this::giveUpStrategy)

    override fun resumeWith(
        lastStepResult: Continuation.LastStepResult
    ): Continuation.ContinuationResult =
        when (suspensionPoint) {
            is SuspensionPoint.BeforeFirstStep -> {
                currentStep = suspensionPoint.firstStep
                resumeCoroutine(lastStepResult)
            }

            is SuspensionPoint.BetweenSteps ->
                when (lastStepResult) {
                    is Continuation.LastStepResult.Failure -> {
                        currentStep = suspensionPoint.previousStep
                        resumeCoroutine(lastStepResult)
                    }

                    is Continuation.LastStepResult.SuccessfullyInvoked,
                    is Continuation.LastStepResult.SuccessfullyRolledBack -> {
                        currentStep = suspensionPoint.nextStep
                        resumeCoroutine(lastStepResult)
                    }
                }

            is SuspensionPoint.AfterLastStep -> {
                currentStep = suspensionPoint.lastStep
                resumeCoroutine(lastStepResult)
            }
        }

    fun resumeCoroutine(
        lastStepResult: Continuation.LastStepResult
    ): Continuation.ContinuationResult {
        return try {
            giveUpIfNecessary()
            handleFailuresOrResume(lastStepResult).also { giveUpIfNecessary() }
        } catch (e: Exception) {
            Continuation.ContinuationResult.Failure(e)
        }
    }

    private fun handleFailuresOrResume(
        lastStepResult: Continuation.LastStepResult
    ): Continuation.ContinuationResult =
        when (lastStepResult) {
            is Continuation.LastStepResult.Failure -> {
                currentStep.handleChildFailures(
                    this,
                    lastStepResult.message,
                    lastStepResult.throwable,
                )
                Continuation.ContinuationResult.Suspend(emittedMessages)
            }

            is Continuation.LastStepResult.SuccessfullyInvoked ->
                resume { currentStep.invoke(this, lastStepResult.message) }

            is Continuation.LastStepResult.SuccessfullyRolledBack ->
                resume {
                    currentStep.rollback(this, lastStepResult.message, lastStepResult.throwable)
                }
        }

    private fun resume(resumeStep: () -> Unit): Continuation.ContinuationResult =
        if (suspensionPoint is SuspensionPoint.AfterLastStep) {
            Continuation.ContinuationResult.Success
        } else {
            resumeStep()
            Continuation.ContinuationResult.Suspend(emittedMessages)
        }
}
