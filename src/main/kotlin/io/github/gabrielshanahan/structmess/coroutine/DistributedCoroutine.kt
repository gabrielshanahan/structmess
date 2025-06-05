package io.github.gabrielshanahan.structmess.coroutine

import com.github.f4b6a3.uuid.UuidCreator
import io.github.gabrielshanahan.structmess.domain.CooperationException
import io.github.gabrielshanahan.structmess.domain.Message
import io.github.gabrielshanahan.structmess.messaging.CooperationRoot
import io.github.gabrielshanahan.structmess.messaging.StructuredCooperationManager
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.SqlConnection
import java.util.UUID
import kotlin.addSuppressed

// TODO: Config
const val ROLLING_BACK_PREFIX = "Rollback of "
const val ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX = " (rolling back child scopes)"

sealed interface CooperationScopeIdentifier {
    val cooperationLineage: List<UUID>
}

data class RootCooperationScopeIdentifier(val cooperationId: UUID) : CooperationScopeIdentifier {
    override val cooperationLineage = listOf(cooperationId)
}

data class ContinuationCooperationScopeIdentifier(override val cooperationLineage: List<UUID>) :
    CooperationScopeIdentifier

data class DistributedCoroutineIdentifier(val name: String, val instance: String) {
    constructor(name: String) : this(name, UuidCreator.getTimeOrderedEpoch().toString())
}

fun DistributedCoroutineIdentifier.renderAsString() = "$name[$instance]"

data class ContinuationIdentifier(
    val stepName: String,
    val distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
)

// TODO: Doc that the suspension points are just after the last step finished, but before the new
// one started!
interface Continuation {

    val continuationIdentifier: ContinuationIdentifier

    fun resumeWith(lastStepResult: LastStepResult): Uni<out Result>

    sealed interface LastStepResult {
        val message: Message

        data class SuccessfullyInvoked(override val message: Message) : LastStepResult

        data class SuccessfullyRolledBack(override val message: Message, val throwable: Throwable) :
            LastStepResult

        data class Failure(override val message: Message, val throwable: Throwable) :
            LastStepResult
    }

    sealed interface Result {
        data class Suspend(val emittedMessages: List<Message>) :
            Result // TODO: Doc that Emitted at the end of each step no matter what

        data object Success : Result

        data class Failure(val exception: Throwable) : Result
    }
}

// TODO: Doc what these states mean
interface TransactionalStep {
    val name: String

    fun invoke(scope: CooperationScope, message: Message): Uni<Unit>

    fun rollback(scope: CooperationScope, message: Message, throwable: Throwable): Uni<Unit> =
        Uni.createFrom().item(Unit)

    // TODO: Doc that whatever happens here is, conceptually, part of the same step as invoke (even
    //  though its a different transaction), so a) no guarantees can be made on rollback order (all
    //  ROLLBACK_EMITTED are done at once) and b) any definition of rollback() needs to take into
    //  account that handleChildFailures() ran, and roll that back as well. In general, advise that
    //  you should mostly only use this for super-simple retries, or deliberately ignoring errors.
    //  Also doc that this is why we're not providing a retry implementation out of the box.
    //
    // TODO: It might be cleaner to have a rollback step for handleChildFailures as well,
    //  but let's keep that as a future improvement, since that would mean we would need to be able
    //  to support a dynamic amount of "mezi-steps" between each step - def. doable, but not now.
    fun handleChildFailures(
        scope: CooperationScope,
        message: Message,
        throwable: Throwable,
    ): Uni<Unit> = throw throwable
}

class DistributedCoroutine(
    val identifier: DistributedCoroutineIdentifier,
    val steps: List<TransactionalStep>,
    val eventLoopStrategy: EventLoopStrategy,
) {
    init {
        check(steps.isNotEmpty()) { "Steps cannot be empty" }
        check(steps.mapTo(mutableSetOf()) { it.name }.size == steps.size) {
            "Steps must have unique names"
        }
    }
}

// TODO: Doc that the scope stays the same for entire coroutine run! Even including ROLLING_BACK
interface CooperationScope {

    val continuationCooperationScopeIdentifier: ContinuationCooperationScopeIdentifier

    val message: Message

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

sealed interface SuspensionPoint {
    data class BeforeFirstStep(val firstStep: TransactionalStep) : SuspensionPoint

    data class BetweenSteps(val previousStep: TransactionalStep, val nextStep: TransactionalStep) :
        SuspensionPoint

    data class AfterLastStep(val lastStep: TransactionalStep) : SuspensionPoint
}

// TODO: doc that the continuation is equivalent to the scope (demonstrate graphically?)
interface CooperationContinuation : Continuation, CooperationScope {

    override val continuation
        get() = this
}

class CooperationContinuationImpl(
    override val connection: SqlConnection,
    override var context: CooperationContext,
    override val continuationCooperationScopeIdentifier: ContinuationCooperationScopeIdentifier,
    override val message: Message,
    private val suspensionPoint: SuspensionPoint,
    private val distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
    private val structuredCooperationManager: StructuredCooperationManager,
    private val giveUpSqlProvider: (String) -> String,
) : CooperationContinuation {

    // TODO: Doc that this is so that emitted & rollback correctly mark steps
    private lateinit var currentStep: TransactionalStep

    override val emittedMessages: MutableList<Message> = mutableListOf()

    override val continuationIdentifier: ContinuationIdentifier
        get() = ContinuationIdentifier(currentStep.name, distributedCoroutineIdentifier)

    override fun emitted(message: Message) {
        emittedMessages.add(message)
    }

    override fun launch(
        topic: String,
        payload: JsonObject,
        additionalContext: CooperationContext?,
    ): Uni<Message> = structuredCooperationManager.launch(this, topic, payload, additionalContext)

    override fun launchOnGlobalScope(
        topic: String,
        payload: JsonObject,
        context: CooperationContext?,
    ): Uni<CooperationRoot> =
        structuredCooperationManager.launchOnGlobalScope(connection, topic, payload, context)

    override fun giveUpIfNecessary(): Uni<Unit> =
        structuredCooperationManager.giveUpIfNecessary(this, giveUpSqlProvider)

    override fun resumeWith(lastStepResult: Continuation.LastStepResult): Uni<Continuation.Result> =
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

    fun resumeCoroutine(lastStepResult: Continuation.LastStepResult): Uni<Continuation.Result> {
        return try {
            giveUpIfNecessary()
                .flatMap { handleFailuresOrResume(lastStepResult) }
                .call(this::giveUpIfNecessary)
                .onFailure()
                .recoverWithItem { e -> Continuation.Result.Failure(e) }
        } catch (e: Exception) {
            Uni.createFrom().item(Continuation.Result.Failure(e))
        }
    }

    private fun handleFailuresOrResume(
        lastStepResult: Continuation.LastStepResult
    ): Uni<Continuation.Result> =
        when (lastStepResult) {
            is Continuation.LastStepResult.Failure ->
                currentStep
                    .handleChildFailures(this, lastStepResult.message, lastStepResult.throwable)
                    .replaceWith {
                        Continuation.Result.Suspend(emittedMessages) as Continuation.Result
                    }

            is Continuation.LastStepResult.SuccessfullyInvoked ->
                resume { currentStep.invoke(this, lastStepResult.message) }

            is Continuation.LastStepResult.SuccessfullyRolledBack ->
                resume {
                    currentStep.rollback(this, lastStepResult.message, lastStepResult.throwable)
                }
        }

    private fun resume(resumeStep: () -> Uni<Unit>): Uni<Continuation.Result> =
        if (suspensionPoint is SuspensionPoint.AfterLastStep) {
            Uni.createFrom().item(Continuation.Result.Success)
        } else {
            resumeStep().replaceWith {
                Continuation.Result.Suspend(emittedMessages) as Continuation.Result
            }
        }
}

fun DistributedCoroutine.buildHappyPathContinuation(
    connection: SqlConnection,
    coroutineState: CoroutineState,
    structuredCooperationManager: StructuredCooperationManager,
    giveUpSqlProvider: (String) -> String,
) =
    when (coroutineState.lastSuspendedStep) {
        is NotSuspendedYet -> {
            // No SUSPEND record, so we've just started processing this message
            CooperationContinuationImpl(
                connection,
                coroutineState.cooperationContext,
                coroutineState.continuationCooperationScopeIdentifier,
                coroutineState.message,
                SuspensionPoint.BeforeFirstStep(steps.first()),
                identifier,
                structuredCooperationManager,
                giveUpSqlProvider,
            )
        }

        is SuspendedAt -> {
            val suspendedStepIdx =
                steps.indexOfFirst { it.name == coroutineState.lastSuspendedStep.stepName }
            if (suspendedStepIdx == -1) {
                throw IllegalStateException(
                    "Step ${coroutineState.lastSuspendedStep} was not found"
                )
            }

            if (steps[suspendedStepIdx] == steps.last()) {
                CooperationContinuationImpl(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.continuationCooperationScopeIdentifier,
                    coroutineState.message,
                    SuspensionPoint.AfterLastStep(steps.last()),
                    identifier,
                    structuredCooperationManager,
                    giveUpSqlProvider,
                )
            } else {
                CooperationContinuationImpl(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.continuationCooperationScopeIdentifier,
                    coroutineState.message,
                    SuspensionPoint.BetweenSteps(
                        steps[suspendedStepIdx],
                        steps[suspendedStepIdx + 1],
                    ),
                    identifier,
                    structuredCooperationManager,
                    giveUpSqlProvider,
                )
            }
        }
    }

fun DistributedCoroutine.buildRollbackPathContinuation(
    connection: SqlConnection,
    coroutineState: CoroutineState,
    structuredCooperationManager: StructuredCooperationManager,
    giveUpSqlProvider: (String) -> String,
): CooperationContinuationImpl {
    val prefix = run {
        var prefix = ROLLING_BACK_PREFIX
        while (steps.any { it.name.startsWith(prefix) }) {
            prefix += "$"
        }
        prefix
    }
    val suffix = run {
        var suffix = ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX
        while (steps.any { it.name.endsWith(suffix) }) {
            suffix += "$"
        }
        suffix
    }

    val rollbackSteps =
        steps.flatMap { step ->
            listOf(
                object : TransactionalStep {
                    override val name: String
                        get() = prefix + step.name

                    override fun invoke(scope: CooperationScope, message: Message) =
                        step.invoke(scope, message)

                    override fun rollback(
                        scope: CooperationScope,
                        message: Message,
                        throwable: Throwable,
                    ): Uni<Unit> = step.rollback(scope, message, throwable)

                    override fun handleChildFailures(
                        scope: CooperationScope,
                        message: Message,
                        throwable: Throwable,
                    ): Uni<Unit> = step.handleChildFailures(scope, message, throwable)
                },
                object : TransactionalStep {
                    override val name: String
                        get() = prefix + step.name + suffix

                    override fun invoke(scope: CooperationScope, message: Message) =
                        throw IllegalStateException("Should never be invoked")

                    override fun rollback(
                        scope: CooperationScope,
                        message: Message,
                        throwable: Throwable,
                    ): Uni<Unit> =
                        structuredCooperationManager.emitRollbacksForEmissions(
                            scope,
                            step.name,
                            throwable,
                        )

                    override fun handleChildFailures(
                        scope: CooperationScope,
                        message: Message,
                        throwable: Throwable,
                    ): Uni<Unit> = step.handleChildFailures(scope, message, throwable)
                },
            )
        }

    return when (coroutineState.lastSuspendedStep) {
        is NotSuspendedYet -> {
            // We're rolling back before the first step committed. Since the
            // transaction itself was never committed, nothing was emitted, and we're done.
            CooperationContinuationImpl(
                connection,
                coroutineState.cooperationContext,
                coroutineState.continuationCooperationScopeIdentifier,
                coroutineState.message,
                SuspensionPoint.AfterLastStep(rollbackSteps.first()),
                identifier,
                structuredCooperationManager,
                giveUpSqlProvider,
            )
        }

        is SuspendedAt -> {

            val cleanLastSuspendedStepName =
                coroutineState.lastSuspendedStep.stepName.removePrefix(prefix).removeSuffix(suffix)

            val stepToRevert = steps.indexOfFirst { it.name == cleanLastSuspendedStepName }

            if (stepToRevert == -1) {
                throw IllegalStateException("Step $cleanLastSuspendedStepName was not found")
            }

            // TODO: Doc that this means "rollback just started" and it allows us to have this logic
            //  here, instead of in SQL. Although now that I think of if, adding a "rollback is the
            //  last event" might as well be made part of the SQL, since it's almost identical to
            //  what we do with contexts anyway
            if (cleanLastSuspendedStepName == coroutineState.lastSuspendedStep.stepName) {
                // We've doubled the number of steps by transforming each step into two substeps, so
                // we multiply by two, and we want to start with the last one (reverting child
                // scopes), so we add one
                val nextStepIdx = 2 * stepToRevert + 1
                CooperationContinuationImpl(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.continuationCooperationScopeIdentifier,
                    coroutineState.message,
                    SuspensionPoint.BeforeFirstStep(rollbackSteps[nextStepIdx]),
                    identifier,
                    structuredCooperationManager,
                    giveUpSqlProvider,
                )
            } else {
                val nextStepIdx =
                    rollbackSteps.indexOfFirst {
                        it.name == coroutineState.lastSuspendedStep.stepName
                    }
                if (rollbackSteps[nextStepIdx] == rollbackSteps.first()) {
                    CooperationContinuationImpl(
                        connection,
                        coroutineState.cooperationContext,
                        coroutineState.continuationCooperationScopeIdentifier,
                        coroutineState.message,
                        SuspensionPoint.AfterLastStep(rollbackSteps.first()),
                        identifier,
                        structuredCooperationManager,
                        giveUpSqlProvider,
                    )
                } else {
                    CooperationContinuationImpl(
                        connection,
                        coroutineState.cooperationContext,
                        coroutineState.continuationCooperationScopeIdentifier,
                        coroutineState.message,
                        SuspensionPoint.BetweenSteps(
                            rollbackSteps[nextStepIdx],
                            rollbackSteps[nextStepIdx - 1],
                        ),
                        identifier,
                        structuredCooperationManager,
                        giveUpSqlProvider,
                    )
                }
            }
        }
    }
}

abstract class ScopeException(message: String?, cause: Throwable?, stackTrace: Boolean) :
    Exception(message, cause, true, stackTrace)

class ParentSaidSoException(cause: Throwable) : ScopeException(null, cause, false)

class ChildRolledBackException(causes: List<CooperationException>) :
    ScopeException(null, causes.first(), false) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
    }
}

class ChildRollbackFailedException(causes: List<Throwable>) :
    ScopeException(null, causes.first(), false) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
    }
}

class CancellationRequestedException(
    reason: String,
    causes: List<CooperationException> = emptyList(),
) : ScopeException(reason, causes.firstOrNull(), true) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
        // Point to the place where cancel() was actually called
        stackTrace = stackTrace.drop(1).toTypedArray()
    }
}

class RollbackRequestedException(reason: String, causes: List<CooperationException> = emptyList()) :
    ScopeException(reason, causes.firstOrNull(), true) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
        // Point to the place where rollback() was actually called
        stackTrace = stackTrace.drop(1).toTypedArray()
    }
}

class GaveUpException(causes: List<Throwable>) : ScopeException(null, causes.first(), false) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
    }
}
