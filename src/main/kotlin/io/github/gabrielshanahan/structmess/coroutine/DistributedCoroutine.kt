package io.github.gabrielshanahan.structmess.coroutine

import com.github.f4b6a3.uuid.UuidCreator
import io.github.gabrielshanahan.structmess.domain.CooperationException
import io.github.gabrielshanahan.structmess.domain.Message
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.infrastructure.Infrastructure
import io.vertx.mutiny.sqlclient.SqlConnection
import java.util.UUID

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
    val childStrategy: ChildStrategy,
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

    val cooperationScopeIdentifier: ContinuationCooperationScopeIdentifier

    val message: Message

    var context: CooperationContext

    val continuation: Continuation

    val connection: SqlConnection
    val emittedMessages: List<Message>

    fun emitted(message: Message)

    // TODO: Definition of this should actually be part of step, since want to be able to create an
    //  uncancellable step
    //  This seems to be intimately related to cancellation tokens. So maybe we should have the
    //  event loop available here, and ensureActive should live there? and the method on EventLoop
    //  should accept EventLoopStrategy. This means that EventLoopStrategy must be part of step definition,
    //  and we need to build it in such a way so that it's applied differently to different steps.
    fun ensureActive(): Uni<Unit>
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

abstract class CooperationContinuationBase(
    override val connection: SqlConnection,
    override var context: CooperationContext,
    override val cooperationScopeIdentifier: ContinuationCooperationScopeIdentifier,
    override val message: Message,
    private val suspensionPoint: SuspensionPoint,
    private val distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
    private val fetchCancellationExceptions: (CooperationScope) -> Uni<List<CooperationException>>,
) : CooperationContinuation {

    // TODO: Doc that this is so that emitted & rollback correctly mark steps
    protected lateinit var currentStep: TransactionalStep

    override val emittedMessages: MutableList<Message> = mutableListOf()

    override val continuationIdentifier: ContinuationIdentifier
        get() = ContinuationIdentifier(currentStep.name, distributedCoroutineIdentifier)

    override fun emitted(message: Message) {
        emittedMessages.add(message)
    }

    override fun ensureActive(): Uni<Unit> =
        fetchCancellationExceptions(this).emitOn(Infrastructure.getDefaultWorkerPool()).map {
            exceptions ->
            if (exceptions.any()) {
                throw CancellationRequestedException("Cancellation request received", exceptions)
            }
        }

    override fun resumeWith(
        lastStepResult: Continuation.LastStepResult
    ): Uni<out Continuation.Result> =
        when (suspensionPoint) {
            is SuspensionPoint.BeforeFirstStep -> {
                currentStep = suspensionPoint.firstStep
                handleFailuresAndResume(lastStepResult)
            }
            is SuspensionPoint.BetweenSteps ->
                when (lastStepResult) {
                    is Continuation.LastStepResult.Failure -> {
                        currentStep = suspensionPoint.previousStep
                        handleFailuresAndResume(lastStepResult)
                    }
                    is Continuation.LastStepResult.SuccessfullyInvoked,
                    is Continuation.LastStepResult.SuccessfullyRolledBack -> {
                        currentStep = suspensionPoint.nextStep
                        handleFailuresAndResume(lastStepResult)
                    }
                }
            is SuspensionPoint.AfterLastStep -> {
                currentStep = suspensionPoint.lastStep
                handleFailuresAndResume(lastStepResult)
            }
        }

    fun handleFailuresAndResume(
        lastStepResult: Continuation.LastStepResult
    ): Uni<out Continuation.Result> {

        return when (lastStepResult) {
            is Continuation.LastStepResult.Failure ->
                try {
                    currentStep
                        .handleChildFailures(this, lastStepResult.message, lastStepResult.throwable)
                        .replaceWith {
                            Continuation.Result.Suspend(emittedMessages) as Continuation.Result
                        }
                        .onFailure()
                        .recoverWithItem { e -> Continuation.Result.Failure(e) }
                } catch (e: Exception) {
                    Uni.createFrom().item(Continuation.Result.Failure(e))
                }
            is Continuation.LastStepResult.SuccessfullyInvoked,
            is Continuation.LastStepResult.SuccessfullyRolledBack -> resume(lastStepResult)
        }
    }

    private fun resume(lastStepResult: Continuation.LastStepResult) =
        when (suspensionPoint) {
            is SuspensionPoint.AfterLastStep -> Uni.createFrom().item(Continuation.Result.Success)
            is SuspensionPoint.BeforeFirstStep,
            is SuspensionPoint.BetweenSteps ->
                try {
                    runStep(lastStepResult)
                        .replaceWith {
                            Continuation.Result.Suspend(emittedMessages) as Continuation.Result
                        }
                        .onFailure()
                        .recoverWithItem { e -> Continuation.Result.Failure(e) }
                } catch (e: Exception) {
                    Uni.createFrom().item(Continuation.Result.Failure(e))
                }
        }

    abstract fun runStep(lastStepResult: Continuation.LastStepResult): Uni<Unit>
}

class RegularCooperationContinuation(
    connection: SqlConnection,
    context: CooperationContext,
    cooperationScopeIdentifier: ContinuationCooperationScopeIdentifier,
    message: Message,
    suspensionPoint: SuspensionPoint,
    distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
    fetchCancellationExceptions: (CooperationScope) -> Uni<List<CooperationException>>,
) :
    CooperationContinuationBase(
        connection,
        context,
        cooperationScopeIdentifier,
        message,
        suspensionPoint,
        distributedCoroutineIdentifier,
        fetchCancellationExceptions,
    ) {
    override fun runStep(lastStepResult: Continuation.LastStepResult): Uni<Unit> =
        when (lastStepResult) {
            is Continuation.LastStepResult.Failure,
            is Continuation.LastStepResult.SuccessfullyRolledBack ->
                throw IllegalStateException(
                    "Invalid last step result type ${lastStepResult::class.simpleName} for regular coroutine continuation"
                )
            is Continuation.LastStepResult.SuccessfullyInvoked ->
                ensureActive()
                    .replaceWith(currentStep.invoke(this, lastStepResult.message))
                    .replaceWith(ensureActive())
        }
}

class RollbackCooperationContinuation(
    connection: SqlConnection,
    context: CooperationContext,
    cooperationScopeIdentifier: ContinuationCooperationScopeIdentifier,
    message: Message,
    suspensionPoint: SuspensionPoint,
    distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
    fetchCancellationExceptions: (CooperationScope) -> Uni<List<CooperationException>>,
) :
    CooperationContinuationBase(
        connection,
        context,
        cooperationScopeIdentifier,
        message,
        suspensionPoint,
        distributedCoroutineIdentifier,
        fetchCancellationExceptions,
    ) {
    override fun runStep(lastStepResult: Continuation.LastStepResult): Uni<Unit> =
        when (lastStepResult) {
            is Continuation.LastStepResult.Failure,
            is Continuation.LastStepResult.SuccessfullyInvoked ->
                throw IllegalStateException(
                    "Invalid last step result type ${lastStepResult::class.simpleName} for rollback coroutine continuation"
                )
            is Continuation.LastStepResult.SuccessfullyRolledBack ->
                currentStep.rollback(this, lastStepResult.message, lastStepResult.throwable)
        }
}

fun DistributedCoroutine.buildRegularContinuation(
    connection: SqlConnection,
    cooperationContext: CooperationContext,
    lastSuspendedStepName: LastSuspendedStep,
    cooperationScopeIdentifier: ContinuationCooperationScopeIdentifier,
    message: Message,
    fetchCancellationException: (CooperationScope) -> Uni<List<CooperationException>>,
) =
    when (lastSuspendedStepName) {
        is NotSuspendedYet -> {
            // No SUSPEND record, so we've just started processing this message
            RegularCooperationContinuation(
                connection,
                cooperationContext,
                cooperationScopeIdentifier,
                message,
                SuspensionPoint.BeforeFirstStep(steps.first()),
                identifier,
                fetchCancellationException,
            )
        }

        is SuspendedAt -> {
            val suspendedStepIdx = steps.indexOfFirst { it.name == lastSuspendedStepName.stepName }
            if (suspendedStepIdx == -1) {
                throw IllegalStateException("Step $lastSuspendedStepName was not found")
            }

            if (steps[suspendedStepIdx] == steps.last()) {
                RegularCooperationContinuation(
                    connection,
                    cooperationContext,
                    cooperationScopeIdentifier,
                    message,
                    SuspensionPoint.AfterLastStep(steps.last()),
                    identifier,
                    fetchCancellationException,
                )
            } else {
                RegularCooperationContinuation(
                    connection,
                    cooperationContext,
                    cooperationScopeIdentifier,
                    message,
                    SuspensionPoint.BetweenSteps(
                        steps[suspendedStepIdx],
                        steps[suspendedStepIdx + 1],
                    ),
                    identifier,
                    fetchCancellationException,
                )
            }
        }
    }

fun DistributedCoroutine.buildRollbackContinuation(
    connection: SqlConnection,
    cooperationContext: CooperationContext,
    lastSuspendedStep: LastSuspendedStep,
    cooperationScopeIdentifier: ContinuationCooperationScopeIdentifier,
    message: Message,
    emitRollbacks: (scope: CooperationScope, stepName: String, throwable: Throwable) -> Uni<Unit>,
    fetchCancellationException: (CooperationScope) -> Uni<List<CooperationException>>,
): RollbackCooperationContinuation {
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
                    ): Uni<Unit> = emitRollbacks(scope, step.name, throwable)

                    override fun handleChildFailures(
                        scope: CooperationScope,
                        message: Message,
                        throwable: Throwable,
                    ): Uni<Unit> = step.handleChildFailures(scope, message, throwable)
                },
            )
        }

    return when (lastSuspendedStep) {
        is NotSuspendedYet -> {
            // We're rolling back before the first step committed. Since the
            // transaction itself was never committed, nothing was emitted, and we're done.
            RollbackCooperationContinuation(
                connection,
                cooperationContext,
                cooperationScopeIdentifier,
                message,
                SuspensionPoint.AfterLastStep(rollbackSteps.first()),
                identifier,
                fetchCancellationException,
            )
        }

        is SuspendedAt -> {

            val cleanLastSuspendedStepName =
                lastSuspendedStep.stepName.removePrefix(prefix).removeSuffix(suffix)

            val stepToRevert = steps.indexOfFirst { it.name == cleanLastSuspendedStepName }

            if (stepToRevert == -1) {
                throw IllegalStateException("Step $cleanLastSuspendedStepName was not found")
            }

            // TODO: Doc that this means "rollback just started" and it allows us to have this logic
            //  here, instead of in SQL. Although now that I think of if, adding a "rollback is the
            //  last event" might as well be made part of the SQL, since it's almost identical to
            //  what we do with contexts anyway
            if (cleanLastSuspendedStepName == lastSuspendedStep.stepName) {
                // We've doubled the number of steps by transforming each step into two substeps, so
                // we multiply by two, and we want to start with the last one (reverting child
                // scopes), so we add one
                val nextStepIdx = 2 * stepToRevert + 1
                RollbackCooperationContinuation(
                    connection,
                    cooperationContext,
                    cooperationScopeIdentifier,
                    message,
                    SuspensionPoint.BeforeFirstStep(rollbackSteps[nextStepIdx]),
                    identifier,
                    fetchCancellationException,
                )
            } else {
                val nextStepIdx =
                    rollbackSteps.indexOfFirst { it.name == lastSuspendedStep.stepName }
                if (rollbackSteps[nextStepIdx] == rollbackSteps.first()) {
                    RollbackCooperationContinuation(
                        connection,
                        cooperationContext,
                        cooperationScopeIdentifier,
                        message,
                        SuspensionPoint.AfterLastStep(rollbackSteps.first()),
                        identifier,
                        fetchCancellationException,
                    )
                } else {
                    RollbackCooperationContinuation(
                        connection,
                        cooperationContext,
                        cooperationScopeIdentifier,
                        message,
                        SuspensionPoint.BetweenSteps(
                            rollbackSteps[nextStepIdx],
                            rollbackSteps[nextStepIdx - 1],
                        ),
                        identifier,
                        fetchCancellationException,
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
