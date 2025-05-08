package io.github.gabrielshanahan.structmess.coroutine

import com.github.f4b6a3.uuid.UuidCreator
import io.github.gabrielshanahan.structmess.domain.CooperationException
import io.github.gabrielshanahan.structmess.domain.Message
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.infrastructure.Infrastructure
import io.vertx.mutiny.sqlclient.SqlConnection
import java.util.UUID
import kotlin.time.Duration

// TODO: Config
const val ROLLING_BACK_PREFIX = "Rollback of "
const val ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX = " (rolling back child scopes)"

data class CoroutineIdentifier(val name: String, val instance: String) {
    constructor(name: String) : this(name, UuidCreator.getTimeOrderedEpoch().toString())
}

fun CoroutineIdentifier.renderAsString() = "$name[$instance]"

data class ContinuationIdentifier(
    val stepName: String,
    val coroutineIdentifier: CoroutineIdentifier,
)

// TODO: Doc that the suspension points are just after the last step finished, but before the new
// one started!
interface Continuation {
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
            Result // TODO: Emitted at the end of each step no matter what

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

    // TODO: HandleChildFailures
    fun handleScopeFailure(scope: CooperationScope, throwable: Throwable): Uni<Unit> {
        throw throwable
    }
}

// TODO: ChildStrategy or something
fun interface CooperationHierarchyStrategy {
    // TODO: anySeenEventMissingSQL
    fun anyMissingSeenSql(emissionsAlias: String, seenAlias: String): String
}

// TODO: allMustFinish
fun whenAllHandlersHaveCompleted(
    topicsToHandlerNames: Map<String, List<String>>
): CooperationHierarchyStrategy {
    val pairs =
        topicsToHandlerNames.flatMap { (topic, handlers) ->
            handlers.map { handler -> topic to handler }
        }

    return if (pairs.isEmpty()) {
        CooperationHierarchyStrategy { _, _ -> "true" }
    } else {
        CooperationHierarchyStrategy { emissionsAlias, seenAlias ->
            val valuesClause =
                pairs.joinToString(", ") { (topic, handler) -> "('$topic', '$handler')" }
            // Exists missing
            """
            EXISTS (
                SELECT
                    1
                FROM (VALUES $valuesClause) AS topic_handler(topic, handler)
                JOIN message ON topic_handler.topic = message.topic
                JOIN $emissionsAlias ON $emissionsAlias.message_id = message.id
                LEFT JOIN 
                    $seenAlias ON $seenAlias.message_id = $emissionsAlias.message_id
                    AND $seenAlias.coroutine_name = topic_handler.handler
                WHERE $seenAlias.id is NULL
            )    
            """
                .trimIndent()
        }
    }
}

// TODO: TimeMustElapse
fun whenTimeHasElapsed(duration: Duration) = CooperationHierarchyStrategy { emissionsAlias, _ ->
    "$emissionsAlias.created_at > now() - INTERVAL '${duration.inWholeSeconds} second'"
}

class Coroutine(
    val identifier: CoroutineIdentifier,
    val steps: List<TransactionalStep>,
    val cooperationHierarchyStrategy: CooperationHierarchyStrategy,
) {
    init {
        if (steps.isEmpty()) {
            throw IllegalArgumentException("Steps cannot be empty")
        }
    }
}

data class CooperationRoot(val cooperationLineage: List<UUID>, val message: Message)

// TODO: Doc that the scope stays the same for entire coroutine run! Even including ROLLING_BACK
interface CooperationScope {
    val cooperationRoot: CooperationRoot

    var context: CooperationContext

    // TODO: Should instead contain continuation, implemented as "this" + Continuation interface
    //  should contain this identifier
    val identifier: ContinuationIdentifier
    val connection: SqlConnection
    val emittedMessages: List<Message>

    fun emitted(message: Message)

    fun ensureActive(): Uni<Unit>
}

sealed interface SuspensionPoint {
    data class BeforeFirstStep(val firstStep: TransactionalStep) : SuspensionPoint

    data class BetweenSteps(val previousStep: TransactionalStep, val nextStep: TransactionalStep) :
        SuspensionPoint

    data class AfterLastStep(val lastStep: TransactionalStep) : SuspensionPoint
}

// TODO: doc that the continuation is equivalent to the scope (demonstrate graphically?)
// TODO: Rename to CoroutineContinuation?
interface CooperativeContinuation : Continuation, CooperationScope

class CooperativeContinuationImpl(
    override val connection: SqlConnection,
    override var context: CooperationContext,
    override val cooperationRoot: CooperationRoot,
    private val suspensionPoint: SuspensionPoint,
    private val coroutineIdentifier: CoroutineIdentifier,
    private val fetchCancellationExceptions: (CooperationScope) -> Uni<List<CooperationException>>,
) : CooperativeContinuation {

    // TODO: Doc that this is so that emitted & rollback correctly mark steps
    private lateinit var currentStep: TransactionalStep

    override val emittedMessages: MutableList<Message> = mutableListOf()

    override fun resumeWith(
        lastStepResult: Continuation.LastStepResult
    ): Uni<out Continuation.Result> {

        fun handlerFailuresAndResume(): Uni<out Continuation.Result> {

            fun resume(action: () -> Uni<Unit>) =
                when (suspensionPoint) {
                    is SuspensionPoint.AfterLastStep ->
                        Uni.createFrom().item(Continuation.Result.Success)
                    is SuspensionPoint.BeforeFirstStep,
                    is SuspensionPoint.BetweenSteps ->
                        try {
                            action()
                                .replaceWith {
                                    Continuation.Result.Suspend(emittedMessages)
                                        as Continuation.Result
                                }
                                .onFailure()
                                .recoverWithItem { e -> Continuation.Result.Failure(e) }
                        } catch (e: Exception) {
                            Uni.createFrom().item(Continuation.Result.Failure(e))
                        }
                }

            return when (lastStepResult) {
                is Continuation.LastStepResult.Failure ->
                    try {
                        currentStep
                            .handleScopeFailure(this, lastStepResult.throwable)
                            // TODO: Test this! What happens when we get a scope failure in
                            // AfterLastStep? Do we ever get multiple SUSPEND records with the same
                            // step, will that work?
                            .replaceWith {
                                Continuation.Result.Suspend(emittedMessages) as Continuation.Result
                            }
                            .onFailure()
                            .recoverWithItem { e -> Continuation.Result.Failure(e) }
                    } catch (e: Exception) {
                        Uni.createFrom().item(Continuation.Result.Failure(e))
                    }

                is Continuation.LastStepResult.SuccessfullyInvoked ->
                    resume {
                        ensureActive()
                            .replaceWith(currentStep.invoke(this, lastStepResult.message))
                            .replaceWith(ensureActive())
                    }

                // TODO: have implementation of 'Rollback' and 'Normal'continuation that would
                //  differ only in ensureActive().invoke.ensureActive() vs. .rollback. This means
                //  we wouldn't need to distinguish between SuccessfullyInvoked and
                // SuccessfullyRolledBack
                //  and it would also solve the problem with the boolean flag in buildContinuation
                is Continuation.LastStepResult.SuccessfullyRolledBack ->
                    resume {
                        currentStep.rollback(this, lastStepResult.message, lastStepResult.throwable)
                    }
            }
        }

        return when (suspensionPoint) {
            is SuspensionPoint.BeforeFirstStep -> {
                currentStep = suspensionPoint.firstStep
                handlerFailuresAndResume()
            }
            is SuspensionPoint.BetweenSteps ->
                when (lastStepResult) {
                    is Continuation.LastStepResult.Failure -> {
                        currentStep = suspensionPoint.previousStep
                        handlerFailuresAndResume()
                    }
                    is Continuation.LastStepResult.SuccessfullyInvoked,
                    is Continuation.LastStepResult.SuccessfullyRolledBack -> {
                        currentStep = suspensionPoint.nextStep
                        handlerFailuresAndResume()
                    }
                }
            is SuspensionPoint.AfterLastStep -> {
                currentStep = suspensionPoint.lastStep
                handlerFailuresAndResume()
            }
        }
    }

    override val identifier: ContinuationIdentifier
        get() = ContinuationIdentifier(currentStep.name, coroutineIdentifier)

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
}

// TODO: Change this into not-so-many nullable things
fun Coroutine.buildContinuation(
    connection: SqlConnection,
    cooperationContext: CooperationContext,
    // TODO: Separate datatype for steps that knows how to render itself? And we can
    //  have FirstStep as well.
    //  At the very least have the step be initialized when fetched from the DB (since we
    //  have the coroutine there). Think about who's responsibility is it to deal with steps.
    lastSuspendedStepName: String?,
    cooperationRoot: CooperationRoot,
    // TODO: Build rollbackContinuation
    rollingBack: Boolean,
    emitRollbacks: (scope: CooperationScope, stepName: String, throwable: Throwable) -> Uni<Unit>,
    fetchCancellationException: (CooperationScope) -> Uni<List<CooperationException>>,
) =
    if (!rollingBack) {
        if (lastSuspendedStepName == null) {
            // No SUSPEND record, so we've just started processing this message
            CooperativeContinuationImpl(
                connection,
                cooperationContext,
                cooperationRoot,
                SuspensionPoint.BeforeFirstStep(steps.first()),
                identifier,
                fetchCancellationException,
            )
        } else {
            val suspendedStepIdx = steps.indexOfFirst { it.name == lastSuspendedStepName }
            if (suspendedStepIdx == -1) {
                throw IllegalStateException("Step $lastSuspendedStepName was not found")
            }

            if (steps[suspendedStepIdx] == steps.last()) {
                CooperativeContinuationImpl(
                    connection,
                    cooperationContext,
                    cooperationRoot,
                    SuspensionPoint.AfterLastStep(steps.last()),
                    identifier,
                    fetchCancellationException,
                )
            } else {
                CooperativeContinuationImpl(
                    connection,
                    cooperationContext,
                    cooperationRoot,
                    SuspensionPoint.BetweenSteps(
                        steps[suspendedStepIdx],
                        steps[suspendedStepIdx + 1],
                    ),
                    identifier,
                    fetchCancellationException,
                )
            }
        }
    } else {
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

                        override fun handleScopeFailure(
                            scope: CooperationScope,
                            throwable: Throwable,
                        ): Uni<Unit> = step.handleScopeFailure(scope, throwable)
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

                        override fun handleScopeFailure(
                            scope: CooperationScope,
                            throwable: Throwable,
                        ): Uni<Unit> = step.handleScopeFailure(scope, throwable)
                    },
                )
            }

        if (lastSuspendedStepName == null) {
            // No SUSPEND record, so we're rolling back before the first step committed. Since the
            // transaction itself was never committed, nothing was emitted, and we're done.
            CooperativeContinuationImpl(
                connection,
                cooperationContext,
                cooperationRoot,
                SuspensionPoint.AfterLastStep(rollbackSteps.first()),
                identifier,
                fetchCancellationException,
            )
        } else {
            val cleanLastSuspendedStepName =
                lastSuspendedStepName.removePrefix(prefix).removeSuffix(suffix)

            val stepToRevert = steps.indexOfFirst { it.name == cleanLastSuspendedStepName }

            if (stepToRevert == -1) {
                throw IllegalStateException("Step $cleanLastSuspendedStepName was not found")
            }

            // TODO: Doc that this means "rollback just started" and it allows us to have this logic
            //  here, instead of in SQL. Although now that I think of if, adding a "rollback is the
            //  last event" might as well be made part of the SQL, since it's almost identical to
            //  what we do with contexts anyway
            if (cleanLastSuspendedStepName == lastSuspendedStepName) {
                // We've doubled the number of steps by transforming each step into two substeps, so
                // we multiply by two, and we want to start with the last one (reverting child
                // scopes), so we add one
                val nextStepIdx = 2 * stepToRevert + 1
                CooperativeContinuationImpl(
                    connection,
                    cooperationContext,
                    cooperationRoot,
                    SuspensionPoint.BeforeFirstStep(rollbackSteps[nextStepIdx]),
                    identifier,
                    fetchCancellationException,
                )
            } else {
                val nextStepIdx = rollbackSteps.indexOfFirst { it.name == lastSuspendedStepName }
                if (rollbackSteps[nextStepIdx] == rollbackSteps.first()) {
                    CooperativeContinuationImpl(
                        connection,
                        cooperationContext,
                        cooperationRoot,
                        SuspensionPoint.AfterLastStep(rollbackSteps.first()),
                        identifier,
                        fetchCancellationException,
                    )
                } else {
                    CooperativeContinuationImpl(
                        connection,
                        cooperationContext,
                        cooperationRoot,
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
