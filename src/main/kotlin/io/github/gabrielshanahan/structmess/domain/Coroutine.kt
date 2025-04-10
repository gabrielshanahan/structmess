package io.github.gabrielshanahan.structmess.domain

import com.github.f4b6a3.uuid.UuidCreator
import io.github.gabrielshanahan.structmess.domain.ContinuationResult.*
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
    fun resumeWith(lastStepResult: LastStepResult): Uni<out ContinuationResult>
}

sealed interface LastStepResult {
    val message: Message

    data class SuccessfullyInvoked(override val message: Message) : LastStepResult

    data class SuccessfullyRolledBack(override val message: Message, val throwable: Throwable) :
        LastStepResult

    data class Failure(override val message: Message, val throwable: Throwable) : LastStepResult
}

sealed interface ContinuationResult {
    data class Suspend(val emittedMessages: List<Message>) :
        ContinuationResult // TODO: Emitted at the end of each step no matter what

    data object Success : ContinuationResult

    data class Failure(val exception: Throwable) : ContinuationResult
}

// TODO: Doc what these states mean
interface TransactionalStep {
    val name: String

    fun invoke(scope: CooperationScope, message: Message): Uni<Unit>

    fun rollback(scope: CooperationScope, message: Message, throwable: Throwable): Uni<Unit> =
        Uni.createFrom().item(Unit)

    fun handleScopeFailure(scope: CooperationScope, throwable: Throwable): Uni<Unit> {
        throw throwable
    }
}

// TODO: ChildDeterminationStrategy or something
fun interface CooperationHierarchyStrategy {
    fun anyMissingSeenSql(emissionsAlias: String, seenAlias: String): String
}

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

class CoroutineBuilder(
    val name: String,
    val cooperationHierarchyStrategy: CooperationHierarchyStrategy,
) {

    private val steps: MutableList<TransactionalStep> = mutableListOf()

    fun uniStep(
        name: String,
        invoke: (CooperationScope, Message) -> Uni<Unit>,
        rollback: ((CooperationScope, Message, Throwable) -> Uni<Unit>)? = null,
        handleScopeFailure: ((CooperationScope, Throwable) -> Uni<Unit>)? = null,
    ) {
        steps.add(
            object : TransactionalStep {
                override val name: String
                    get() = name

                override fun invoke(scope: CooperationScope, message: Message) =
                    invoke(scope, message)

                override fun rollback(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                ): Uni<Unit> =
                    rollback?.invoke(scope, message, throwable)
                        ?: super.rollback(scope, message, throwable)

                override fun handleScopeFailure(
                    scope: CooperationScope,
                    throwable: Throwable,
                ): Uni<Unit> =
                    handleScopeFailure?.invoke(scope, throwable)
                        ?: super.handleScopeFailure(scope, throwable)
            }
        )
    }

    fun uniStep(
        invoke: (CooperationScope, Message) -> Uni<Unit>,
        rollback: ((CooperationScope, Message, Throwable) -> Uni<Unit>)? = null,
        handleScopeFailure: ((CooperationScope, Throwable) -> Uni<Unit>)? = null,
    ) = uniStep(steps.size.toString(), invoke, rollback, handleScopeFailure)

    fun uniStep(invoke: (CooperationScope, Message) -> Uni<Unit>) =
        uniStep(steps.size.toString(), invoke, null, null)

    fun step(
        name: String,
        invoke: (CooperationScope, Message) -> Unit,
        rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
        handleScopeFailure: ((CooperationScope, Throwable) -> Unit)? = null,
    ) = uniStep(name, unify(invoke), rollback?.let(::unify), handleScopeFailure?.let(::unify))

    fun step(
        invoke: (CooperationScope, Message) -> Unit,
        rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
        handleScopeFailure: ((CooperationScope, Throwable) -> Unit)? = null,
    ) = step(steps.size.toString(), invoke, rollback, handleScopeFailure)

    fun step(name: String, invoke: (CooperationScope, Message) -> Unit) =
        step(name, invoke, null, null)

    fun step(invoke: (CooperationScope, Message) -> Unit) =
        step(steps.size.toString(), invoke, null, null)

    private fun <T, R, S> unify(f: (T, R) -> S): (T, R) -> Uni<S> = { t, r ->
        Uni.createFrom().item(f(t, r))
    }

    private fun <T, R, S, U> unify(f: (T, R, S) -> U): (T, R, S) -> Uni<U> = { t, r, s ->
        Uni.createFrom().item(f(t, r, s))
    }

    fun build(): Coroutine =
        Coroutine(CoroutineIdentifier(name), steps, cooperationHierarchyStrategy)
}

fun coroutine(
    name: String,
    cooperationHierarchyStrategy: CooperationHierarchyStrategy,
    block: CoroutineBuilder.() -> Unit,
): Coroutine = CoroutineBuilder(name, cooperationHierarchyStrategy).apply(block).build()

data class CooperationRoot(val cooperationLineage: List<UUID>, val message: Message)

// TODO: Doc that the scope stays the same for entire coroutine run! Even including ROLLING_BACK
interface CooperationScope {
    val cooperationRoot: CooperationRoot

    var context: CooperationContext

    // TODO: Should instead contain continuation, implemented as "this" + Continuation interface
    // should contain this identifier
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

// TODO: nuke this interface, no point
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

    override fun resumeWith(lastStepResult: LastStepResult): Uni<out ContinuationResult> {

        fun handlerFailuresAndResume(): Uni<out ContinuationResult> {

            fun resume(action: () -> Uni<Unit>) =
                when (suspensionPoint) {
                    is SuspensionPoint.AfterLastStep -> Uni.createFrom().item(Success)
                    is SuspensionPoint.BeforeFirstStep,
                    is SuspensionPoint.BetweenSteps ->
                        try {
                            action()
                                .replaceWith { Suspend(emittedMessages) as ContinuationResult }
                                .onFailure()
                                .recoverWithItem { e -> Failure(e) }
                        } catch (e: Exception) {
                            Uni.createFrom().item(Failure(e))
                        }
                }

            return when (lastStepResult) {
                is LastStepResult.Failure ->
                    try {
                        currentStep
                            .handleScopeFailure(this, lastStepResult.throwable)
                            // TODO: Test this! What happens when we get a scope failure in
                            // AfterLastStep? Do we ever get multiple SUSPEND records with the same
                            // step, will that work?
                            .replaceWith { Suspend(emittedMessages) as ContinuationResult }
                            .onFailure()
                            .recoverWithItem { e -> Failure(e) }
                    } catch (e: Exception) {
                        Uni.createFrom().item(Failure(e))
                    }

                is LastStepResult.SuccessfullyInvoked ->
                    resume {
                        ensureActive()
                            .replaceWith(currentStep.invoke(this, lastStepResult.message))
                            .replaceWith(ensureActive())
                    }

                is LastStepResult.SuccessfullyRolledBack ->
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
                    is LastStepResult.Failure -> {
                        currentStep = suspensionPoint.previousStep
                        handlerFailuresAndResume()
                    }
                    is LastStepResult.SuccessfullyInvoked,
                    is LastStepResult.SuccessfullyRolledBack -> {
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
        fetchCancellationExceptions(this)
            .emitOn(Infrastructure.getDefaultWorkerPool())
            .invoke { exceptions ->
                if (exceptions.any()) {
                    throw CancellationRequestedException(
                        "Cancellation request received",
                        exceptions,
                    )
                }
            }
            .replaceWith(Unit)
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
            // transaction itself
            // was never committed, nothing was emitted, and we're done.
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

            if (cleanLastSuspendedStepName == lastSuspendedStepName) {
                // We've doubled the number of steps by transforming each step into two substeps, so
                // we multiply by
                // two, and we want to start with the last one (reverting child scopes), so we add
                // one
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

// TODO: refactor so that this is rendered specially in CooperationFailure, eg.g
// $$CANCELLATION_REQUESTED$$
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

// TODO: refactor so that this is rendered specially in CooperationFailure, eg.g
// $$ROLLBACK_REQUESTED$$
class RollbackRequestedException(reason: String, causes: List<CooperationException> = emptyList()) :
    ScopeException(reason, causes.firstOrNull(), true) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
        // Point to the place where rollback() was actually called
        stackTrace = stackTrace.drop(1).toTypedArray()
    }
}
