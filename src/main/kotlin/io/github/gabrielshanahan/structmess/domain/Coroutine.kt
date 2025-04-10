package io.github.gabrielshanahan.structmess.domain

import com.github.f4b6a3.uuid.UuidCreator
import io.vertx.mutiny.sqlclient.SqlConnection
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class CoroutineIdentifier(val name: String, val instance: String) {
    constructor(name: String) : this(name, UuidCreator.getTimeOrderedEpoch().toString())
}

data class ContinuationIdentifier(
    val stepName: String,
    val coroutineIdentifier: CoroutineIdentifier,
)

// TODO: Doc that the suspension points are just after the last step finished, but before the new
// one started!
interface Continuation {
    fun resumeWith(lastStepResult: StepResult): ContinuationResult
}

sealed interface StepResult {
    val message: Message

    data class Success(override val message: Message) : StepResult

    data class Failure(override val message: Message, val exception: Exception) : StepResult
}

sealed interface ContinuationResult {
    data object Suspend : ContinuationResult

    data object Commit : ContinuationResult

    data class Rollback(val exception: Exception) : ContinuationResult
}

interface TransactionalStep {
    val name: String

    fun invoke(scope: CooperationScope, message: Message)

    fun handleScopeFailure(scope: CooperationScope, exception: Exception) {
        throw exception
    }
}

interface SupervisorTransactionalStep : TransactionalStep {
    override fun handleScopeFailure(scope: CooperationScope, exception: Exception) {}
}

fun interface CooperationHierarchyStrategy {
    fun sql(): String
}

fun whenAllHandlersHaveCompleted(topicsToHandlerNames: Map<String, List<String>>): CooperationHierarchyStrategy {
    val pairs = topicsToHandlerNames.flatMap { (topic, handlers) ->
        handlers.map { handler -> topic to handler }
    }

    return if (pairs.isEmpty()) {
        CooperationHierarchyStrategy {
            "true"
        }
    } else {
        CooperationHierarchyStrategy {
            val valuesClause = pairs.joinToString(", ") { (topic, handler) ->
                "('${topic.replace("'", "''")}', '${handler.replace("'", "''")}')"
            }

            """
            EXISTS (
                SELECT 1
                FROM (VALUES $valuesClause) AS topic_handler(topic, handler)
                WHERE topic_handler.topic = messages.topic
                  AND topic_handler.handler = seen.handler_name
            )
            """.trimIndent()
        }
    }
}

fun whenTimeHasElapsed(duration: Duration) = CooperationHierarchyStrategy {
    "emitted_in_step.created_at <= now() - INTERVAL '${duration.inWholeSeconds} second'"
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

class CoroutineBuilder(val name: String, val cooperationHierarchyStrategy: CooperationHierarchyStrategy) {

    private val steps: MutableList<StepBuilder> = mutableListOf()

    fun step(invoke: (CooperationScope, Message) -> Unit) = StepStage(steps.size.toString(), invoke).also(steps::add)
    fun step(name: String, invoke: (CooperationScope, Message) -> Unit) = StepStage(name, invoke).also(steps::add)

    fun build(): Coroutine = Coroutine(CoroutineIdentifier(name), steps.map { it.build() }, cooperationHierarchyStrategy)

    inner class StepStage(val name: String, val invoke: (CooperationScope, Message) -> Unit): StepBuilder {
        infix fun handleScopeFailure(handleScopeFailure: (CooperationScope, Exception) -> Unit) {
            ScopeFailureStage(name, invoke, handleScopeFailure).also {
                steps.replaceAll { step -> if (step == this) it else step }
            }
        }

        override fun build(): TransactionalStep = object : TransactionalStep {
            override val name: String
                get() = this@StepStage.name

            override fun invoke(scope: CooperationScope, message: Message) = this@StepStage.invoke(scope, message)
        }
    }

    inner class ScopeFailureStage(val name: String, val invoke: (CooperationScope, Message) -> Unit, val handleScopeFailure: (CooperationScope, Exception) -> Unit): StepBuilder {
        override fun build(): TransactionalStep = object : TransactionalStep {
            override val name: String
                get() = this@ScopeFailureStage.name

            override fun invoke(scope: CooperationScope, message: Message) = this@ScopeFailureStage.invoke(scope, message)

            override fun handleScopeFailure(scope: CooperationScope, exception: Exception) = this@ScopeFailureStage.handleScopeFailure(scope, exception)
        }
    }

    sealed interface StepBuilder {
        fun build(): TransactionalStep
    }
}

fun coroutine(name: String, cooperationHierarchyStrategy: CooperationHierarchyStrategy, block: CoroutineBuilder.() -> Unit): Coroutine = CoroutineBuilder(name, cooperationHierarchyStrategy).apply(block).build()


interface CooperationScope {
    val cooperationLineage: List<UUID>
    val continuationIdentifier: ContinuationIdentifier // context
    val connection: SqlConnection // Context

    fun emitted(message: Message)
}

sealed interface CoroutineExecutionState {
    data class Starting(val firstStep: TransactionalStep) : CoroutineExecutionState

    data class InProgress(val previousStep: TransactionalStep, val nextStep: TransactionalStep) :
        CoroutineExecutionState

    data class Finishing(val lastStep: TransactionalStep) : CoroutineExecutionState
}

class CooperativeContinuation(
    override val connection: SqlConnection,
    private val ancestorCooperationLineage: List<UUID>,
    private val coroutineExecutionState: CoroutineExecutionState,
    private val coroutineIdentifier: CoroutineIdentifier,
) : Continuation, CooperationScope {

    private val cooperationId: UUID = UuidCreator.getTimeOrderedEpoch()

    // TODO: Doc that this is so that emitted & rollback correctly mark steps
    private lateinit var currentStep: TransactionalStep

    override val cooperationLineage: List<UUID>
        get() = ancestorCooperationLineage + cooperationId

    private val emittedMessages: MutableList<Message> = mutableListOf()

    override fun resumeWith(lastStepResult: StepResult): ContinuationResult {

        fun runStep(): ContinuationResult {
            if (lastStepResult is StepResult.Failure) {
                try {
                    currentStep.handleScopeFailure(this, lastStepResult.exception)
                } catch (e: Exception) {
                    return ContinuationResult.Rollback(e)
                }
            }

            return try {
                currentStep.invoke(this, lastStepResult.message)
                if (emittedMessages.isNotEmpty()) ContinuationResult.Suspend
                else ContinuationResult.Commit
            } catch (e: Exception) {
                return ContinuationResult.Rollback(e)
            }
        }

        return when (coroutineExecutionState) {
            is CoroutineExecutionState.Starting -> {
                currentStep = coroutineExecutionState.firstStep
                runStep()
            }
            is CoroutineExecutionState.InProgress ->
                when (lastStepResult) {
                    is StepResult.Success -> {
                        currentStep = coroutineExecutionState.nextStep
                        runStep()
                    }
                    is StepResult.Failure -> {
                        currentStep = coroutineExecutionState.previousStep
                        runStep()
                    }
                }
            is CoroutineExecutionState.Finishing -> {
                currentStep = coroutineExecutionState.lastStep
                runStep()
            }
        }
    }

    override val continuationIdentifier: ContinuationIdentifier
        get() = ContinuationIdentifier(currentStep.name, coroutineIdentifier)

    override fun emitted(message: Message) {
        emittedMessages.add(message)
    }
}

//TODO: Show how we can use this even outside handlers! - EventLoop should be a separate thing!
fun Coroutine.buildContinuation(connection: SqlConnection, currentStepName: String?, ancestorCooperationLineage: List<UUID>) =
    if (currentStepName == null) {
        // No SUSPEND record, so we've just started processing this message
        CooperativeContinuation(
            connection,
            ancestorCooperationLineage,
            CoroutineExecutionState.Starting(steps.first()),
            identifier,
        )
    } else {
        val previousStepIndex = steps.indexOfFirst { it.name == currentStepName }
        if (previousStepIndex == -1) {
            throw IllegalStateException("Step $currentStepName was not found")
        }
        val nextStepIndex = previousStepIndex + 1

        if (nextStepIndex == steps.size) {
            CooperativeContinuation(
                connection,
                ancestorCooperationLineage,
                CoroutineExecutionState.Finishing(steps.last()),
                identifier,
            )
        } else {
            CooperativeContinuation(
                connection,
                ancestorCooperationLineage,
                CoroutineExecutionState.InProgress(steps[previousStepIndex], steps[nextStepIndex]),
                identifier,
            )
        }
    }

class CooperationScopeRolledBackException : Exception()
