package io.github.gabrielshanahan.scoop.reactive.coroutine.continuation

import io.github.gabrielshanahan.scoop.reactive.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.reactive.coroutine.CoroutineState
import io.github.gabrielshanahan.scoop.reactive.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep
import io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.StructuredCooperationManager
import io.github.gabrielshanahan.scoop.reactive.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.LastSuspendedStep
import io.smallrye.mutiny.Uni
import io.vertx.mutiny.sqlclient.SqlConnection

// TODO: Config
const val ROLLING_BACK_PREFIX = "Rollback of "
const val ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX = " (rolling back child scopes)"

class RollbackPathContinuation(
    connection: SqlConnection,
    context: CooperationContext,
    cooperationScopeIdentifier: CooperationScopeIdentifier.Child,
    suspensionPoint: SuspensionPoint,
    distributedCoroutine: DistributedCoroutine,
    structuredCooperationManager: StructuredCooperationManager,
) :
    BaseCooperationContinuation(
        connection,
        context,
        cooperationScopeIdentifier,
        suspensionPoint,
        distributedCoroutine,
        structuredCooperationManager,
    ) {
    override fun giveUpStrategy(seen: String): String =
        distributedCoroutine.eventLoopStrategy.giveUpOnRollbackPath(seen)
}

fun DistributedCoroutine.buildRollbackPathContinuation(
    connection: SqlConnection,
    coroutineState: CoroutineState,
    structuredCooperationManager: StructuredCooperationManager,
): RollbackPathContinuation {
    val rollingBackPrefix = run {
        var prefix = ROLLING_BACK_PREFIX
        while (steps.any { it.name.startsWith(prefix) }) {
            prefix += "$"
        }
        prefix
    }
    val rollingBackSuffix = run {
        var suffix = ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX
        while (steps.any { it.name.endsWith(suffix) }) {
            suffix += "$"
        }
        suffix
    }

    val rollbackSteps =
        buildRollbackSteps(rollingBackPrefix, rollingBackSuffix, structuredCooperationManager)

    return when (coroutineState.lastSuspendedStep) {
        is LastSuspendedStep.NotSuspendedYet -> {
            // We're rolling back before the first step committed. Since the
            // transaction itself was never committed, nothing was emitted, and we're done.
            RollbackPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                SuspensionPoint.AfterLastStep(rollbackSteps.first()),
                this,
                structuredCooperationManager,
            )
        }

        is LastSuspendedStep.SuspendedAt -> {

            val cleanLastSuspendedStepName =
                coroutineState.lastSuspendedStep.stepName
                    .removePrefix(rollingBackPrefix)
                    .removeSuffix(rollingBackSuffix)

            val stepToRevert = steps.indexOfFirst { it.name == cleanLastSuspendedStepName }

            check(stepToRevert > -1) { "Step $cleanLastSuspendedStepName was not found" }

            // TODO: Doc that this means "rollback just started" and it allows us to have this logic
            //  here, instead of in SQL. Although now that I think of if, adding a "rollback is the
            //  last event" might as well be made part of the SQL, since it's almost identical to
            //  what we do with contexts anyway
            if (cleanLastSuspendedStepName == coroutineState.lastSuspendedStep.stepName) {
                // We've doubled the number of steps by transforming each step into two substeps, so
                // we multiply by two, and we want to start with the last one (reverting child
                // scopes), so we add one
                val nextStepIdx = 2 * stepToRevert + 1
                RollbackPathContinuation(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.scopeIdentifier,
                    SuspensionPoint.BeforeFirstStep(rollbackSteps[nextStepIdx]),
                    this,
                    structuredCooperationManager,
                )
            } else {
                val nextStepIdx =
                    rollbackSteps.indexOfFirst {
                        it.name == coroutineState.lastSuspendedStep.stepName
                    }
                if (rollbackSteps[nextStepIdx] == rollbackSteps.first()) {
                    RollbackPathContinuation(
                        connection,
                        coroutineState.cooperationContext,
                        coroutineState.scopeIdentifier,
                        SuspensionPoint.AfterLastStep(rollbackSteps.first()),
                        this,
                        structuredCooperationManager,
                    )
                } else {
                    RollbackPathContinuation(
                        connection,
                        coroutineState.cooperationContext,
                        coroutineState.scopeIdentifier,
                        SuspensionPoint.BetweenSteps(
                            rollbackSteps[nextStepIdx],
                            rollbackSteps[nextStepIdx - 1],
                        ),
                        this,
                        structuredCooperationManager,
                    )
                }
            }
        }
    }
}

private fun DistributedCoroutine.buildRollbackSteps(
    rollingBackPrefix: String,
    rollingBackSuffix: String,
    structuredCooperationManager: StructuredCooperationManager,
): List<TransactionalStep> =
    steps.flatMap { step ->
        listOf(
            object : TransactionalStep {
                override val name: String
                    get() = rollingBackPrefix + step.name

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
                    get() = rollingBackPrefix + step.name + rollingBackSuffix

                override fun invoke(scope: CooperationScope, message: Message) =
                    error("Should never be invoked")

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
