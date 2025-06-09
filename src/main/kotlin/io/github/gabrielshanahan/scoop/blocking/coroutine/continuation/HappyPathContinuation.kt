package io.github.gabrielshanahan.scoop.blocking.coroutine.continuation

import io.github.gabrielshanahan.scoop.blocking.coroutine.CoroutineState
import io.github.gabrielshanahan.scoop.blocking.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.StructuredCooperationManager
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.LastSuspendedStep
import java.sql.Connection

class HappyPathContinuation(
    connection: Connection,
    context: CooperationContext,
    scopeIdentifier: CooperationScopeIdentifier.Child,
    suspensionPoint: SuspensionPoint,
    distributedCoroutine: DistributedCoroutine,
    structuredCooperationManager: StructuredCooperationManager,
) :
    BaseCooperationContinuation(
        connection,
        context,
        scopeIdentifier,
        suspensionPoint,
        distributedCoroutine,
        structuredCooperationManager,
    ) {
    override fun giveUpStrategy(seen: String): String =
        distributedCoroutine.eventLoopStrategy.giveUpOnHappyPath(seen)
}

fun DistributedCoroutine.buildHappyPathContinuation(
    connection: Connection,
    coroutineState: CoroutineState,
    structuredCooperationManager: StructuredCooperationManager,
) =
    when (coroutineState.lastSuspendedStep) {
        is LastSuspendedStep.NotSuspendedYet -> {
            // No SUSPEND record, so we've just started processing this message
            HappyPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                SuspensionPoint.BeforeFirstStep(steps.first()),
                this,
                structuredCooperationManager,
            )
        }

        is LastSuspendedStep.SuspendedAt -> {
            val suspendedStepIdx =
                steps.indexOfFirst { it.name == coroutineState.lastSuspendedStep.stepName }

            check(suspendedStepIdx > -1) {
                "Step ${coroutineState.lastSuspendedStep} was not found"
            }

            if (steps[suspendedStepIdx] == steps.last()) {
                HappyPathContinuation(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.scopeIdentifier,
                    SuspensionPoint.AfterLastStep(steps.last()),
                    this,
                    structuredCooperationManager,
                )
            } else {
                HappyPathContinuation(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.scopeIdentifier,
                    SuspensionPoint.BetweenSteps(
                        steps[suspendedStepIdx],
                        steps[suspendedStepIdx + 1],
                    ),
                    this,
                    structuredCooperationManager,
                )
            }
        }
    }
