package io.github.gabrielshanahan.scoop.blocking.coroutine.builder

import io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext

data object TryFinallyKey : CooperationContext.MappedKey<TryFinallyElement>()

data class TryFinallyElement(val finallyRunForSteps: List<String>) :
    CooperationContext.MappedElement(TryFinallyKey) {
    override fun plus(context: CooperationContext): CooperationContext =
        when (context) {
            is TryFinallyElement ->
                TryFinallyElement(finallyRunForSteps + context.finallyRunForSteps)
            else -> super.plus(context)
        }
}

fun CooperationScope.finallyRun(name: String) =
    context[TryFinallyKey]?.finallyRunForSteps?.contains(name) ?: false

fun CooperationScope.markFinallyRun(name: String) {
    context += TryFinallyElement(listOf(name))
}

fun SagaBuilder.tryFinallyStep(
    invoke: (CooperationScope, Message) -> Unit,
    finally: (CooperationScope, Message) -> Unit,
) {
    val name = steps.size.toString()
    step(
        invoke = { scope, message ->
            try {
                invoke(scope, message)
            } catch (e: Exception) {
                // In case the handler itself throws, and not a child scope,
                // we still want to run the `finally` block, but there's no point
                // in doing any checks or markings, since we never leave the step
                finally(scope, message)
                throw e
            }
        },
        rollback = { scope, message, throwable ->
            if (!scope.finallyRun(name)) {
                scope.markFinallyRun(name)
                finally(scope, message)
            }
        },
    )

    step { scope, message ->
        if (!scope.finallyRun(name)) {
            scope.markFinallyRun(name)
            finally(scope, message)
        }
    }
}
