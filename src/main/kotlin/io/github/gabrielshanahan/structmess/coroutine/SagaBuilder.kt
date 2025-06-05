package io.github.gabrielshanahan.structmess.coroutine

import io.github.gabrielshanahan.structmess.domain.Message
import io.github.gabrielshanahan.structmess.unify
import io.smallrye.mutiny.Uni

class SagaBuilder(val name: String, val eventLoopStrategy: EventLoopStrategy) {

    private val steps: MutableList<TransactionalStep> = mutableListOf()

    fun uniStep(
        name: String,
        invoke: (CooperationScope, Message) -> Uni<Unit>,
        rollback: ((CooperationScope, Message, Throwable) -> Uni<Unit>)? = null,
        handleChildFailures: ((CooperationScope, Message, Throwable) -> Uni<Unit>)? = null,
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

                override fun handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                ): Uni<Unit> =
                    handleChildFailures?.invoke(scope, message, throwable)
                        ?: super.handleChildFailures(scope, message, throwable)
            }
        )
    }

    fun uniStep(
        invoke: (CooperationScope, Message) -> Uni<Unit>,
        rollback: ((CooperationScope, Message, Throwable) -> Uni<Unit>)? = null,
        handleChildFailures: ((CooperationScope, Message, Throwable) -> Uni<Unit>)? = null,
    ) = uniStep(steps.size.toString(), invoke, rollback, handleChildFailures)

    fun uniStep(invoke: (CooperationScope, Message) -> Uni<Unit>) =
        uniStep(steps.size.toString(), invoke, null, null)

    fun step(
        name: String,
        invoke: (CooperationScope, Message) -> Unit,
        rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
        handleChildFailures: ((CooperationScope, Message, Throwable) -> Unit)? = null,
    ) = uniStep(name, unify(invoke), rollback?.let(::unify), handleChildFailures?.let(::unify))

    fun step(
        invoke: (CooperationScope, Message) -> Unit,
        rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
        handleChildFailures: ((CooperationScope, Message, Throwable) -> Unit)? = null,
    ) = step(steps.size.toString(), invoke, rollback, handleChildFailures)

    fun step(name: String, invoke: (CooperationScope, Message) -> Unit) =
        step(name, invoke, null, null)

    fun step(invoke: (CooperationScope, Message) -> Unit) =
        step(steps.size.toString(), invoke, null, null)

    fun tryFinallyStep(
        invoke: (CooperationScope, Message) -> Unit,
        finally: (CooperationScope, Message) -> Unit,
    ) {
        val name = steps.size.toString()
        step(
            invoke = { scope, message ->
                try {
                    invoke(scope, message)
                } catch (e: Throwable) {
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

    fun build(): DistributedCoroutine =
        DistributedCoroutine(DistributedCoroutineIdentifier(name), steps, eventLoopStrategy)
}

fun saga(
    name: String,
    eventLoopStrategy: EventLoopStrategy,
    block: SagaBuilder.() -> Unit,
): DistributedCoroutine = SagaBuilder(name, eventLoopStrategy).apply(block).build()

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
