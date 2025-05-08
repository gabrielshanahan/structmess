package io.github.gabrielshanahan.structmess.coroutine

import io.github.gabrielshanahan.structmess.domain.Message
import io.github.gabrielshanahan.structmess.unify
import io.smallrye.mutiny.Uni

// TODO: Define try-finally helper. Or maybe just add it to the current builders?
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

    fun build(): Coroutine =
        Coroutine(CoroutineIdentifier(name), steps, cooperationHierarchyStrategy)
}

fun coroutine(
    name: String,
    cooperationHierarchyStrategy: CooperationHierarchyStrategy,
    block: CoroutineBuilder.() -> Unit,
): Coroutine = CoroutineBuilder(name, cooperationHierarchyStrategy).apply(block).build()
