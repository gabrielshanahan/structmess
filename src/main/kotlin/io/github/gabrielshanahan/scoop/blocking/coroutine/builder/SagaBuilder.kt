package io.github.gabrielshanahan.scoop.blocking.coroutine.builder

import io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.blocking.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.blocking.coroutine.TransactionalStep
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy

class SagaBuilder(val name: String, val eventLoopStrategy: EventLoopStrategy) {

    val steps: MutableList<TransactionalStep> = mutableListOf()

    fun step(
        name: String,
        invoke: (CooperationScope, Message) -> Unit,
        rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
        handleChildFailures: ((CooperationScope, Message, Throwable) -> Unit)? = null,
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
                ) =
                    rollback?.invoke(scope, message, throwable)
                        ?: super.rollback(scope, message, throwable)

                override fun handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                ) =
                    handleChildFailures?.invoke(scope, message, throwable)
                        ?: super.handleChildFailures(scope, message, throwable)
            }
        )
    }

    fun step(
        invoke: (CooperationScope, Message) -> Unit,
        rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
        handleChildFailures: ((CooperationScope, Message, Throwable) -> Unit)? = null,
    ) = step(steps.size.toString(), invoke, rollback, handleChildFailures)

    fun step(name: String, invoke: (CooperationScope, Message) -> Unit) =
        step(name, invoke, null, null)

    fun step(invoke: (CooperationScope, Message) -> Unit) =
        step(steps.size.toString(), invoke, null, null)

    fun build(): DistributedCoroutine =
        DistributedCoroutine(DistributedCoroutineIdentifier(name), steps, eventLoopStrategy)
}

fun saga(
    name: String,
    eventLoopStrategy: EventLoopStrategy,
    block: SagaBuilder.() -> Unit,
): DistributedCoroutine = SagaBuilder(name, eventLoopStrategy).apply(block).build()
