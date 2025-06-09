package io.github.gabrielshanahan.scoop.reactive.coroutine

import io.github.gabrielshanahan.scoop.reactive.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy
import io.smallrye.mutiny.Uni

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
    val eventLoopStrategy: EventLoopStrategy,
) {
    init {
        check(steps.isNotEmpty()) { "Steps cannot be empty" }
        check(steps.mapTo(mutableSetOf()) { it.name }.size == steps.size) {
            "Steps must have unique names"
        }
    }
}
