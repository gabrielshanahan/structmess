package io.github.gabrielshanahan.scoop.shared.coroutine.context

abstract class CancellationToken<SELF : CancellationToken<SELF>>(
    key: CooperationContext.MappedKey<*>
) : CooperationContext.MappedElement(key) {
    override fun plus(context: CooperationContext): CooperationContext =
        if (context is CancellationToken<SELF> && context.has(key)) {
            @Suppress("UNCHECKED_CAST") and(context as SELF)
        } else {
            super.plus(context)
        }

    abstract fun and(other: SELF): CancellationToken<SELF>
}
