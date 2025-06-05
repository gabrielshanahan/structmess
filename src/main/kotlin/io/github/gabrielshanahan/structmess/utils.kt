package io.github.gabrielshanahan.structmess

import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.groups.MultiTimePeriod
import java.time.Duration

fun MultiTimePeriod.everyJittered(duration: Duration): Multi<Long> =
    every(duration)
        .onOverflow()
        .drop() // avoid backpressure issues with delay
        .onItem()
        .transformToUniAndConcatenate { tick ->
            val jitterMillis = duration.toMillis() * 0.02
            Uni.createFrom()
                .item(tick)
                .onItem()
                .delayIt()
                .by(Duration.ofMillis(jitterMillis.toLong()))
        }

fun <T, R> Uni<T>.flatMapNonNull(mapper: (T) -> Uni<R>): Uni<R> = flatMap {
    if (it == null) {
        Uni.createFrom().nullItem()
    } else {
        mapper(it)
    }
}

fun <T, R> Uni<T>.mapNonNull(mapper: (T) -> R): Uni<R> = map { it?.let(mapper) }

fun <T, R, S> unify(f: (T, R) -> S): (T, R) -> Uni<S> = { t, r -> Uni.createFrom().item(f(t, r)) }

fun <T, R, S, U> unify(f: (T, R, S) -> U): (T, R, S) -> Uni<U> = { t, r, s ->
    Uni.createFrom().item(f(t, r, s))
}
