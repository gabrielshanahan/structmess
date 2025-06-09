package io.github.gabrielshanahan.scoop.reactive

import io.smallrye.mutiny.Uni

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
