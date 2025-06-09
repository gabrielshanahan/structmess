package io.github.gabrielshanahan.scoop.reactive.messaging

import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.StandardEventLoopStrategy
import java.time.OffsetDateTime

interface HandlerRegistry {
    fun listenersByTopic(): Map<String, List<String>>
}

fun HandlerRegistry.eventLoopStrategy() =
    StandardEventLoopStrategy(OffsetDateTime.now(), this::listenersByTopic)
