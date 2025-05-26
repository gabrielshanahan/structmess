package io.github.gabrielshanahan.structmess.messaging

interface HandlerRegistry {
    fun listenersByTopic(): Map<String, List<String>>
}
