package io.github.gabrielshanahan.scoop.shared.coroutine

import com.github.f4b6a3.uuid.UuidCreator

data class DistributedCoroutineIdentifier(val name: String, val instance: String) {
    constructor(name: String) : this(name, UuidCreator.getTimeOrderedEpoch().toString())
}

fun DistributedCoroutineIdentifier.renderAsString() = "$name[$instance]"
