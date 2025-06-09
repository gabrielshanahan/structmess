package io.github.gabrielshanahan.scoop.shared.coroutine.continuation

import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier

data class ContinuationIdentifier(
    val stepName: String,
    val distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
)
