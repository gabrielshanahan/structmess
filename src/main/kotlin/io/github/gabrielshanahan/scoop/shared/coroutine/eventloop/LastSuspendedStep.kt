package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop

sealed interface LastSuspendedStep {
    data object NotSuspendedYet : LastSuspendedStep

    data class SuspendedAt(val stepName: String) : LastSuspendedStep
}
