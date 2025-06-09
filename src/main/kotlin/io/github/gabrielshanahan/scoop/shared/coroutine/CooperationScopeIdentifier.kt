package io.github.gabrielshanahan.scoop.shared.coroutine

import java.util.UUID

sealed interface CooperationScopeIdentifier {
    val cooperationLineage: List<UUID>

    data class Root(val cooperationId: UUID) : CooperationScopeIdentifier {
        override val cooperationLineage = listOf(cooperationId)
    }

    data class Child(override val cooperationLineage: List<UUID>) : CooperationScopeIdentifier
}
