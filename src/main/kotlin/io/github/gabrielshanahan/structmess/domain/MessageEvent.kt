package io.github.gabrielshanahan.structmess.domain

import java.util.UUID

/**
 * Enum representing the different types of ledger records that track the lifecycle of message
 * handling.
 */
enum class MessageEventType {
    /** Message was initially emitted */
    EMITTED,

    /** Message was seen by a handler */
    SEEN,

    SUSPENDED,

    /** Message was successfully handled, effects were commited */
    COMMITTED,

    /** Message was not successfully handled, effects were rolled back */
    ROLLED_BACK,
}

// TODO: Cleanup
sealed interface Ledger {
    val id: UUID
    val messageId: UUID
    val scopeLineage: List<UUID>
}

data class Emitted(
    override val id: UUID,
    override val messageId: UUID,
    override val scopeLineage: List<UUID>,
    val emittedByCoroutine: Coroutine?,
    val emittedFromScope: CooperationScope?,
) : Ledger

// This is what is locked
data class Seen(
    override val id: UUID,
    override val messageId: UUID,
    override val scopeLineage: List<UUID>,
    val coroutine: Coroutine,
) : Ledger

data class Commited(
    override val id: UUID,
    override val messageId: UUID,
    override val scopeLineage: List<UUID>,
    val coroutine: Coroutine,
    val scope: CooperationScope?,
) : Ledger

data class RolledBack(
    override val id: UUID,
    override val messageId: UUID,
    override val scopeLineage: List<UUID>,
    val coroutine: Coroutine,
    val scope: CooperationScope?,
) : Ledger
