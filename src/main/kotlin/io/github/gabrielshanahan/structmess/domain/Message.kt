package io.github.gabrielshanahan.structmess.domain

import com.fasterxml.jackson.databind.JsonNode
import java.time.ZonedDateTime
import java.util.UUID

/** Represents a message in the message queue system. */
data class Message(
    val id: UUID,
    val topic: String,
    val payload: JsonNode,
    val createdAt: ZonedDateTime,
)
