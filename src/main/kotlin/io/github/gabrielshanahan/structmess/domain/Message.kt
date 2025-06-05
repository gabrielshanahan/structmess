package io.github.gabrielshanahan.structmess.domain

import io.vertx.core.json.JsonObject
import java.time.ZonedDateTime
import java.util.UUID

data class Message(
    val id: UUID,
    val topic: String,
    val payload: JsonObject,
    val createdAt: ZonedDateTime,
)
