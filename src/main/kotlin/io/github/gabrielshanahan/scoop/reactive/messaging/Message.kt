package io.github.gabrielshanahan.scoop.reactive.messaging

import io.vertx.core.json.JsonObject
import java.time.OffsetDateTime
import java.util.UUID

data class Message(
    val id: UUID,
    val topic: String,
    val payload: JsonObject,
    val createdAt: OffsetDateTime,
)
