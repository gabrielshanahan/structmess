package io.github.gabrielshanahan.scoop.shared.coroutine.context

import java.time.OffsetDateTime
import java.time.ZoneOffset

val postgresMaxTime: OffsetDateTime =
    OffsetDateTime.of(9999, 12, 31, 23, 59, 59, 999999000, ZoneOffset.UTC)
