package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.domain.ContinuationIdentifier
import io.github.gabrielshanahan.structmess.domain.CooperationFailure
import io.github.gabrielshanahan.structmess.domain.CoroutineIdentifier
import io.github.gabrielshanahan.structmess.domain.Message
import io.vertx.mutiny.sqlclient.Pool
import io.vertx.mutiny.sqlclient.Tuple
import java.time.ZoneId
import java.util.UUID

class SqlTestUtils(private val pool: Pool, private val objectMapper: ObjectMapper) {

    fun createMessage(topic: String, payload: JsonNode): Message {
        val result =
            pool.executeAndAwaitPreparedQuery(
                "INSERT INTO message (topic, payload) VALUES ($1, $2::jsonb) RETURNING id, created_at",
                Tuple.of(topic, payload.toString()),
            )

        return result.first().run {
            Message(
                getUUID("id"),
                topic,
                payload,
                getLocalDateTime("created_at").atZone(ZoneId.systemDefault()),
            )
        }
    }

    fun createSimpleMessage(topic: String, key: String = "key", value: String = "value"): Message {
        val payload = objectMapper.createObjectNode().put(key, value)
        return createMessage(topic, payload)
    }

    fun emitted(
        messageId: UUID,
        continuationIdentifier: ContinuationIdentifier? = null,
        cooperationLineage: List<UUID> = emptyList(),
    ): UUID {
        val sql =
            if (continuationIdentifier != null) {
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage) VALUES ($1, 'EMITTED', $2, $3, $4, $5) RETURNING id"
            } else {
                "INSERT INTO message_event (message_id, type) VALUES ($1, 'EMITTED') RETURNING id"
            }

        val params =
            if (continuationIdentifier != null) {
                Tuple.of(
                    messageId,
                    continuationIdentifier.coroutineIdentifier.name,
                    continuationIdentifier.coroutineIdentifier.instance,
                    continuationIdentifier.stepName,
                    cooperationLineage.toTypedArray(),
                )
            } else {
                Tuple.of(messageId)
            }

        val result = pool.executeAndAwaitPreparedQuery(sql, params)

        return result.first().getUUID("id")
    }

    fun coroutineEvent(
        messageId: UUID,
        coroutineIdentifier: CoroutineIdentifier,
        cooperationLineage: List<UUID>,
        eventType: String,
        throwable: Throwable?,
    ): UUID {
        val exceptionJson =
            throwable?.let {
                val cooperationFailure =
                    CooperationFailure.fromThrowable(
                        it,
                        coroutineIdentifier.run { "$name[$instance]" },
                    )
                objectMapper.writeValueAsString(cooperationFailure)
            }
        val result =
            pool.executeAndAwaitPreparedQuery(
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, cooperation_lineage, exception) VALUES ($1, '$eventType', $2, $3, $4, $5::jsonb) RETURNING id",
                Tuple.of(
                    messageId,
                    coroutineIdentifier.name,
                    coroutineIdentifier.instance,
                    cooperationLineage.toTypedArray(),
                    exceptionJson,
                ),
            )

        return result.first().getUUID("id")
    }

    fun seen(
        messageId: UUID,
        coroutineIdentifier: CoroutineIdentifier,
        cooperationLineage: List<UUID>,
    ): UUID = coroutineEvent(messageId, coroutineIdentifier, cooperationLineage, "SEEN", null)

    fun continuationEvent(
        messageId: UUID,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: List<UUID>,
        eventType: String,
        throwable: Throwable?,
    ): UUID {
        val exceptionJson =
            throwable?.let {
                val cooperationFailure =
                    CooperationFailure.fromThrowable(
                        it,
                        continuationIdentifier.coroutineIdentifier.run { "$name[$instance]" },
                    )
                objectMapper.writeValueAsString(cooperationFailure)
            }
        val result =
            pool.executeAndAwaitPreparedQuery(
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, exception) VALUES ($1, '$eventType', $2, $3, $4, $5, $6::jsonb) RETURNING id",
                Tuple.of(
                    messageId,
                    continuationIdentifier.coroutineIdentifier.name,
                    continuationIdentifier.coroutineIdentifier.instance,
                    continuationIdentifier.stepName,
                    cooperationLineage.toTypedArray(),
                    exceptionJson,
                ),
            )

        return result.first().getUUID("id")
    }

    fun suspended(
        messageId: UUID,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: List<UUID>,
    ): UUID =
        continuationEvent(messageId, continuationIdentifier, cooperationLineage, "SUSPENDED", null)

    fun committed(
        messageId: UUID,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: List<UUID>,
    ): UUID =
        continuationEvent(messageId, continuationIdentifier, cooperationLineage, "COMMITTED", null)

    fun rollingBack(
        messageId: UUID,
        coroutineIdentifier: CoroutineIdentifier,
        cooperationLineage: List<UUID>,
        throwable: Throwable,
    ): UUID =
        coroutineEvent(
            messageId,
            coroutineIdentifier,
            cooperationLineage,
            "ROLLING_BACK",
            throwable,
        )

    fun rollingBack(
        messageId: UUID,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: List<UUID>,
        throwable: Throwable,
    ): UUID =
        continuationEvent(
            messageId,
            continuationIdentifier,
            cooperationLineage,
            "ROLLING_BACK",
            throwable,
        )

    fun rollbackEmitted(
        messageId: UUID,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: List<UUID>,
        throwable: Throwable,
    ): UUID =
        continuationEvent(
            messageId,
            continuationIdentifier,
            cooperationLineage,
            "ROLLBACK_EMITTED",
            throwable,
        )

    fun rolledBack(
        messageId: UUID,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: List<UUID>,
    ): UUID =
        continuationEvent(
            messageId,
            continuationIdentifier,
            cooperationLineage,
            "ROLLED_BACK",
            null,
        )

    fun rollbackFailed(
        messageId: UUID,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: List<UUID>,
        throwable: Throwable,
    ): UUID =
        continuationEvent(
            messageId,
            continuationIdentifier,
            cooperationLineage,
            "ROLLBACK_FAILED",
            throwable,
        )
}
