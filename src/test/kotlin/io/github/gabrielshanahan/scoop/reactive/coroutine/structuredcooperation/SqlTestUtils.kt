package io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.reactive.coroutine.executeAndAwaitPreparedQuery
import io.github.gabrielshanahan.scoop.reactive.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.continuation.ContinuationIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.Pool
import io.vertx.mutiny.sqlclient.Tuple
import java.time.ZoneOffset
import java.util.UUID

class SqlTestUtils(private val pool: Pool) {

    fun createMessage(topic: String, payload: JsonObject): Message {
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
                getLocalDateTime("created_at").atOffset(ZoneOffset.UTC),
            )
        }
    }

    fun createSimpleMessage(topic: String, key: String = "key", value: String = "value"): Message {
        val payload = JsonObject().put(key, value)
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
                    continuationIdentifier.distributedCoroutineIdentifier.name,
                    continuationIdentifier.distributedCoroutineIdentifier.instance,
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
        distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
        cooperationLineage: List<UUID>,
        eventType: String,
        throwable: Throwable?,
    ): UUID {
        val exception =
            throwable?.let {
                CooperationFailure.fromThrowable(
                    it,
                    distributedCoroutineIdentifier.run { "$name[$instance]" },
                )
            }
        val result =
            pool.executeAndAwaitPreparedQuery(
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, cooperation_lineage, exception) " +
                    "VALUES ($1, '$eventType', $2, $3, $4, $5) " +
                    "RETURNING id",
                Tuple.of(
                    messageId,
                    distributedCoroutineIdentifier.name,
                    distributedCoroutineIdentifier.instance,
                    cooperationLineage.toTypedArray(),
                    JsonObject.mapFrom(exception),
                ),
            )

        return result.first().getUUID("id")
    }

    fun seen(
        messageId: UUID,
        distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
        cooperationLineage: List<UUID>,
    ): UUID =
        coroutineEvent(messageId, distributedCoroutineIdentifier, cooperationLineage, "SEEN", null)

    fun continuationEvent(
        messageId: UUID,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: List<UUID>,
        eventType: String,
        throwable: Throwable?,
    ): UUID {
        val exception =
            throwable?.let {
                CooperationFailure.fromThrowable(
                    it,
                    continuationIdentifier.distributedCoroutineIdentifier.run { "$name[$instance]" },
                )
            }
        val result =
            pool.executeAndAwaitPreparedQuery(
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, exception) " +
                    "VALUES ($1, '$eventType', $2, $3, $4, $5, $6) " +
                    "RETURNING id",
                Tuple.of(
                    messageId,
                    continuationIdentifier.distributedCoroutineIdentifier.name,
                    continuationIdentifier.distributedCoroutineIdentifier.instance,
                    continuationIdentifier.stepName,
                    cooperationLineage.toTypedArray(),
                    JsonObject.mapFrom(exception),
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
        distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
        cooperationLineage: List<UUID>,
        throwable: Throwable,
    ): UUID =
        coroutineEvent(
            messageId,
            distributedCoroutineIdentifier,
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
