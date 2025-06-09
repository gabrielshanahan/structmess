package io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.blocking.JsonbHelper
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.continuation.ContinuationIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import java.time.ZoneOffset
import java.util.UUID
import org.codejargon.fluentjdbc.api.FluentJdbc
import org.postgresql.util.PGobject

class SqlTestUtils(private val fluentJdbc: FluentJdbc, private val jsonbHelper: JsonbHelper) {

    fun createMessage(topic: String, payload: PGobject): Message =
        fluentJdbc
            .query()
            .select(
                "INSERT INTO message (topic, payload) VALUES (:topic, :payload) RETURNING id, created_at"
            )
            .namedParam("topic", topic)
            .namedParam("payload", payload)
            .singleResult {
                Message(
                    it.getObject("id", UUID::class.java),
                    topic,
                    payload,
                    it.getTimestamp("created_at").toLocalDateTime().atOffset(ZoneOffset.UTC),
                )
            }

    fun createSimpleMessage(topic: String, key: String = "key", value: String = "value"): Message {
        val payload = jsonbHelper.toPGobject(mapOf(key to value))
        return createMessage(topic, payload)
    }

    fun emitted(
        messageId: UUID,
        continuationIdentifier: ContinuationIdentifier? = null,
        cooperationLineage: List<UUID> = emptyList(),
    ): UUID {
        val sql =
            if (continuationIdentifier != null) {
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage) VALUES (:message_id, 'EMITTED', :coroutine_name, :coroutine_identifier, :step, :cooperation_lineage) RETURNING id"
            } else {
                "INSERT INTO message_event (message_id, type) VALUES (:message_id, 'EMITTED') RETURNING id"
            }

        val params =
            if (continuationIdentifier != null) {
                mapOf(
                    "message_id" to messageId,
                    "coroutine_name" to continuationIdentifier.distributedCoroutineIdentifier.name,
                    "coroutine_identifier" to
                        continuationIdentifier.distributedCoroutineIdentifier.instance,
                    "step" to continuationIdentifier.stepName,
                    "cooperation_lineage" to cooperationLineage.toTypedArray(),
                )
            } else {
                mapOf("message_id" to messageId)
            }

        return fluentJdbc.query().select(sql).namedParams(params).singleResult {
            it.getObject("id", UUID::class.java)
        }
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

        return fluentJdbc
            .query()
            .select(
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, cooperation_lineage, exception) " +
                    "VALUES (:message_id, :event_type::message_event_type, :coroutine_name, :coroutine_identifier, :cooperation_lineage, :exception) " +
                    "RETURNING id"
            )
            .namedParam("message_id", messageId)
            .namedParam("event_type", eventType)
            .namedParam("coroutine_name", distributedCoroutineIdentifier.name)
            .namedParam("coroutine_identifier", distributedCoroutineIdentifier.instance)
            .namedParam("cooperation_lineage", cooperationLineage.toTypedArray())
            .namedParam("exception", exception?.let(jsonbHelper::toPGobject))
            .singleResult { it.getObject("id", UUID::class.java) }
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

        return fluentJdbc
            .query()
            .select(
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, exception) " +
                    "VALUES (:message_id, :event_type::message_event_type, :coroutine_name, :coroutine_identifier, :step, :cooperation_lineage, :exception) " +
                    "RETURNING id"
            )
            .namedParam("message_id", messageId)
            .namedParam("event_type", eventType)
            .namedParam(
                "coroutine_name",
                continuationIdentifier.distributedCoroutineIdentifier.name,
            )
            .namedParam(
                "coroutine_identifier",
                continuationIdentifier.distributedCoroutineIdentifier.instance,
            )
            .namedParam("step", continuationIdentifier.stepName)
            .namedParam("cooperation_lineage", cooperationLineage.toTypedArray())
            .namedParam("exception", exception?.let(jsonbHelper::toPGobject))
            .singleResult { it.getObject("id", UUID::class.java) }
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
