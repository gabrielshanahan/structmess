package io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.blocking.JsonbHelper
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.isNotEmpty
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import jakarta.enterprise.context.ApplicationScoped
import java.sql.Connection
import java.util.UUID
import org.codejargon.fluentjdbc.api.FluentJdbc

@ApplicationScoped
class MessageEventRepository(
    private val jsonbHelper: JsonbHelper,
    private val fluentJdbc: FluentJdbc,
) {

    fun insertGlobalEmittedEvent(
        connection: Connection,
        messageId: UUID,
        cooperationLineage: List<UUID>,
        context: CooperationContext?,
    ) {
        fluentJdbc
            .queryOn(connection)
            .update(
                "--${messageId} || ${cooperationLineage}\n" +
                    "INSERT INTO message_event (message_id, type, cooperation_lineage, context) VALUES (:messageId, 'EMITTED', :cooperationLineage, :context)"
            )
            .namedParam("messageId", messageId)
            .namedParam(
                "cooperationLineage",
                connection.createArrayOf("uuid", cooperationLineage.toTypedArray()),
            )
            .namedParam(
                "context",
                context?.takeIf { it.isNotEmpty() }?.let { jsonbHelper.toPGobject(it) },
            )
            .run()
    }

    fun insertScopedEmittedEvent(
        connection: Connection,
        messageId: UUID,
        coroutineName: String,
        coroutineIdentifier: String,
        stepName: String?,
        cooperationLineage: List<UUID>,
        context: CooperationContext?,
    ) {
        fluentJdbc
            .queryOn(connection)
            .update(
                """
                INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, context) 
                VALUES (:messageId, 'EMITTED', :coroutineName, :coroutineIdentifier, :stepName, :cooperationLineage, :context)
            """
                    .trimIndent()
            )
            .namedParam("messageId", messageId)
            .namedParam("coroutineName", coroutineName)
            .namedParam("coroutineIdentifier", coroutineIdentifier)
            .namedParam("stepName", stepName)
            .namedParam(
                "cooperationLineage",
                connection.createArrayOf("uuid", cooperationLineage.toTypedArray()),
            )
            .namedParam(
                "context",
                context?.takeIf { it.isNotEmpty() }?.let { jsonbHelper.toPGobject(it) },
            )
            .run()
    }

    fun insertRollbackEmittedEvent(
        connection: Connection,
        cooperationLineage: List<UUID>,
        exception: CooperationFailure,
    ) {
        val lineageArray = connection.createArrayOf("uuid", cooperationLineage.toTypedArray())

        fluentJdbc
            .queryOn(connection)
            .update(
                """
                WITH message_id_lookup AS (
                    SELECT DISTINCT message_id 
                    FROM message_event 
                    WHERE cooperation_lineage = :cooperationLineage
                    LIMIT 1
                )
                INSERT INTO message_event (
                    message_id, 
                    type,  
                    cooperation_lineage, 
                    exception
                )
                SELECT 
                    message_id_lookup.message_id, 'ROLLBACK_EMITTED', :cooperationLineage, :exception
                FROM message_id_lookup
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM message_event seen
                    LEFT JOIN message_event committed
                      ON committed.cooperation_lineage = seen.cooperation_lineage
                         AND committed.type = 'COMMITTED'
                    WHERE seen.type = 'SEEN'
                      AND seen.cooperation_lineage <@ :cooperationLineage
                      AND committed.cooperation_lineage IS NULL
                      AND cardinality(seen.cooperation_lineage) > 1
                )
                ON CONFLICT (message_id, type) WHERE type = 'ROLLBACK_EMITTED' DO NOTHING
            """
                    .trimIndent()
            )
            .namedParam("cooperationLineage", lineageArray)
            .namedParam("exception", jsonbHelper.toPGobject(exception))
            .run()
    }

    fun insertCancellationRequestedEvent(
        connection: Connection,
        coroutineName: String?,
        coroutineIdentifier: String?,
        stepName: String?,
        cooperationLineage: List<UUID>,
        exception: CooperationFailure,
    ) {
        val lineageArray = connection.createArrayOf("uuid", cooperationLineage.toTypedArray())

        fluentJdbc
            .queryOn(connection)
            .update(
                """
                WITH message_id_lookup AS (
                    SELECT DISTINCT message_id 
                    FROM message_event 
                    WHERE cooperation_lineage = :cooperationLineage
                    LIMIT 1
                )
                INSERT INTO message_event (
                    message_id, 
                    type, 
                    coroutine_name, 
                    coroutine_identifier, 
                    step, 
                    cooperation_lineage, 
                    exception
                ) 
                SELECT 
                    message_id_lookup.message_id,
                    'CANCELLATION_REQUESTED',
                    :coroutineName, :coroutineIdentifier, :stepName, :cooperationLineage, :exception
                FROM message_id_lookup
                ON CONFLICT (cooperation_lineage, type) WHERE type = 'CANCELLATION_REQUESTED' DO NOTHING
            """
                    .trimIndent()
            )
            .namedParam("cooperationLineage", lineageArray)
            .namedParam("coroutineName", coroutineName)
            .namedParam("coroutineIdentifier", coroutineIdentifier)
            .namedParam("stepName", stepName)
            .namedParam("exception", jsonbHelper.toPGobject(exception))
            .run()
    }

    fun insertMessageEvent(
        connection: Connection,
        messageId: UUID,
        messageEventType: String,
        coroutineName: String,
        coroutineIdentifier: String,
        stepName: String?,
        cooperationLineage: List<UUID>,
        exception: CooperationFailure?,
        context: CooperationContext?,
    ) {
        fluentJdbc
            .queryOn(connection)
            .update(
                """
                INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, exception, context) 
                VALUES (:messageId, :messageEventType::message_event_type, :coroutineName, :coroutineIdentifier, :stepName, :cooperationLineage, :exception, :context)
            """
                    .trimIndent()
            )
            .namedParam("messageId", messageId)
            .namedParam("messageEventType", messageEventType)
            .namedParam("coroutineName", coroutineName)
            .namedParam("coroutineIdentifier", coroutineIdentifier)
            .namedParam("stepName", stepName)
            .namedParam(
                "cooperationLineage",
                connection.createArrayOf("uuid", cooperationLineage.toTypedArray()),
            )
            .namedParam("exception", exception?.let { jsonbHelper.toPGobject(it) })
            .namedParam(
                "context",
                context?.takeIf { it.isNotEmpty() }?.let { jsonbHelper.toPGobject(it) },
            )
            .run()
    }

    fun insertRollbackEmittedEventsForStep(
        connection: Connection,
        stepName: String,
        cooperationLineage: List<UUID>,
        coroutineName: String,
        coroutineIdentifier: String,
        scopeStepName: String?,
        exception: CooperationFailure,
        context: CooperationContext?,
    ) {
        val lineageArray = connection.createArrayOf("uuid", cooperationLineage.toTypedArray())

        fluentJdbc
            .queryOn(connection)
            .update(
                """
                WITH emitted_record AS (
                    SELECT cooperation_lineage, message_id
                    FROM message_event
                    WHERE type = 'EMITTED'
                        AND step = :stepName
                        AND cooperation_lineage = :cooperationLineage
                )
                INSERT INTO message_event (
                    message_id, type, 
                    cooperation_lineage, coroutine_name, coroutine_identifier, step,
                    exception, context
                )
                SELECT 
                    message_id, 'ROLLBACK_EMITTED', 
                    :cooperationLineage, :coroutineName, :coroutineIdentifier, :scopeStepName, :exception, :context
                FROM emitted_record
            """
                    .trimIndent()
            )
            .namedParam("stepName", stepName)
            .namedParam("cooperationLineage", lineageArray)
            .namedParam("coroutineName", coroutineName)
            .namedParam("coroutineIdentifier", coroutineIdentifier)
            .namedParam("scopeStepName", scopeStepName)
            .namedParam("exception", jsonbHelper.toPGobject(exception))
            .namedParam(
                "context",
                context?.takeIf { it.isNotEmpty() }?.let { jsonbHelper.toPGobject(it) },
            )
            .run()
    }

    fun fetchGiveUpExceptions(
        connection: Connection,
        giveUpSqlProvider: (String) -> String,
        cooperationLineage: List<UUID>,
    ): List<CooperationException> =
        fluentJdbc
            .queryOn(connection)
            .select(
                """
                WITH seen AS (
                    SELECT * FROM message_event WHERE cooperation_lineage = :cooperationLineage AND type = 'SEEN'
                )
                ${giveUpSqlProvider("seen")}
            """
                    .trimIndent()
            )
            .namedParam(
                "cooperationLineage",
                connection.createArrayOf("uuid", cooperationLineage.toTypedArray()),
            )
            .listResult { resultSet ->
                val cooperationFailure =
                    jsonbHelper.fromPGobject<CooperationFailure>(resultSet.getObject("exception"))
                cooperationFailure?.let { CooperationFailure.Companion.toCooperationException(it) }
            }
            .filterNotNull()
}
