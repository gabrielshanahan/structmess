package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.coroutine.ChildStrategy
import io.github.gabrielshanahan.structmess.coroutine.CooperationContext
import io.github.gabrielshanahan.structmess.coroutine.writeCooperationContext
import io.github.gabrielshanahan.structmess.flatMapNonNull
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonArray
import io.vertx.mutiny.sqlclient.SqlClient
import io.vertx.mutiny.sqlclient.SqlConnection
import io.vertx.mutiny.sqlclient.Tuple
import jakarta.enterprise.context.ApplicationScoped
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.UUID

@ApplicationScoped
class MessageEventRepository(private val objectMapper: ObjectMapper) {

    fun insertGlobalEmittedEvent(
        connection: SqlConnection,
        messageId: UUID,
        cooperationLineage: List<UUID>,
        context: CooperationContext?,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                "INSERT INTO message_event (message_id, type, cooperation_lineage, context) VALUES ($1, 'EMITTED', $2, $3::jsonb)"
            )
            .execute(
                Tuple.of(
                    messageId,
                    cooperationLineage.toTypedArray(),
                    context?.let(objectMapper::writeCooperationContext)?.takeIf { it != "{}" },
                )
            )
            .replaceWith(Unit)

    fun insertScopedEmittedEvent(
        connection: SqlConnection,
        messageId: UUID,
        coroutineName: String,
        coroutineIdentifier: String,
        stepName: String?,
        cooperationLineage: List<UUID>,
        context: CooperationContext?,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, context) VALUES ($1, 'EMITTED', $2, $3, $4, $5, $6::jsonb)"
            )
            .execute(
                Tuple.of(
                    messageId,
                    coroutineName,
                    coroutineIdentifier,
                    stepName,
                    cooperationLineage.toTypedArray(),
                    context?.let(objectMapper::writeCooperationContext)?.takeIf { it != "{}" },
                )
            )
            .replaceWith(Unit)

    fun insertRollbackEmittedEvent(
        connection: SqlConnection,
        cooperationLineage: List<UUID>,
        exceptionJson: String,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                """
                    WITH message_id_lookup AS (
                        SELECT DISTINCT message_id 
                        FROM message_event 
                        WHERE cooperation_lineage = $1
                        LIMIT 1
                    )
                    INSERT INTO message_event (
                        message_id, 
                        type,  
                        cooperation_lineage, 
                        exception
                    )
                    SELECT 
                        message_id_lookup.message_id, 'ROLLBACK_EMITTED', $1, $2::jsonb
                    FROM message_id_lookup
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM message_event seen
                        LEFT JOIN message_event committed
                          ON committed.cooperation_lineage = seen.cooperation_lineage
                             AND committed.type = 'COMMITTED'
                        WHERE seen.type = 'SEEN'
                          AND seen.cooperation_lineage <@ $1
                          AND committed.cooperation_lineage IS NULL
                          AND cardinality(seen.cooperation_lineage) > 1
                    )
                    ON CONFLICT (message_id, type) WHERE type = 'ROLLBACK_EMITTED' DO NOTHING;
                """
                    .trimIndent()
            )
            .execute(Tuple.tuple(listOf(cooperationLineage.toTypedArray<UUID>(), exceptionJson)))
            .replaceWith(Unit)

    fun insertCancellationRequestedEvent(
        connection: SqlConnection,
        coroutineName: String?,
        coroutineIdentifier: String?,
        stepName: String?,
        cooperationLineage: List<UUID>,
        exceptionJson: String,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                """
                    WITH message_id_lookup AS (
                        SELECT DISTINCT message_id 
                        FROM message_event 
                        WHERE cooperation_lineage = $1
                        LIMIT 1
                    )
                    INSERT INTO message_event (
                        message_id, 
                        type, 
                        coroutine_name, 
                        coroutine_identifier, 
                        step, 
                        cooperation_lineage, 
                        exception) 
                    SELECT 
                        message_id_lookup.message_id,
                        'CANCELLATION_REQUESTED',
                        $2, $3, $4, $1, $5::jsonb
                    FROM message_id_lookup
                    ON CONFLICT (cooperation_lineage, type) WHERE type = 'CANCELLATION_REQUESTED' DO NOTHING
                """
                    .trimIndent()
            )
            .execute(
                Tuple.tuple(
                    listOf(
                        cooperationLineage.toTypedArray(),
                        coroutineName,
                        coroutineIdentifier,
                        stepName,
                        exceptionJson,
                    )
                )
            )
            .replaceWith(Unit)

    fun insertMessageEvent(
        client: SqlClient,
        messageId: UUID,
        messageEventType: String,
        coroutineName: String,
        coroutineIdentifier: String,
        stepName: String?,
        cooperationLineage: List<UUID>,
        exceptionJson: String?,
        context: CooperationContext?,
    ): Uni<Unit> =
        client
            .preparedQuery(
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, exception, context) VALUES ($1, '$messageEventType', $2, $3, $4, $5, $6::jsonb, $7::jsonb)"
            )
            .execute(
                Tuple.tuple(
                    listOf(
                        messageId,
                        coroutineName,
                        coroutineIdentifier,
                        stepName,
                        cooperationLineage.toTypedArray(),
                        exceptionJson,
                        context?.let(objectMapper::writeCooperationContext)?.takeIf { it != "{}" },
                    )
                )
            )
            .replaceWith(Unit)

    fun insertRollbackEmittedEventsForStep(
        connection: SqlConnection,
        stepName: String,
        cooperationLineage: List<UUID>,
        coroutineName: String,
        coroutineIdentifier: String,
        scopeStepName: String?,
        exceptionJson: String,
        context: CooperationContext?,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                """
                    WITH emitted_record AS (
                        SELECT cooperation_lineage, message_id
                        FROM message_event
                        WHERE type = 'EMITTED'
                            AND step = $1
                            AND cooperation_lineage = $2
                    )
                    INSERT INTO message_event (
                        message_id, type, 
                        cooperation_lineage, coroutine_name, coroutine_identifier, step,
                        exception, context
                    )
                    SELECT 
                        message_id, 'ROLLBACK_EMITTED', 
                        $2, $3, $4, $5, $6::jsonb, $7::jsonb
                    FROM emitted_record;
                """
            )
            .execute(
                Tuple.tuple(
                    listOf(
                        stepName,
                        cooperationLineage.toTypedArray(),
                        coroutineName,
                        coroutineIdentifier,
                        scopeStepName,
                        exceptionJson,
                        context?.let(objectMapper::writeCooperationContext)?.takeIf { it != "{}" },
                    )
                )
            )
            .replaceWith(Unit)

    data class CancellationExceptionsResult(val exceptionJsons: List<String>)

    /** Fetches cancellation exceptions for a cooperation scope, or any of its parents. */
    fun fetchCancellationExceptions(
        connection: SqlConnection,
        cooperationLineage: List<UUID>,
    ): Uni<CancellationExceptionsResult> =
        connection
            .preparedQuery(
                "SELECT exception FROM message_event WHERE cooperation_lineage <@ $1 AND type = 'CANCELLATION_REQUESTED'"
            )
            .execute(Tuple.of(cooperationLineage.toTypedArray()))
            .map { rowSet ->
                CancellationExceptionsResult(rowSet.map { it.getString("exception") })
            }

    fun markSeenOrRollingBack(
        connection: SqlConnection,
        coroutineName: String,
        coroutineIdentifier: String,
        topic: String,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                """
                WITH
                -- Find EMITTED records missing a SEEN
                emitted_missing_seen AS (
                    SELECT message_event.message_id, message_event.cooperation_lineage, message_event.context 
                    FROM message_event
                    LEFT JOIN message_event AS seen_check
                        ON seen_check.message_id = message_event.message_id AND seen_check.type = 'SEEN' AND seen_check.coroutine_name = $1
                    JOIN message
                        ON message.id = message_event.message_id AND message.topic = $3
                    WHERE message_event.type = 'EMITTED' AND seen_check.id IS NULL
                ),
                -- Find ROLLBACK_EMITTED records missing a ROLLING_BACK
                rollback_emitted_missing_rolling_back AS (
                    SELECT message_event.message_id, seen.cooperation_lineage, message_event.exception, message_event.context
                    FROM message_event
                    LEFT JOIN message_event AS rolling_back_check
                        ON rolling_back_check.message_id = message_event.message_id AND rolling_back_check.type = 'ROLLING_BACK' AND rolling_back_check.coroutine_name = $1
                    JOIN message
                        ON message.id = message_event.message_id AND message.topic = $3
                    JOIN message_event AS seen
                         ON seen.message_id = message_event.message_id AND seen.type = 'SEEN' AND seen.coroutine_name = $1
                    WHERE message_event.type = 'ROLLBACK_EMITTED' AND rolling_back_check.id IS NULL
                ),
                -- Insert SEEN if EMITTED exists without SEEN
                seen_insert AS (
                    INSERT INTO message_event (
                        message_id, type, 
                        coroutine_name, coroutine_identifier, 
                        cooperation_lineage,
                        context
                    )
                    SELECT 
                        emitted_missing_seen.message_id, 
                        'SEEN', 
                        $1,
                        $2,
                        emitted_missing_seen.cooperation_lineage || gen_uuid_v7(),
                        emitted_missing_seen.context
                    FROM emitted_missing_seen
                    ON CONFLICT (coroutine_name, message_id, type) WHERE type = 'SEEN' DO NOTHING
                    RETURNING id
                ),
                -- Insert ROLLING_BACK if ROLLBACK_EMITTED exists without ROLLING_BACK
                rolling_back_insert AS (
                    INSERT INTO message_event (
                        message_id, type, 
                        coroutine_name, coroutine_identifier, 
                        cooperation_lineage, 
                        exception,
                        context
                    )
                    SELECT 
                        rollback_emitted_missing_rolling_back.message_id, 
                        'ROLLING_BACK', 
                        $1,
                        $2,
                        rollback_emitted_missing_rolling_back.cooperation_lineage,
                        rollback_emitted_missing_rolling_back.exception,
                        rollback_emitted_missing_rolling_back.context
                    FROM rollback_emitted_missing_rolling_back
                    ON CONFLICT (coroutine_name, message_id, type) WHERE type = 'ROLLING_BACK' DO NOTHING
                    RETURNING id
                )
                -- Just here to execute the CTEs
                SELECT 1;
            """
            )
            .execute(Tuple.of(coroutineName, coroutineIdentifier, topic))
            .replaceWith(Unit)

    data class PendingCoroutineRun(
        val messageId: UUID,
        val topic: String,
        val cooperationLineage: List<UUID>,
        val payload: String,
        val createdAt: ZonedDateTime,
        val step: String?,
        val latestScopeContext: String?,
        val latestContext: String?,
        val childRolledBackExceptions: JsonArray,
        val childRollbackFailedExceptions: JsonArray,
        val rollingBackException: String?,
    )

    fun fetchPendingCoroutineRun(
        connection: SqlConnection,
        coroutineName: String,
        childStrategy: ChildStrategy,
    ): Uni<PendingCoroutineRun?> =
        connection
            .preparedQuery(finalSelect(childStrategy).build())
            .execute(Tuple.of(coroutineName))
            .flatMap { rowSet ->
                val row = rowSet.firstOrNull()
                if (row == null) {
                    // Nothing to do -> we're done
                    Uni.createFrom().nullItem()
                } else {
                    // This is, in essence, the equivalent of double-checked locking
                    // (https://en.wikipedia.org/wiki/Double-checked_locking).
                    //
                    // The query in finalSelect() essentially works in two steps:
                    //    1. Evaluate which SEEN records are waiting to be processed
                    //    2. Pick one and lock it, skipping locked ones.
                    //
                    // An important refresher: FOR UPDATE SKIP LOCKED uses the state of locks
                    // at the time it's executed, not at the beginning of the transaction.
                    //
                    // A race condition can arise in the following way:
                    //
                    // During the evaluation of step 1, some other handler can already be
                    // processing a SEEN, but not have committed yet, so we still get "ready to be
                    // processed" for that SEEN. But before we get to step 2, where the lock would
                    // prevent us from picking it up, the handler commits and releases the lock.
                    // This causes us to pick up the SEEN, but still retain and use the old data
                    // we fetched earlier, resulting in us re-running the same step again.
                    // Therefore,
                    // we mitigate this by rerunning the selection process once we have acquired the
                    // lock.
                    //
                    // Postgres attempts to mitigate this to a certain extent by checking that the
                    // row being locked has not been modified during the time the query is run and
                    // refetches the record if that happens. Some solutions to this problem revolve
                    // around this, recommending you always modify the row being locked. However,
                    // Postgres only refetches that specific row and doesn't evaluate the rest of
                    // the conditions. This makes it unusable for our purposes, since, e.g., the
                    // latest SUSPEND record is one of the key things we use and need to refetch.
                    //
                    // There are various other approaches that could be taken, sometimes referred
                    // to as "materializing the conflict", but the simplest solution for a POC
                    // application such as this is to just run the query again after we've
                    // acquired the lock, which is exactly what double-checked locking is.
                    //
                    // For a different example of this behavior, take a look at, e.g.,
                    // https://postgrespro.com/list/thread-id/2470837
                    connection
                        .preparedQuery(finalSelect(childStrategy, true).build())
                        .execute(Tuple.of(coroutineName, row.getUUID("id")))
                }
            }
            .flatMapNonNull { rowSet ->
                val row = rowSet.firstOrNull()
                if (row == null) {
                    // After the second fetch, we found out that the record is no longer ready for
                    // processing (i.e., the race condition described above happened)
                    Uni.createFrom().nullItem()
                } else {
                    Uni.createFrom()
                        .item(
                            PendingCoroutineRun(
                                messageId = row.getUUID("id"),
                                topic = row.getString("topic"),
                                payload = row.getString("payload"),
                                createdAt =
                                    row.getLocalDateTime("created_at")
                                        .atZone(ZoneId.systemDefault()),
                                cooperationLineage =
                                    row.getArrayOfUUIDs("cooperation_lineage").toList(),
                                step = row.getString("step"),
                                latestScopeContext = row.getString("latest_scope_context"),
                                latestContext = row.getString("latest_context"),
                                childRolledBackExceptions =
                                    row.getJsonArray("child_rolled_back_exceptions"),
                                childRollbackFailedExceptions =
                                    row.getJsonArray("child_rollback_failed_exceptions"),
                                rollingBackException = row.getString("rolling_back_exception"),
                            )
                        )
                }
            }
}
