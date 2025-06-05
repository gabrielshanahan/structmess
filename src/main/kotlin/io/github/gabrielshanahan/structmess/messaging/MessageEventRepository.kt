package io.github.gabrielshanahan.structmess.messaging

import io.github.gabrielshanahan.structmess.coroutine.CooperationContext
import io.github.gabrielshanahan.structmess.coroutine.EventLoopStrategy
import io.github.gabrielshanahan.structmess.coroutine.isNotEmpty
import io.github.gabrielshanahan.structmess.domain.CooperationException
import io.github.gabrielshanahan.structmess.domain.CooperationFailure
import io.github.gabrielshanahan.structmess.flatMapNonNull
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.SqlClient
import io.vertx.mutiny.sqlclient.SqlConnection
import io.vertx.mutiny.sqlclient.Tuple
import jakarta.enterprise.context.ApplicationScoped
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.UUID
import org.intellij.lang.annotations.Language

@ApplicationScoped
class MessageEventRepository {

    fun insertGlobalEmittedEvent(
        connection: SqlConnection,
        messageId: UUID,
        cooperationLineage: List<UUID>,
        context: CooperationContext?,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                "INSERT INTO message_event (message_id, type, cooperation_lineage, context) VALUES ($1, 'EMITTED', $2, $3)"
            )
            .execute(
                Tuple.of(
                    messageId,
                    cooperationLineage.toTypedArray(),
                    JsonObject.mapFrom(context?.takeIf { it.isNotEmpty() }),
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
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, context) VALUES ($1, 'EMITTED', $2, $3, $4, $5, $6)"
            )
            .execute(
                Tuple.of(
                    messageId,
                    coroutineName,
                    coroutineIdentifier,
                    stepName,
                    cooperationLineage.toTypedArray(),
                    JsonObject.mapFrom(context?.takeIf { it.isNotEmpty() }),
                )
            )
            .replaceWith(Unit)

    fun insertRollbackEmittedEvent(
        connection: SqlConnection,
        cooperationLineage: List<UUID>,
        exception: CooperationFailure,
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
                        message_id_lookup.message_id, 'ROLLBACK_EMITTED', $1, $2
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
            .execute(
                Tuple.tuple(
                    listOf(cooperationLineage.toTypedArray<UUID>(), JsonObject.mapFrom(exception))
                )
            )
            .replaceWith(Unit)

    fun insertCancellationRequestedEvent(
        connection: SqlConnection,
        coroutineName: String?,
        coroutineIdentifier: String?,
        stepName: String?,
        cooperationLineage: List<UUID>,
        exception: CooperationFailure,
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
                        $2, $3, $4, $1, $5
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
                        JsonObject.mapFrom(exception),
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
        exception: CooperationFailure?,
        context: CooperationContext?,
    ): Uni<Unit> =
        client
            .preparedQuery(
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, exception, context) VALUES ($1, '$messageEventType', $2, $3, $4, $5, $6, $7)"
            )
            .execute(
                Tuple.tuple(
                    listOf(
                        messageId,
                        coroutineName,
                        coroutineIdentifier,
                        stepName,
                        cooperationLineage.toTypedArray(),
                        JsonObject.mapFrom(exception),
                        JsonObject.mapFrom(context?.takeIf { it.isNotEmpty() }),
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
        exception: CooperationFailure,
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
                        $2, $3, $4, $5, $6, $7
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
                        JsonObject.mapFrom(exception),
                        JsonObject.mapFrom(context?.takeIf { it.isNotEmpty() }),
                    )
                )
            )
            .replaceWith(Unit)

    fun startContinuationsForCoroutine(
        connection: SqlConnection,
        coroutineName: String,
        coroutineIdentifier: String,
        topic: String,
        eventLoopStrategy: EventLoopStrategy,
    ): Uni<Unit> {
        // TODO: Doc that the lock gymnastics are necessary in combination with the place where we
        // evaluate the strategy in in PendingCoroutineRunSql - specifically, we need to guarantee
        // that the "All seens have been terminated" holds when
        // a parent coroutine is picked up due to a giving up condition becoming true. The
        // quintessential place where this is necessary is a network partition, where a certain
        // coroutine expected by the strategy is unvailable for the entire
        // time until a timeout happens (in which case we correctly don't want to block the revert),
        // but then, at the precise moment we pick up the parent to start the cancellation, it
        // appears, registers a SEEN, and starts executing.

        // TODO: Doc that we don't do the last_parent_event check for rollbacks to support partial
        // rollbacks of only some subtree, as in test `rolling back sub-hierarchy works`
        @Language("PostgreSQL")
        val sql =
            """
                WITH
                -- Find EMITTED records missing a SEEN
                emitted_missing_seen AS (
                    SELECT emitted.message_id, emitted.cooperation_lineage, emitted.context, emitted.step
                    FROM message_event emitted
                    LEFT JOIN message_event AS coroutine_seen
                        ON coroutine_seen.message_id = emitted.message_id AND coroutine_seen.type = 'SEEN' AND coroutine_seen.coroutine_name = $1
                    JOIN message
                        ON message.id = emitted.message_id AND message.topic = $3
                    LEFT JOIN LATERAL (
                        -- First, check if a parent SEEN record exists at all
                        SELECT id, cooperation_lineage
                        FROM message_event seen
                        WHERE seen.type = 'SEEN' AND seen.cooperation_lineage = emitted.cooperation_lineage
                    ) parent_seen_exists ON parent_seen_exists.cooperation_lineage = emitted.cooperation_lineage
                    LEFT JOIN LATERAL (
                        -- Then try to lock it if it exists
                        SELECT 1 as locked
                        FROM message_event seen_lock
                        WHERE seen_lock.id = parent_seen_exists.id
                        FOR UPDATE SKIP LOCKED
                    ) parent_seen_lock_attempt ON parent_seen_exists.id IS NOT NULL
                    LEFT JOIN LATERAL (
                        -- Get the last event in parent sequence along with its type and step
                        SELECT type, step
                        FROM message_event last_event
                        WHERE last_event.cooperation_lineage = parent_seen_exists.cooperation_lineage
                        ORDER BY last_event.created_at DESC
                        LIMIT 1
                    ) last_parent_event ON parent_seen_exists.id IS NOT NULL
                    WHERE emitted.type = 'EMITTED' 
                        AND coroutine_seen.id IS NULL
                        AND (
                            -- Either no SEEN record exists (i.e., this is a toplevel emission, and there's nothing to lock)
                            parent_seen_exists.id IS NULL
                            -- OR the SEEN record exists AND we successfully locked it AND the parent sequence's last event is SUSPENDED with matching step
                            OR (
                                parent_seen_exists.id IS NOT NULL 
                                AND parent_seen_lock_attempt.locked IS NOT NULL
                                AND last_parent_event.type = 'SUSPENDED'
                                AND last_parent_event.step = emitted.step
                            )
                        )
                        AND (${eventLoopStrategy.start("emitted")})
                ),
                -- Find ROLLBACK_EMITTED records missing a ROLLING_BACK
                rollback_emitted_missing_rolling_back AS (
                    SELECT rollback_emitted.message_id, coroutine_seen.cooperation_lineage, rollback_emitted.exception, rollback_emitted.context, rollback_emitted.step
                    FROM message_event rollback_emitted
                    LEFT JOIN message_event AS rolling_back_check
                        ON rolling_back_check.message_id = rollback_emitted.message_id AND rolling_back_check.type = 'ROLLING_BACK' AND rolling_back_check.coroutine_name = $1
                    JOIN message
                        ON message.id = rollback_emitted.message_id AND message.topic = $3
                    JOIN message_event AS coroutine_seen
                         ON coroutine_seen.message_id = rollback_emitted.message_id AND coroutine_seen.type = 'SEEN' AND coroutine_seen.coroutine_name = $1
                    LEFT JOIN LATERAL (
                        -- First, check if a parent SEEN record exists at all
                        SELECT id, cooperation_lineage
                        FROM message_event seen
                        WHERE seen.type = 'SEEN' AND seen.cooperation_lineage = rollback_emitted.cooperation_lineage
                    ) parent_seen_exists ON parent_seen_exists.cooperation_lineage = rollback_emitted.cooperation_lineage 
                    LEFT JOIN LATERAL (
                        -- Then try to lock it if it exists
                        SELECT 1 as locked
                        FROM message_event seen_lock
                        WHERE seen_lock.id = parent_seen_exists.id
                        FOR UPDATE SKIP LOCKED
                    ) parent_seen_lock_attempt ON parent_seen_exists.id IS NOT NULL
                    WHERE rollback_emitted.type = 'ROLLBACK_EMITTED' 
                        AND rolling_back_check.id IS NULL
                        AND (
                                -- Either no SEEN record exists (i.e., this is a toplevel emission, and there's nothing to lock)
                                parent_seen_exists.id IS NULL
                                -- OR the SEEN record exists AND we successfully locked it AND the parent sequence's last event is SUSPENDED with matching step
                                OR (
                                    parent_seen_exists.id IS NOT NULL 
                                    AND parent_seen_lock_attempt.locked IS NOT NULL
                                )
                            )
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
                        emitted_missing_seen.cooperation_lineage || gen_uuid_v7(), -- append additional cooperation id
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
        return connection
            .preparedQuery(sql)
            .execute(Tuple.of(coroutineName, coroutineIdentifier, topic))
            .replaceWith(Unit)
    }

    data class PendingCoroutineRun(
        val messageId: UUID,
        val topic: String,
        val cooperationLineage: List<UUID>,
        val payload: JsonObject,
        val createdAt: ZonedDateTime,
        val step: String?,
        val latestScopeContext: JsonObject?,
        val latestContext: JsonObject?,
        val childRolledBackExceptions: JsonArray,
        val childRollbackFailedExceptions: JsonArray,
        val rollingBackException: JsonObject?,
    )

    fun fetchPendingCoroutineRun(
        connection: SqlConnection,
        coroutineName: String,
        eventLoopStrategy: EventLoopStrategy,
    ): Uni<PendingCoroutineRun?> =
        connection
            .preparedQuery(finalSelect(eventLoopStrategy).build())
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
                        .preparedQuery(finalSelect(eventLoopStrategy, true).build())
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
                                payload = row.getJsonObject("payload"),
                                createdAt =
                                    row.getLocalDateTime("created_at")
                                        .atZone(ZoneId.systemDefault()),
                                cooperationLineage =
                                    row.getArrayOfUUIDs("cooperation_lineage").toList(),
                                step = row.getString("step"),
                                latestScopeContext = row.getJsonObject("latest_scope_context"),
                                latestContext = row.getJsonObject("latest_context"),
                                childRolledBackExceptions =
                                    row.getJsonArray("child_rolled_back_exceptions"),
                                childRollbackFailedExceptions =
                                    row.getJsonArray("child_rollback_failed_exceptions"),
                                rollingBackException = row.getJsonObject("rolling_back_exception"),
                            )
                        )
                }
            }

    fun fetchGiveUpExceptions(
        connection: SqlConnection,
        giveUpSqlProvider: (String) -> String,
        cooperationLineage: List<UUID>,
    ): Uni<List<CooperationException>> =
        connection
            .preparedQuery(
                """
                    WITH seen AS (
                        SELECT * FROM message_event WHERE cooperation_lineage = $1 AND type = 'SEEN'
                    )
                    ${giveUpSqlProvider("seen")}
                """
                    .trimIndent()
            )
            .execute(Tuple.of(cooperationLineage.toTypedArray()))
            .map { rowSet ->
                rowSet.map {
                    CooperationFailure.toCooperationException(
                        it.getJsonObject("exception").mapTo(CooperationFailure::class.java)
                    )
                }
            }
}
