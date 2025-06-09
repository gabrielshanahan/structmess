package io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.isNotEmpty
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.SqlClient
import io.vertx.mutiny.sqlclient.SqlConnection
import io.vertx.mutiny.sqlclient.Tuple
import jakarta.enterprise.context.ApplicationScoped
import java.util.UUID

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
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, exception, context) " +
                    "VALUES ($1, '$messageEventType', $2, $3, $4, $5, $6, $7)"
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
                    CooperationFailure.Companion.toCooperationException(
                        it.getJsonObject("exception").mapTo(CooperationFailure::class.java)
                    )
                }
            }
}
