package io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy
import org.intellij.lang.annotations.Language

data class SQL(val cte: SQL?, val name: String?, @Language("PostgreSQL") val sql: String)

fun SQL.appendAs(name: String?, @Language("PostgreSQL") sql: String): SQL = SQL(this, name, sql)

val candidateSeens =
    SQL(
        cte = null,
        "candidate_seens",
        """
            SELECT DISTINCT ON (seen.id)
                seen.id, 
                seen.message_id,
                seen.cooperation_lineage,
                seen.context,
                emitted.created_at as emitted_at, 
                rollback_emitted.created_at as rollback_emitted_at
            FROM message_event seen
            JOIN message_event AS emitted
                ON seen.message_id = emitted.message_id 
                    AND emitted.type = 'EMITTED'
            LEFT JOIN message_event AS rolling_back
                ON seen.message_id = rolling_back.message_id 
                    AND rolling_back.type = 'ROLLING_BACK'
                    AND rolling_back.cooperation_lineage = seen.cooperation_lineage
            LEFT JOIN message_event AS rollback_emitted
                ON seen.message_id = rollback_emitted.message_id 
                    AND rollback_emitted.type = 'ROLLBACK_EMITTED'
            WHERE seen.coroutine_name = :coroutine_name
              AND seen.type = 'SEEN'
              AND (
              ((rollback_emitted.id IS NULL AND rolling_back.id IS NULL) AND
                NOT EXISTS (
                    SELECT 1
                    FROM message_event
                    WHERE cooperation_lineage = seen.cooperation_lineage
                      AND type = 'COMMITTED'
                ))
                OR
                (
                  (rollback_emitted.id IS NULL AND rolling_back.id IS NOT NULL)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM message_event
                      WHERE cooperation_lineage = seen.cooperation_lineage
                        AND type IN ('ROLLED_BACK', 'ROLLBACK_FAILED')
                  )
                )
                OR
                (
                  (rollback_emitted.id IS NOT NULL AND rolling_back.id IS NOT NULL)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM message_event
                      WHERE cooperation_lineage = seen.cooperation_lineage
                        AND type IN ('ROLLED_BACK', 'ROLLBACK_FAILED')
                  )
                )
              )
        """
            .trimIndent(),
    )

val latestSuspended =
    candidateSeens.appendAs(
        "latest_suspended",
        """
                SELECT DISTINCT ON (message_event.message_id) message_event.cooperation_lineage, message_event.step, message_event.context, message_event.created_at
                FROM message_event
                JOIN candidate_seens ON message_event.cooperation_lineage = candidate_seens.cooperation_lineage
                WHERE message_event.type = 'SUSPENDED'
                ORDER BY message_event.message_id, message_event.created_at DESC
            """
            .trimIndent(),
    )

val childEmissionsInLatestStep =
    latestSuspended.appendAs(
        "child_emissions_in_latest_step",
        """
                SELECT emissions.*
                FROM message_event AS emissions
                JOIN latest_suspended
                    ON emissions.cooperation_lineage = latest_suspended.cooperation_lineage
                WHERE emissions.type = 'EMITTED'
                    AND emissions.step = latest_suspended.step
            """
            .trimIndent(),
    )

val childSeens =
    childEmissionsInLatestStep.appendAs(
        "child_seens",
        """
                SELECT seen.*, child_emissions_in_latest_step.cooperation_lineage as parent_cooperation_lineage
                FROM message_event seen
                JOIN child_emissions_in_latest_step ON
                    seen.message_id = child_emissions_in_latest_step.message_id
                WHERE seen.type = 'SEEN'
                    AND seen.cooperation_lineage <> child_emissions_in_latest_step.cooperation_lineage 
                    AND child_emissions_in_latest_step.cooperation_lineage <@ seen.cooperation_lineage 
                    AND cardinality(seen.cooperation_lineage) = cardinality(child_emissions_in_latest_step.cooperation_lineage) + 1
            """
            .trimIndent(),
    )

val terminatedChildSeens =
    childSeens.appendAs(
        "terminated_child_seens",
        """
                SELECT child_seens.*
                FROM message_event seen_terminations
                JOIN child_seens ON
                    seen_terminations.message_id = child_seens.message_id
                        AND seen_terminations.cooperation_lineage = child_seens.cooperation_lineage
                WHERE 
                    seen_terminations.type in ('COMMITTED', 'ROLLED_BACK', 'ROLLBACK_FAILED')
            """
            .trimIndent(),
    )

val childRollbackEmissionsInLatestStep =
    terminatedChildSeens.appendAs(
        "child_rollback_emissions_in_latest_step",
        """
                SELECT rollback_emissions.*
                FROM message_event AS rollback_emissions
                JOIN latest_suspended
                    ON rollback_emissions.cooperation_lineage = latest_suspended.cooperation_lineage
                WHERE rollback_emissions.type = 'ROLLBACK_EMITTED'
                    AND rollback_emissions.step = latest_suspended.step
            """
            .trimIndent(),
    )

val childRollingBacks =
    childRollbackEmissionsInLatestStep.appendAs(
        "child_rolling_backs",
        """
                SELECT
                    rolling_backs.*,
                    parent_seen.cooperation_lineage AS parent_cooperation_lineage
                FROM message_event rolling_backs
                JOIN candidate_seens AS parent_seen 
                    ON rolling_backs.cooperation_lineage <> parent_seen.cooperation_lineage 
                        AND parent_seen.cooperation_lineage <@ rolling_backs.cooperation_lineage 
                        AND cardinality(rolling_backs.cooperation_lineage) = cardinality(parent_seen.cooperation_lineage) + 1
                WHERE rolling_backs.type = 'ROLLING_BACK'
                ORDER BY rolling_backs.created_at
            """
            .trimIndent(),
    )

val terminatedChildRollingBacks =
    childRollingBacks.appendAs(
        "terminated_child_rolling_backs",
        """
                SELECT child_rolling_backs.*
                FROM message_event rolling_back_terminations
                JOIN child_rolling_backs ON
                    rolling_back_terminations.message_id = child_rolling_backs.message_id
                        AND rolling_back_terminations.cooperation_lineage = child_rolling_backs.cooperation_lineage
                WHERE
                    rolling_back_terminations.type in ('ROLLED_BACK', 'ROLLBACK_FAILED')
            """
            .trimIndent(),
    )

fun candidateSeensWaitingToBeProcessed(eventLoopStrategy: EventLoopStrategy) =
    terminatedChildRollingBacks.appendAs(
        "candidate_seens_waiting_to_be_processed",
        """
                SELECT candidate_seens.*
                FROM candidate_seens
                WHERE
                    -- no rollback emissions present
                    (
                        NOT EXISTS ( -- no rollback emissions
                            SELECT 1
                            FROM child_rollback_emissions_in_latest_step
                            WHERE child_rollback_emissions_in_latest_step.cooperation_lineage = candidate_seens.cooperation_lineage
                        )
                        AND
                        (
                            (
                                -- strategy says resume
                                ${eventLoopStrategy.resumeHappyPath("candidate_seens", "child_emissions_in_latest_step", "child_seens")}
                                OR
                                -- strategy says give up
                                EXISTS(
                                    ${eventLoopStrategy.giveUpOnHappyPath("candidate_seens")}
                                )
                            )
                            AND 
                                NOT EXISTS ( -- every SEEN has a counterpart in terminated_child_seens
                                    SELECT 1
                                    FROM child_seens
                                    LEFT JOIN terminated_child_seens ON child_seens.cooperation_lineage = terminated_child_seens.cooperation_lineage
                                    WHERE
                                        child_seens.parent_cooperation_lineage = candidate_seens.cooperation_lineage
                                            AND terminated_child_seens.cooperation_lineage IS NULL
                                )
                        )
                    )
                    OR
                    -- rollback emissions present
                    (
                        EXISTS ( -- rollback emissions present
                            SELECT 1
                            FROM child_rollback_emissions_in_latest_step
                            WHERE child_rollback_emissions_in_latest_step.cooperation_lineage = candidate_seens.cooperation_lineage
                        )
                        AND
                        (
                            (
                                -- strategy says resume
                                ${eventLoopStrategy.resumeRollbackPath("candidate_seens", "child_rollback_emissions_in_latest_step", "child_rolling_backs")}
                                OR
                                -- strategy says give up
                                EXISTS(
                                    ${eventLoopStrategy.giveUpOnRollbackPath("candidate_seens")}
                                )
                            )
                            AND 
                                NOT EXISTS ( -- every ROLLING_BACK has a counterpart in terminated_child_rolling_backs
                                    SELECT
                                        1
                                    FROM child_rolling_backs
                                    LEFT JOIN terminated_child_rolling_backs ON child_rolling_backs.cooperation_lineage = terminated_child_rolling_backs.cooperation_lineage
                                    WHERE 
                                        child_rolling_backs.parent_cooperation_lineage = candidate_seens.cooperation_lineage
                                            AND terminated_child_rolling_backs.cooperation_lineage IS NULL
                                )
                        )
                    )
            """
            .trimIndent(),
    )

fun seenForProcessing(eventLoopStrategy: EventLoopStrategy, secondRunAfterLock: Boolean = false) =
    if (!secondRunAfterLock) {
        candidateSeensWaitingToBeProcessed(eventLoopStrategy)
            .appendAs(
                "seen_for_processing",
                """
                SELECT candidate_seens_waiting_to_be_processed.cooperation_lineage, candidate_seens_waiting_to_be_processed.message_id, candidate_seens_waiting_to_be_processed.context
                FROM message_event
                JOIN candidate_seens_waiting_to_be_processed ON message_event.id = candidate_seens_waiting_to_be_processed.id
                -- We want to process things in the order they were emitted, and rollbacks always happen after emissions
                ORDER BY COALESCE(candidate_seens_waiting_to_be_processed.rollback_emitted_at, candidate_seens_waiting_to_be_processed.emitted_at), candidate_seens_waiting_to_be_processed.emitted_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            """
                    .trimIndent(),
            )
    } else {
        // We need to leave out the FOR UPDATE SKIP LOCKED, since once a record is locked, it's
        // locked even for the transaction that locked it
        candidateSeensWaitingToBeProcessed(eventLoopStrategy)
            .appendAs(
                "seen_for_processing",
                """
                SELECT candidate_seens_waiting_to_be_processed.cooperation_lineage, candidate_seens_waiting_to_be_processed.message_id, candidate_seens_waiting_to_be_processed.context
                FROM candidate_seens_waiting_to_be_processed
                WHERE candidate_seens_waiting_to_be_processed.message_id = :message_id
            """
                    .trimIndent(),
            )
    }

fun lastTwoEvents(eventLoopStrategy: EventLoopStrategy, secondRunAfterLock: Boolean = false) =
    seenForProcessing(eventLoopStrategy, secondRunAfterLock)
        .appendAs(
            "last_two_events",
            """
            SELECT
                last_two_events.context,
                last_two_events.type,
                last_two_events.step
            FROM message_event last_two_events
            JOIN seen_for_processing ON seen_for_processing.cooperation_lineage = last_two_events.cooperation_lineage
            WHERE last_two_events.type IN ('SEEN', 'SUSPENDED', 'COMMITTED', 'ROLLING_BACK') --TODO: Doc why these
            ORDER BY last_two_events.created_at DESC
            LIMIT 2
        """
                .trimIndent(),
        )

fun finalSelect(eventLoopStrategy: EventLoopStrategy, secondRunAfterLock: Boolean = false) =
    lastTwoEvents(eventLoopStrategy, secondRunAfterLock)
        .appendAs(
            null,
            """
                SELECT
                    message.*,
                    seen_for_processing.cooperation_lineage,
                    latest_suspended.step,
                    last_event.context as latest_context,
                    CASE
                        WHEN last_event.type = 'ROLLING_BACK' AND last_event.step IS NULL THEN second_to_last_event.context -- TODO: DOC THE step is null which is true only when it's copied from the parent
                    END AS latest_scope_context,
                    (
                        SELECT
                            COALESCE(JSON_AGG(exception), '[]'::json)
                        FROM child_rolling_backs
                        JOIN seen_for_processing ON child_rolling_backs.parent_cooperation_lineage = seen_for_processing.cooperation_lineage
                    ) AS child_rolled_back_exceptions,
                    (
                        SELECT
                            COALESCE(JSON_AGG(termination_event.exception), '[]'::json)
                        FROM child_rolling_backs
                        JOIN message_event termination_event ON child_rolling_backs.cooperation_lineage = termination_event.cooperation_lineage
                        WHERE termination_event.type = 'ROLLBACK_FAILED'
                    ) AS child_rollback_failed_exceptions,
                    (
                        SELECT
                            exception
                        FROM message_event
                        JOIN seen_for_processing ON
                                message_event.cooperation_lineage = seen_for_processing.cooperation_lineage
                        WHERE message_event.type = 'ROLLING_BACK'
                          AND exception IS NOT NULL
                        LIMIT 1
                    ) AS rolling_back_exception
                FROM seen_for_processing
                LEFT JOIN latest_suspended ON seen_for_processing.cooperation_lineage = latest_suspended.cooperation_lineage
                JOIN (SELECT * FROM last_two_events LIMIT 1) last_event ON TRUE
                LEFT JOIN (SELECT * FROM last_two_events OFFSET 1 LIMIT 1) second_to_last_event ON TRUE
                JOIN message ON seen_for_processing.message_id = message.id
            """
                .trimIndent(),
        )

fun commatize(str: String) = if (str.isNotBlank()) "$str," else str

fun withWITH(str: String) = if (str.isNotBlank()) "WITH $str" else str

fun SQL?.asCTE(): String =
    when (this) {
        null -> ""
        else ->
            """
        |${commatize(cte.asCTE())}
        |$name AS (
        |    $sql
        |)
    """
                .trimMargin()
    }

fun SQL.build(): String =
    """
    |${withWITH(cte.asCTE())}
    |$sql;
"""
        .trimMargin()
