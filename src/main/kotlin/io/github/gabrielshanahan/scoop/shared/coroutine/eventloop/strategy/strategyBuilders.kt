package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy

import java.time.OffsetDateTime
import org.intellij.lang.annotations.Language

@Language("PostgreSQL")
fun ignoreHierarchiesOlderThan(ignoreOlderThan: OffsetDateTime): String =
    """
        EXISTS (
            SELECT 1 
            FROM message_event root_emission
            WHERE root_emission.type = 'EMITTED'
                AND cardinality(root_emission.cooperation_lineage) = 1
                AND root_emission.cooperation_lineage[1] = emitted.cooperation_lineage[1]
                AND root_emission.created_at >= '$ignoreOlderThan'
        )
    """
        .trimIndent()

@Language("PostgreSQL")
fun cancellationRequested(seen: String): String =
    """
    SELECT cancellation_request.exception::jsonb
        FROM message_event cancellation_request
        JOIN $seen ON cancellation_request.cooperation_lineage <@ $seen.cooperation_lineage
        AND cancellation_request.type = 'CANCELLATION_REQUESTED'
    """
        .trimIndent()

fun happyPathDeadlineMissed(seen: String): String =
    deadlineMissed(seen, "happy path", listOf("SUSPENDED", "SEEN"))

fun rollbackDeadlineMissed(seen: String): String =
    deadlineMissed(seen, "rollback", listOf("SUSPENDED", "ROLLING_BACK"))

fun absoluteDeadlineMissed(seen: String): String =
    deadlineMissed(seen, "absolute", listOf("SEEN", "SUSPENDED", "COMMITTED", "ROLLING_BACK"))

@Language("PostgreSQL")
fun deadlineMissed(seen: String, deadlineType: String, eventTypes: List<String>): String {
    val deadline = "${deadlineType.capitalizeWords().split(" ").joinToString("")}Deadline"
    val deadlineKey = "${deadline}Key"

    return """
    SELECT jsonb_build_object(
            'message', 'Missed $deadlineType deadline of ' || 
                (deadline_record.context->'$deadlineKey'->>'source') || 
                ' at ' || 
                (deadline_record.context->'$deadlineKey'->>'deadline') || 
                '. Deadline trace: ' || 
                (COALESCE(to_jsonb(deadline_record.context->'$deadlineKey'->'trace')::text, '[]')),
            'type', 'Missed$deadline',
            'source', (deadline_record.context->'$deadlineKey'->>'source'),
            'stackTrace', '[]'::jsonb,
            'causes', '[]'::jsonb
        ) as exception
        FROM (
            SELECT last_event.context
            FROM message_event last_event
            JOIN $seen ON last_event.cooperation_lineage = $seen.cooperation_lineage
            WHERE jsonb_exists_any_indexed(last_event.context, '$deadlineKey')
            AND last_event.type IN ${eventTypes.asSqlList()}
            AND (last_event.context->'$deadlineKey'->>'deadline')::timestamptz < CLOCK_TIMESTAMP()
            LIMIT 1
        ) AS deadline_record
        WHERE deadline_record.context IS NOT NULL
    """
        .trimIndent()
}

private fun String.capitalizeWords() =
    split(" ").joinToString(" ") {
        it.replaceFirstChar { if (it.isLowerCase()) it.titlecase() else it.toString() }
    }

private fun List<String>.asSqlList() = joinToString(prefix = "(", postfix = ")") { "'$it'" }

@Language("PostgreSQL")
fun allEmissionsHaveCorrespondingContinuationStarts(
    topicsToHandlerNames: Map<String, List<String>>,
    candidateSeen: String,
    emissionInLatestStep: String,
    emissionContinuationStart: String,
): String {
    val topicToHandlerPairs =
        topicsToHandlerNames.flatMap { (topic, handlers) ->
            handlers.map { handler -> topic to handler }
        }
    if (topicToHandlerPairs.isEmpty()) {
        return "TRUE"
    }
    val valuesClause =
        topicToHandlerPairs.joinToString(", ") { (topic, handler) -> "('$topic', '$handler')" }

    return """
        NOT EXISTS ( -- all expected handlers have reacted to emissions from the previous step
            SELECT 1
                FROM $emissionInLatestStep
                LEFT JOIN $emissionContinuationStart ON $emissionContinuationStart.parent_cooperation_lineage = $emissionInLatestStep.cooperation_lineage
            WHERE
                $emissionInLatestStep.cooperation_lineage = $candidateSeen.cooperation_lineage
                AND 
                EXISTS (
                    SELECT
                        1
                    FROM (VALUES $valuesClause) AS topic_handler(topic, handler)
                    JOIN message ON topic_handler.topic = message.topic
                    JOIN $emissionInLatestStep ON $emissionInLatestStep.message_id = message.id
                    LEFT JOIN 
                        $emissionContinuationStart ON $emissionContinuationStart.message_id = $emissionInLatestStep.message_id
                        AND $emissionContinuationStart.coroutine_name = topic_handler.handler
                    WHERE $emissionContinuationStart.id is NULL
                )  
        )
        """
        .trimIndent()
}
