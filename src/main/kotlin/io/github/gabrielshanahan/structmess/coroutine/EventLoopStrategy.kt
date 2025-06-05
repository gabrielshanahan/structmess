package io.github.gabrielshanahan.structmess.coroutine

import io.github.gabrielshanahan.structmess.messaging.HandlerRegistry
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.intellij.lang.annotations.Language

// TODO: Resumption strategy? vs. cancellation strategy
// TODO: We can implement cancelation tokens the same way?
// TODO: EventLoopStrategy/SuspensionPointStrategy

// TODO: Sleep can be done by emitting a sleep message, which is "picked up" by a dedicated
//  coroutine (par of the library, so part of every instance by default) which has a
//  strategy that, instead of "all seen exist", only says "you can  never continue", and
//  cancels after time in deadline has elapsed (the sleep message takes  no params itself).
//  Sleep is always emitted by a supervisor scope, so the cancellation  gets caught and
//  ignored (check needs to be done first though).
//  We could even trick things by just adding a fake entry to HandlerRegistry, without actually
// having a handler registered.
//
// We can then use this to do e.g. "run when X", where X is some sort of event happening - we simply
// register a strategy that checks that X happened, whatever X might be. It could also be the
// contents of the context of some other run, so you could even do races (actually races would be
// dependent on finding the success value of another run).
//
// TODO: SHOW ALSO HOW TO DO SCHEDULED/REPEATING TASKS! (use try finally)
interface EventLoopStrategy {
    // Should SEEN be emitted
    fun start(emitted: String): String

    // TODO: Doc that giving up is done by scheduling the task, because giving up is checked just
    //  before the coroutine is run.

    fun resumeHappyPath(
        candidateSeen: String,
        emittedInLatestStep: String,
        childSeens: String,
    ): String

    fun giveUpOnHappyPath(seen: String): String

    fun resumeRollbackPath(
        candidateSeen: String,
        rollbacksEmittedInLatestStep: String,
        childRollingBacks: String,
    ): String

    fun giveUpOnRollbackPath(seen: String): String
}

class StandardEventLoopStrategy(
    val ignoreOlderThan: ZonedDateTime,
    val getTopicsToHandlerNames: () -> Map<String, List<String>>,
) : EventLoopStrategy {

    @Language("PostgreSQL")
    override fun start(emitted: String): String = ignoreHierarchiesOlderThan(ignoreOlderThan)

    override fun resumeHappyPath(
        candidateSeen: String,
        emittedInLatestStep: String,
        childSeens: String,
    ): String =
        allEmissionsHaveCorrespondingContinuationStarts(
            getTopicsToHandlerNames(),
            candidateSeen,
            emittedInLatestStep,
            childSeens,
        )

    // TODO: Doc why 'SEEN', 'SUSPENDED', 'COMMITTED', 'ROLLING_BACK'
    @Language("PostgreSQL")
    override fun giveUpOnHappyPath(seen: String): String =
        """
        ${cancellationRequested(seen)}
        
        UNION ALL
        
        ${happyPathDeadlineMissed(seen)}
        
        UNION ALL
        
        ${absoluteDeadlineMissed(seen)}
        """
            .trimIndent()

    override fun resumeRollbackPath(
        candidateSeen: String,
        rollbacksEmittedInLatestStep: String,
        childRollingBacks: String,
    ): String =
        allEmissionsHaveCorrespondingContinuationStarts(
            getTopicsToHandlerNames(),
            candidateSeen,
            rollbacksEmittedInLatestStep,
            childRollingBacks,
        )

    @Language("PostgreSQL")
    override fun giveUpOnRollbackPath(seen: String): String =
        """
        ${rollbackDeadlineMissed(seen)}
        
        UNION ALL
        
        ${absoluteDeadlineMissed(seen)}
    """
            .trimIndent()
}

@Language("PostgreSQL")
fun ignoreHierarchiesOlderThan(ignoreOlderThan: ZonedDateTime): String =
    """
        EXISTS (
            SELECT 1 
            FROM message_event root_emission
            WHERE root_emission.type = 'EMITTED'
                AND cardinality(root_emission.cooperation_lineage) = 1
                AND root_emission.cooperation_lineage[1] = emitted.cooperation_lineage[1]
                AND root_emission.created_at >= '${ignoreOlderThan.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}'
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
            WHERE last_event.context ? '$deadlineKey'
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

fun HandlerRegistry.eventLoopStrategy() =
    StandardEventLoopStrategy(ZonedDateTime.now(), this::listenersByTopic)
