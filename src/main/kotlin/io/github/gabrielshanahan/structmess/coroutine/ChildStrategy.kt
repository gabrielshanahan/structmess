package io.github.gabrielshanahan.structmess.coroutine

import io.github.gabrielshanahan.structmess.messaging.HandlerRegistry
import kotlin.time.Duration

// TODO: Resumption strategy? vs. cancellation strategy
// TODO: We can implement cancelation tokens the same way?
// TODO: EventLoopStrategy/SuspensionPointStrategy

// TODO: Sleep can be done by emitting a sleep message, which is "picked up" by a dedicated
// coroutine which has a strategy that,
//  instead of "all seen exist", only says "you can never continue", and cancels after time in
// deadline has elapsed (the sleep message takes no params itself). Sleep is always emitted
//  by a supervisor scope, so the cancellation gets caught and ignored (check needs to be done first
// though).
//
// We can then use this to do e.g. "run when X", where X is some sort of event happening - we simply
// register a strategy that checks that X happened, whatever X might be. It could also be the
// contents
// of the context of some other run, so you could even do races (actually races would be dependent
// on
// finding the success value of another run).
fun interface ChildStrategy {
    fun anySeenEventMissingSQL(emissionsAlias: String, seenAlias: String): String
}

fun allMustFinish(topicsToHandlerNames: Map<String, List<String>>): ChildStrategy {
    val pairs =
        topicsToHandlerNames.flatMap { (topic, handlers) ->
            handlers.map { handler -> topic to handler }
        }

    return if (pairs.isEmpty()) {
        ChildStrategy { _, _ -> "true" }
    } else {
        ChildStrategy { emissionsAlias, seenAlias ->
            val valuesClause =
                pairs.joinToString(", ") { (topic, handler) -> "('$topic', '$handler')" }
            // Exists any SEEN event that's missing
            """
            EXISTS (
                SELECT
                    1
                FROM (VALUES $valuesClause) AS topic_handler(topic, handler)
                JOIN message ON topic_handler.topic = message.topic
                JOIN $emissionsAlias ON $emissionsAlias.message_id = message.id
                LEFT JOIN 
                    $seenAlias ON $seenAlias.message_id = $emissionsAlias.message_id
                    AND $seenAlias.coroutine_name = topic_handler.handler
                WHERE $seenAlias.id is NULL
            )    
            """
                .trimIndent()
        }
    }
}

// TODO: Test
fun timeMustElapse(duration: Duration) = ChildStrategy { emissionsAlias, _ ->
    "$emissionsAlias.created_at > now() - INTERVAL '${duration.inWholeSeconds} second'"
}

fun HandlerRegistry.childStrategy() = ChildStrategy { emissionsAlias, seenAlias ->
    allMustFinish(listenersByTopic()).anySeenEventMissingSQL(emissionsAlias, seenAlias)
}
