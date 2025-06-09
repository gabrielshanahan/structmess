package io.github.gabrielshanahan.scoop.reactive.coroutine.builder

import io.github.gabrielshanahan.scoop.reactive.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.reactive.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.has
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.BaseEventLoopStrategy
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonObject
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration
import org.intellij.lang.annotations.Language

const val SLEEP_TOPIC = "sleep-9d24148d-d851-4107-8beb-e5c57f5cca88"

data object SleepUntilKey : CooperationContext.MappedKey<SleepUntil>()

data class SleepUntil(val wakeAfter: OffsetDateTime) :
    CooperationContext.MappedElement(SleepUntilKey)

fun sleepFor(duration: Duration): SleepUntil =
    SleepUntil(OffsetDateTime.now().plus(duration.inWholeMicroseconds, ChronoUnit.MICROS))

fun sleepUntil(wakeAfter: OffsetDateTime): SleepUntil = SleepUntil(wakeAfter)

class SleepEventLoopStrategy(ignoreOlderThan: OffsetDateTime) :
    BaseEventLoopStrategy(ignoreOlderThan) {
    @Language("PostgreSQL")
    override fun resumeHappyPath(
        candidateSeen: String,
        emittedInLatestStep: String,
        childSeens: String,
    ): String =
        """
        EXISTS (
            SELECT 1
                FROM $candidateSeen
                WHERE $candidateSeen.context ? 'SleepUntilKey'
                    AND ($candidateSeen.context->'SleepUntilKey'->>'wakeAfter')::timestamptz < CLOCK_TIMESTAMP()
        )
    """
            .trimIndent()

    @Language("PostgreSQL")
    override fun resumeRollbackPath(
        candidateSeen: String,
        rollbacksEmittedInLatestStep: String,
        childRollingBacks: String,
    ): String = "FALSE"
}

fun SagaBuilder.sleepForStep(name: String, duration: Duration) {
    uniStep(
        invoke = { scope, _ ->
            scope.launch(SLEEP_TOPIC, JsonObject(), sleepFor(duration)).replaceWith(Unit)
        },
        name = name,
    )
}

fun SagaBuilder.sleepForStep(duration: Duration) = sleepForStep(steps.size.toString(), duration)

fun SagaBuilder.scheduledStep(
    name: String,
    wakeAfter: OffsetDateTime,
    invoke: (CooperationScope, Message) -> Unit,
    rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
    handleChildFailures: ((CooperationScope, Message, Throwable) -> Unit)? = null,
) {
    uniStep(
        invoke = { scope, _ ->
            scope.launch(SLEEP_TOPIC, JsonObject(), sleepUntil(wakeAfter)).replaceWith(Unit)
        },
        name = "$name (waiting for scheduled time)",
    )
    step(name, invoke, rollback, handleChildFailures)
}

fun SagaBuilder.scheduledStep(
    runAfter: OffsetDateTime,
    invoke: (CooperationScope, Message) -> Unit,
    rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
    handleChildFailures: ((CooperationScope, Message, Throwable) -> Unit)? = null,
) = scheduledStep(steps.size.toString(), runAfter, invoke, rollback, handleChildFailures)

data object RunCountKey : CooperationContext.MappedKey<RunCount>()

data class RunCount(val value: Int = 0) : CooperationContext.MappedElement(RunCountKey)

fun SagaBuilder.periodic(name: String, runEvery: Duration, runCount: Int) {
    sleepForStep("$name (sleep)", runEvery)
    uniStep(
        name = "$name (launch next)",
        invoke = { scope, message ->
            if (!scope.context.has(RunCountKey)) {
                scope.context += RunCount(0)
            }

            scope.context += RunCount(scope.context[RunCountKey]!!.value + 1)

            if (scope.context[RunCountKey]!!.value < runCount) {
                    scope.launchOnGlobalScope(message.topic, message.payload, scope.context)
                } else {
                    Uni.createFrom().item(Unit)
                }
                .replaceWith(Unit)
        },
        rollback = null,
        handleChildFailures = null,
    )
}

fun SagaBuilder.periodic(runEvery: Duration, runCount: Int) =
    periodic(steps.size.toString(), runEvery, runCount)
