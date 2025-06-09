package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline

import io.github.gabrielshanahan.scoop.shared.coroutine.context.CancellationToken
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.postgresMaxTime
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration

data object AbsoluteDeadlineKey : CooperationContext.MappedKey<AbsoluteDeadline>()

data class AbsoluteDeadline(
    val deadline: OffsetDateTime,
    val source: String,
    val trace: Set<AbsoluteDeadline> = emptySet(),
) : CancellationToken<AbsoluteDeadline>(AbsoluteDeadlineKey) {
    override fun and(other: AbsoluteDeadline): CancellationToken<AbsoluteDeadline> {
        check(key == other.key) { "Trying to mix together $key and ${other.key}" }

        val earlierDeadline = minOf(this, other, compareBy { it.deadline })
        val laterDeadline = if (earlierDeadline == this) other else this
        return AbsoluteDeadline(
            earlierDeadline.deadline,
            earlierDeadline.source,
            earlierDeadline.trace + laterDeadline.asTrace(),
        )
    }

    private fun asTrace(): Set<AbsoluteDeadline> = buildSet {
        add(AbsoluteDeadline(deadline, source))
        addAll(trace)
    }
}

fun absoluteTimeout(timeout: Duration, source: String): AbsoluteDeadline =
    AbsoluteDeadline(
        OffsetDateTime.now().plus(timeout.inWholeMicroseconds, ChronoUnit.MICROS),
        source,
    )

fun noAbsoluteTimeout(source: String): AbsoluteDeadline = AbsoluteDeadline(postgresMaxTime, source)
