package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline

import io.github.gabrielshanahan.scoop.shared.coroutine.context.CancellationToken
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.postgresMaxTime
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration

data object HappyPathDeadlineKey : CooperationContext.MappedKey<HappyPathDeadline>()

data class HappyPathDeadline(
    val deadline: OffsetDateTime,
    val source: String,
    val trace: Set<HappyPathDeadline> = emptySet(),
) : CancellationToken<HappyPathDeadline>(HappyPathDeadlineKey) {
    override fun and(other: HappyPathDeadline): CancellationToken<HappyPathDeadline> {
        check(key == other.key) { "Trying to mix together $key and ${other.key}" }

        val earlierDeadline = minOf(this, other, compareBy { it.deadline })
        val laterDeadline = if (earlierDeadline == this) other else this
        return HappyPathDeadline(
            earlierDeadline.deadline,
            earlierDeadline.source,
            earlierDeadline.trace + laterDeadline.asTrace(),
        )
    }

    private fun asTrace(): Set<HappyPathDeadline> = buildSet {
        add(HappyPathDeadline(deadline, source))
        addAll(trace)
    }
}

fun happyPathTimeout(timeout: Duration, source: String): HappyPathDeadline =
    HappyPathDeadline(
        OffsetDateTime.now().plus(timeout.inWholeMicroseconds, ChronoUnit.MICROS),
        source,
    )

fun noHappyPathTimeout(source: String): HappyPathDeadline =
    HappyPathDeadline(postgresMaxTime, source)
