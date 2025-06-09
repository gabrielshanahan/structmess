package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline

import io.github.gabrielshanahan.scoop.shared.coroutine.context.CancellationToken
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.postgresMaxTime
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration

data object RollbackPathDeadlineKey : CooperationContext.MappedKey<RollbackPathDeadline>()

data class RollbackPathDeadline(
    val deadline: OffsetDateTime,
    val source: String,
    val trace: Set<RollbackPathDeadline> = emptySet(),
) : CancellationToken<RollbackPathDeadline>(RollbackPathDeadlineKey) {
    override fun and(other: RollbackPathDeadline): CancellationToken<RollbackPathDeadline> {
        check(key == other.key) { "Trying to mix together $key and ${other.key}" }

        val earlierDeadline = minOf(this, other, compareBy { it.deadline })
        val laterDeadline = if (earlierDeadline == this) other else this
        return RollbackPathDeadline(
            earlierDeadline.deadline,
            earlierDeadline.source,
            earlierDeadline.trace + laterDeadline.asTrace(),
        )
    }

    private fun asTrace(): Set<RollbackPathDeadline> = buildSet {
        add(RollbackPathDeadline(deadline, source))
        addAll(trace)
    }
}

fun rollbackPathTimeout(timeout: Duration, source: String): RollbackPathDeadline =
    RollbackPathDeadline(
        OffsetDateTime.now().plus(timeout.inWholeMicroseconds, ChronoUnit.MICROS),
        source,
    )

fun noRollbackPathTimeout(source: String): RollbackPathDeadline =
    RollbackPathDeadline(postgresMaxTime, source)
