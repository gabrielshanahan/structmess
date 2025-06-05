package io.github.gabrielshanahan.structmess.coroutine

import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import kotlin.time.Duration

abstract class CancellationToken<SELF : CancellationToken<SELF>>(
    key: CooperationContext.MappedKey<*>
) : CooperationContext.MappedElement(key) {
    override fun plus(context: CooperationContext): CooperationContext =
        if (context is CancellationToken<SELF> && context.has(key)) {
            @Suppress("UNCHECKED_CAST") and(context as SELF)
        } else {
            super.plus(context)
        }

    abstract fun and(other: SELF): CancellationToken<SELF>
}

val postgresMaxTime: OffsetDateTime =
    OffsetDateTime.of(9999, 12, 31, 23, 59, 59, 999999000, ZoneOffset.UTC)

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
        OffsetDateTime.now().plus(timeout.inWholeMicroseconds, ChronoUnit.MILLIS),
        source,
    )

fun noHappyPathTimeout(source: String): HappyPathDeadline =
    HappyPathDeadline(postgresMaxTime, source)

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
        OffsetDateTime.now().plus(timeout.inWholeMicroseconds, ChronoUnit.MILLIS),
        source,
    )

fun noRollbackPathTimeout(source: String): RollbackPathDeadline =
    RollbackPathDeadline(postgresMaxTime, source)

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
        OffsetDateTime.now().plus(timeout.inWholeMicroseconds, ChronoUnit.MILLIS),
        source,
    )

fun noAbsoluteTimeout(source: String): AbsoluteDeadline = AbsoluteDeadline(postgresMaxTime, source)
