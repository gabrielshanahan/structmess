package io.github.gabrielshanahan.scoop.shared

import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.groups.MultiTimePeriod
import java.time.Duration

fun MultiTimePeriod.everyJittered(duration: Duration): Multi<Long> =
    every(duration)
        .onOverflow()
        .drop() // avoid backpressure issues with delay
        .onItem()
        .transformToUniAndConcatenate { tick ->
            val jitterMillis = duration.toMillis() * 0.02
            Uni.createFrom()
                .item(tick)
                .onItem()
                .delayIt()
                .by(Duration.ofMillis(jitterMillis.toLong()))
        }
