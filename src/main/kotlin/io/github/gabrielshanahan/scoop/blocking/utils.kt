package io.github.gabrielshanahan.scoop.blocking

import java.sql.Connection
import org.codejargon.fluentjdbc.api.FluentJdbc

inline fun <T> FluentJdbc.transactional(crossinline block: (Connection) -> T): T =
    query().let { query ->
        query.transaction().`in` { query.plainConnection { connection -> block(connection) } }
    }

inline fun repeatWhileRepeatCalled(
    crossinline block: (repeatCount: Int, repeat: () -> Unit) -> Unit
) {
    var repeatCount = 0
    var repeat = true
    while (repeat) {
        repeat = false
        repeatCount++
        block(repeatCount) { repeat = true }
    }
}
