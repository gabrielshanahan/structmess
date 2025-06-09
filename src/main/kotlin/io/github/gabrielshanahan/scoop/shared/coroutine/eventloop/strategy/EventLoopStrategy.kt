package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy

import java.time.OffsetDateTime
import org.intellij.lang.annotations.Language

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

abstract class BaseEventLoopStrategy(val ignoreOlderThan: OffsetDateTime) : EventLoopStrategy {

    @Language("PostgreSQL")
    override fun start(emitted: String): String = ignoreHierarchiesOlderThan(ignoreOlderThan)

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

    @Language("PostgreSQL")
    override fun giveUpOnRollbackPath(seen: String): String =
        """
        ${rollbackDeadlineMissed(seen)}
        
        UNION ALL
        
        ${absoluteDeadlineMissed(seen)}
    """
            .trimIndent()
}
