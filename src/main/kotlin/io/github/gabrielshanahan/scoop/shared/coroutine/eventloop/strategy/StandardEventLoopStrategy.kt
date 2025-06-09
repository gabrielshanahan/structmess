package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy

import java.time.OffsetDateTime

class StandardEventLoopStrategy(
    ignoreOlderThan: OffsetDateTime,
    val getTopicsToHandlerNames: () -> Map<String, List<String>>,
) : BaseEventLoopStrategy(ignoreOlderThan) {
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
}
