package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop

import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException

sealed interface RollbackState {

    sealed interface NoThrowable : RollbackState

    sealed interface ThrowableExists : RollbackState {
        val throwable: Throwable
    }

    sealed interface Me : RollbackState {
        sealed interface NotRollingBack : Me, NoThrowable

        sealed interface RollingBack : Me, ThrowableExists
    }

    sealed interface Children : RollbackState {
        sealed interface NoRollbacks : Children, NoThrowable

        sealed interface Rollbacks : Children, ThrowableExists {
            sealed interface Successful : Rollbacks

            sealed interface Failed : Rollbacks
        }
    }

    object Gucci : Me.NotRollingBack, Children.NoRollbacks

    class SuccessfullyRolledBackLastStep(override val throwable: CooperationException) :
        Me.RollingBack, Children.NoRollbacks, Children.Rollbacks.Successful

    // TODO: Doc that, unlike in Java suppressed exceptions, the rollback failures take precedence
    data class ChildrenFailedWhileRollingBackLastStep(
        override val throwable: ChildRollbackFailedException
    ) : Me.RollingBack, Children.Rollbacks.Failed {
        constructor(
            rollbackFailures: List<CooperationException>,
            originalRollbackCause: CooperationException,
        ) : this(
            ChildRollbackFailedException(
                rollbackFailures +
                    listOfNotNull(
                        // Prevent exceptions pointlessly multiplying ad absurdum
                        originalRollbackCause.takeIf { !rollbackFailures.containsRecursively(it) }
                    )
            )
        )
    }

    class ChildrenFailedAndSuccessfullyRolledBack(
        override val throwable: ChildRolledBackException
    ) : Me.NotRollingBack, Children.Rollbacks.Successful {

        constructor(
            childrenFailures: List<CooperationException>
        ) : this(ChildRolledBackException(childrenFailures))
    }

    // TODO: Doc that, unlike in Java suppressed exceptions, the rollback failures take precedence
    class ChildrenFailedAndFailedToRollBack(override val throwable: ChildRollbackFailedException) :
        Me.NotRollingBack, Children.Rollbacks.Failed {

        constructor(
            rollbackFailures: List<CooperationException>,
            originalRollbackCauses: List<CooperationException>,
        ) : this(
            ChildRollbackFailedException(
                // This order ensures the first rollback failure is used as the cause
                rollbackFailures + ChildRolledBackException(originalRollbackCauses)
            )
        )
    }
}

private fun List<CooperationException>.containsRecursively(
    exception: CooperationException
): Boolean = any { it == exception || it.causes.containsRecursively(exception) }
