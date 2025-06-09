package io.github.gabrielshanahan.scoop.blocking.coroutine

import io.agroal.pool.wrapper.ConnectionWrapper
import io.github.gabrielshanahan.scoop.blocking.JsonbHelper
import io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.Continuation
import io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.buildHappyPathContinuation
import io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.buildRollbackPathContinuation
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.MessageEventRepository
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.StructuredCooperationManager
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.blocking.repeatWhileRepeatCalled
import io.github.gabrielshanahan.scoop.blocking.transactional
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.emptyContext
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.LastSuspendedStep
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.RollbackState
import io.github.gabrielshanahan.scoop.shared.coroutine.renderAsString
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import io.github.gabrielshanahan.scoop.shared.everyJittered
import io.smallrye.mutiny.Multi
import jakarta.enterprise.context.ApplicationScoped
import java.sql.Connection
import java.time.Duration
import java.util.*
import kotlin.concurrent.thread
import org.codejargon.fluentjdbc.api.FluentJdbc
import org.jboss.logging.Logger
import org.postgresql.jdbc.PgConnection

@ApplicationScoped
class EventLoop(
    private val fluentJdbc: FluentJdbc,
    private val messageEventRepository: MessageEventRepository,
    private val structuredCooperationManager: StructuredCooperationManager,
    private val jsonbHelper: JsonbHelper,
) {
    private val logger = Logger.getLogger(javaClass)

    fun tick(topic: String, distributedCoroutine: DistributedCoroutine) {
        try {
            fluentJdbc.transactional { connection ->
                try {
                    structuredCooperationManager.startContinuationsForCoroutine(
                        connection,
                        distributedCoroutine.identifier.name,
                        distributedCoroutine.identifier.instance,
                        topic,
                        distributedCoroutine.eventLoopStrategy,
                    )
                } catch (e: Exception) {
                    logger.error(
                        "[${(connection as ConnectionWrapper).unwrap(PgConnection::class.java).backendPID}] Error when starting continuations for coroutine ${distributedCoroutine.identifier}",
                        e,
                    )
                    throw e
                }
            }

            repeatWhileRepeatCalled { repeatCount, repeat ->
                fluentJdbc.transactional { connection ->
                    try {
                        logger.info(
                            "Run number $repeatCount for topic $topic and coroutine ${distributedCoroutine.identifier}"
                        )
                        val coroutineState =
                            fetchSomePendingCoroutineState(connection, distributedCoroutine)
                        if (coroutineState == null) {
                            return@transactional
                        }
                        repeat()
                        val continuationResult =
                            resumeCoroutine(connection, distributedCoroutine, coroutineState)

                        if (continuationResult is Continuation.ContinuationResult.Failure) {
                            throw continuationResult.exception
                        }
                    } catch (e: Exception) {
                        logger.error(
                            "[${(connection as ConnectionWrapper).unwrap(PgConnection::class.java).backendPID}] Error when running coroutine ${distributedCoroutine.identifier}",
                            e,
                        )
                        throw e
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Error in when ticking", e)
        }
    }

    fun tickPeriodically(
        topic: String,
        distributedCoroutine: DistributedCoroutine,
        runApproximatelyEvery: Duration,
    ): AutoCloseable {
        val eventLoop =
            Multi.createFrom()
                .ticks()
                .everyJittered(runApproximatelyEvery)
                .invoke { _ ->
                    logger.info(
                        "Starting tick for topic $topic and coroutine ${distributedCoroutine.identifier}"
                    )
                    tick(topic, distributedCoroutine)
                }
                .subscribe()
                .with(
                    {},
                    { e ->
                        logger.error("Event loop for ${distributedCoroutine.identifier} failed", e)
                    },
                )

        return AutoCloseable { eventLoop.cancel() }
    }

    private fun fetchSomePendingCoroutineState(
        connection: Connection,
        distributedCoroutine: DistributedCoroutine,
    ): CoroutineState? {

        val result =
            structuredCooperationManager.fetchPendingCoroutineRun(
                connection,
                distributedCoroutine.identifier.name,
                distributedCoroutine.eventLoopStrategy,
            )

        if (result == null) {
            logger.info(
                "[${(connection as ConnectionWrapper).unwrap(PgConnection::class.java).backendPID}] " +
                    "No messages for coroutine ${distributedCoroutine.identifier}"
            )
            return null
        } else {
            logger.info(
                "[${(connection as ConnectionWrapper).unwrap(PgConnection::class.java).backendPID}] " +
                    "Processing message for coroutine " +
                    "${distributedCoroutine.identifier}: " +
                    "id=${result.messageId}"
            )

            val message =
                Message(
                    id = result.messageId,
                    topic = result.topic,
                    payload = result.payload,
                    createdAt = result.createdAt,
                )

            val childRolledBackExceptions =
                jsonbHelper
                    .fromPGobjectToList<CooperationFailure>(result.childRolledBackExceptions)
                    .map(CooperationFailure::toCooperationException)
            val childRollbackFailedExceptions =
                jsonbHelper
                    .fromPGobjectToList<CooperationFailure>(result.childRollbackFailedExceptions)
                    .map(CooperationFailure::toCooperationException)
            val rollingBackException: CooperationException? =
                result.rollingBackException?.let {
                    CooperationFailure.toCooperationException(
                        jsonbHelper.fromPGobject<CooperationFailure>(it)
                    )
                }

            val latestScopeContext =
                result.latestScopeContext?.let { jsonbHelper.fromPGobject<CooperationContext>(it) }
                    ?: emptyContext()
            val latestContext =
                result.latestContext?.let { jsonbHelper.fromPGobject<CooperationContext>(it) }
                    ?: emptyContext()

            val rollbackState =
                when {
                    childRollbackFailedExceptions.isNotEmpty() ->
                        if (rollingBackException == null) {
                            RollbackState.ChildrenFailedAndFailedToRollBack(
                                childRollbackFailedExceptions,
                                childRolledBackExceptions,
                            )
                        } else {
                            RollbackState.ChildrenFailedWhileRollingBackLastStep(
                                childRollbackFailedExceptions,
                                rollingBackException,
                            )
                        }

                    childRolledBackExceptions.isNotEmpty() ->
                        if (rollingBackException == null) {
                            RollbackState.ChildrenFailedAndSuccessfullyRolledBack(
                                childRolledBackExceptions
                            )
                        } else {
                            RollbackState.SuccessfullyRolledBackLastStep(rollingBackException)
                        }

                    else -> {
                        if (rollingBackException == null) {
                            RollbackState.Gucci
                        } else {
                            RollbackState.SuccessfullyRolledBackLastStep(rollingBackException)
                        }
                    }
                }

            return CoroutineState(
                message,
                if (result.step == null) LastSuspendedStep.NotSuspendedYet
                else LastSuspendedStep.SuspendedAt(result.step),
                CooperationScopeIdentifier.Child(result.cooperationLineage),
                // TODO: this is a little too "silent" - the fact that contexts are
                // combined in this way,
                //  in this order, is pretty important to their semantics. Should
                // either be extracted to
                //  a dedicated function or commented emphatically
                latestScopeContext + latestContext,
                rollbackState,
            )
        }
    }

    private fun resumeCoroutine(
        connection: Connection,
        distributedCoroutine: DistributedCoroutine,
        coroutineState: CoroutineState,
    ): Continuation.ContinuationResult {
        val cooperativeContinuation =
            if (coroutineState.rollbackState is RollbackState.Me.RollingBack) {
                distributedCoroutine.buildRollbackPathContinuation(
                    connection,
                    coroutineState,
                    structuredCooperationManager,
                )
            } else {
                distributedCoroutine.buildHappyPathContinuation(
                    connection,
                    coroutineState,
                    structuredCooperationManager,
                )
            }

        val input =
            when (coroutineState.rollbackState) {
                is RollbackState.Gucci ->
                    Continuation.LastStepResult.SuccessfullyInvoked(coroutineState.message)

                is RollbackState.SuccessfullyRolledBackLastStep ->
                    Continuation.LastStepResult.SuccessfullyRolledBack(
                        coroutineState.message,
                        coroutineState.rollbackState.throwable,
                    )

                is RollbackState.Children.Rollbacks ->
                    Continuation.LastStepResult.Failure(
                        coroutineState.message,
                        coroutineState.rollbackState.throwable,
                    )
            }

        val continuationResult = cooperativeContinuation.resumeWith(input)

        when (continuationResult) {
            is Continuation.ContinuationResult.Success ->
                when (coroutineState.rollbackState) {
                    is RollbackState.Me.NotRollingBack ->
                        markCommited(cooperativeContinuation, coroutineState.message.id)

                    is RollbackState.Me.RollingBack ->
                        markRolledBack(cooperativeContinuation, coroutineState.message.id)
                }

            is Continuation.ContinuationResult.Failure ->
                when (coroutineState.rollbackState) {
                    is RollbackState.Me.NotRollingBack ->
                        markRollingBackInSeparateTransaction(
                            cooperativeContinuation,
                            coroutineState.message.id,
                            continuationResult.exception,
                        )

                    is RollbackState.Me.RollingBack -> {
                        markRollbackFailedInSeparateTransaction(
                            cooperativeContinuation,
                            coroutineState.message.id,
                            continuationResult.exception,
                        )
                    }
                }

            is Continuation.ContinuationResult.Suspend ->
                markSuspended(cooperativeContinuation, coroutineState.message.id)
        }

        logger.info(
            "[${(connection as ConnectionWrapper).unwrap(PgConnection::class.java).backendPID}] " +
                "Finished processing message for continuation " +
                "${cooperativeContinuation.continuationIdentifier}: " +
                "id=${coroutineState.message.id} with $continuationResult"
        )

        return continuationResult
    }

    private fun markCommited(scope: CooperationScope, messageId: UUID) =
        mark(scope, scope.connection, messageId, "COMMITTED")

    private fun markRolledBack(scope: CooperationScope, messageId: UUID) =
        mark(scope, scope.connection, messageId, "ROLLED_BACK")

    private fun markSuspended(scope: CooperationScope, messageId: UUID) =
        mark(scope, scope.connection, messageId, "SUSPENDED")

    private fun markRollingBackInSeparateTransaction(
        scope: CooperationScope,
        messageId: UUID,
        exception: Throwable? = null,
    ) {
        thread {
                fluentJdbc.transactional { connection ->
                    mark(scope, connection, messageId, "ROLLING_BACK", exception)
                }
            }
            .join()
    }

    private fun markRollbackFailedInSeparateTransaction(
        scope: CooperationScope,
        messageId: UUID,
        exception: Throwable? = null,
    ) =
        thread {
                fluentJdbc.transactional { connection ->
                    mark(scope, connection, messageId, "ROLLBACK_FAILED", exception)
                }
            }
            .join()

    private fun mark(
        scope: CooperationScope,
        connection: Connection,
        messageId: UUID,
        messageEventType: String,
        exception: Throwable? = null,
    ) {
        val cooperationFailure =
            exception?.let {
                CooperationFailure.Companion.fromThrowable(
                    it,
                    scope.continuation.continuationIdentifier.distributedCoroutineIdentifier
                        .renderAsString(),
                )
            }

        return messageEventRepository.insertMessageEvent(
            connection,
            messageId,
            messageEventType,
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.name,
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.instance,
            scope.continuation.continuationIdentifier.stepName,
            scope.scopeIdentifier.cooperationLineage,
            cooperationFailure,
            scope.context,
        )
    }
}

data class CoroutineState(
    val message: Message,
    val lastSuspendedStep: LastSuspendedStep,
    val scopeIdentifier: CooperationScopeIdentifier.Child,
    val cooperationContext: CooperationContext,
    val rollbackState: RollbackState,
)
