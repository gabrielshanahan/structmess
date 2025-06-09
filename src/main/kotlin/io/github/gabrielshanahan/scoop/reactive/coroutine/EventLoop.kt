package io.github.gabrielshanahan.scoop.reactive.coroutine

import io.github.gabrielshanahan.scoop.reactive.coroutine.continuation.Continuation
import io.github.gabrielshanahan.scoop.reactive.coroutine.continuation.buildHappyPathContinuation
import io.github.gabrielshanahan.scoop.reactive.coroutine.continuation.buildRollbackPathContinuation
import io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.MessageEventRepository
import io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.StructuredCooperationManager
import io.github.gabrielshanahan.scoop.reactive.flatMapNonNull
import io.github.gabrielshanahan.scoop.reactive.mapNonNull
import io.github.gabrielshanahan.scoop.reactive.messaging.Message
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
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.infrastructure.Infrastructure
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.Pool
import io.vertx.mutiny.sqlclient.SqlClient
import io.vertx.mutiny.sqlclient.SqlConnection
import io.vertx.pgclient.impl.PgConnectionImpl
import jakarta.enterprise.context.ApplicationScoped
import java.time.Duration
import java.util.*
import org.jboss.logging.Logger

@ApplicationScoped
class EventLoop(
    private val pool: Pool,
    private val messageEventRepository: MessageEventRepository,
    private val structuredCooperationManager: StructuredCooperationManager,
) {
    private val logger = Logger.getLogger(javaClass)

    fun tick(
        topic: String,
        distributedCoroutine: DistributedCoroutine,
        recursionCount: Int = 0,
    ): Uni<Unit> =
        pool
            .withConnection { connection ->
                structuredCooperationManager.startContinuationsForCoroutine(
                    connection,
                    distributedCoroutine.identifier.name,
                    distributedCoroutine.identifier.instance,
                    topic,
                    distributedCoroutine.eventLoopStrategy,
                )
            }
            .onFailure()
            .invoke { exc ->
                logger.error("Error in result of startContinuationsForCoroutine", exc)
            }
            .flatMap {
                // The following Uni evaluates to either Unit (if there was something to do,
                // and we did it) or null (if there was nothing to do). Any failures are
                // rethrown to kill the transaction
                pool
                    .withTransaction { connection ->
                        fetchSomePendingCoroutineState(connection, distributedCoroutine)
                            // TODO: move this to coroutine as a parameter! Ideally should be part
                            // of step definition
                            .emitOn(Infrastructure.getDefaultWorkerPool())
                            .flatMapNonNull { coroutineState ->
                                resumeCoroutine(connection, distributedCoroutine, coroutineState)
                                    .onFailure()
                                    // TODO: Remove these when finished
                                    .invoke { exc ->
                                        logger.error(
                                            "[${(connection.delegate as PgConnectionImpl).processId()}] Error in result of resumeCoroutine1",
                                            exc,
                                        )
                                    }
                            }
                            // TODO: Remove these when finished
                            .onFailure()
                            .invoke { exc ->
                                logger.error(
                                    "[${(connection.delegate as PgConnectionImpl).processId()}] Error in result of resumeCoroutine2",
                                    exc,
                                )
                            }
                            .invoke { continuationResult ->
                                if (continuationResult is Continuation.ContinuationResult.Failure) {
                                    throw continuationResult.exception
                                }
                            }
                            // TODO: Remove these when finished
                            .onFailure()
                            .invoke { exc ->
                                logger.error(
                                    "[${(connection.delegate as PgConnectionImpl).processId()}] Error in result of resumeCoroutine3",
                                    exc,
                                )
                            }
                            .mapNonNull { _ -> } // make typechecker happy
                    }
                    // TODO: Remove these when finished
                    .onFailure()
                    .invoke { exc -> logger.error("Error in result of resumeCoroutine4", exc) }
                    .onFailure()
                    .recoverWithItem(Unit)
            }
            // TODO: Remove these when finished
            .onFailure()
            .invoke { exc -> logger.error("Error in result of resumeCoroutine5", exc) }
            .flatMapNonNull {
                // TODO: Remove recursionCount
                logger.info(
                    "Recursing for ${recursionCount + 1} time(s) for topic $topic and coroutine ${distributedCoroutine.identifier}"
                )
                // Run until there's nothing to do. We can do this without fear of stack overflow,
                // because tick() doesn't recurse directly, it just produces a value. While the
                // result is effectively recursion, it's unrolled via Mutiny's event loop. In
                // effect, we're trampolining the code
                // (https://marmelab.com/blog/2018/02/12/understanding-recursion.html#trampoline-optimization).
                // This is similar to what Kotlin's DeepRecursiveFunction does over Kotlin's
                // internal coroutine event loop.
                // https://elizarov.medium.com/deep-recursion-with-coroutines-7c53e15993e3
                tick(topic, distributedCoroutine, recursionCount + 1)
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
                .onItem()
                .transformToUniAndConcatenate {
                    tick(topic, distributedCoroutine)
                        .onFailure()
                        .invoke { e ->
                            logger.error(
                                "Event loop iteration for ${distributedCoroutine.identifier} failed",
                                e,
                            )
                        }
                        .onFailure()
                        .recoverWithItem(Unit)
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
        connection: SqlConnection,
        distributedCoroutine: DistributedCoroutine,
    ): Uni<CoroutineState> =
        structuredCooperationManager
            .fetchPendingCoroutineRun(
                connection,
                distributedCoroutine.identifier.name,
                distributedCoroutine.eventLoopStrategy,
            )
            .flatMapNonNull { result ->
                if (result == null) {
                    logger.info(
                        "[${(connection.delegate as PgConnectionImpl).processId()}] " +
                            "No messages for coroutine ${distributedCoroutine.identifier}"
                    )
                    Uni.createFrom().nullItem()
                } else {
                    logger.info(
                        "[${(connection.delegate as PgConnectionImpl).processId()}] " +
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
                        result.childRolledBackExceptions.map { exceptionJson ->
                            CooperationFailure.Companion.toCooperationException(
                                (exceptionJson as JsonObject).mapTo(CooperationFailure::class.java)
                            )
                        }
                    val childRollbackFailedExceptions =
                        result.childRollbackFailedExceptions.map { exceptionJson ->
                            CooperationFailure.Companion.toCooperationException(
                                (exceptionJson as JsonObject).mapTo(CooperationFailure::class.java)
                            )
                        }
                    val rollingBackException: CooperationException? =
                        result.rollingBackException?.let {
                            CooperationFailure.Companion.toCooperationException(
                                it.mapTo(CooperationFailure::class.java)
                            )
                        }
                    val latestScopeContext =
                        result.latestScopeContext?.mapTo(CooperationContext::class.java)
                            ?: emptyContext()
                    val latestContext =
                        result.latestContext?.mapTo(CooperationContext::class.java)
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
                                    RollbackState.SuccessfullyRolledBackLastStep(
                                        rollingBackException
                                    )
                                }

                            else -> {
                                if (rollingBackException == null) {
                                    RollbackState.Gucci
                                } else {
                                    RollbackState.SuccessfullyRolledBackLastStep(
                                        rollingBackException
                                    )
                                }
                            }
                        }

                    Uni.createFrom()
                        .item(
                            CoroutineState(
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
                        )
                }
            }

    private fun resumeCoroutine(
        connection: SqlConnection,
        distributedCoroutine: DistributedCoroutine,
        coroutineState: CoroutineState,
    ): Uni<Continuation.ContinuationResult> {
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

        return cooperativeContinuation
            .resumeWith(input)
            .flatMap { continuationResult ->
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
                }.replaceWith(continuationResult)
            }
            .invoke { result ->
                logger.info(
                    "[${(connection.delegate as PgConnectionImpl).processId()}] " +
                        "Finished processing message for continuation " +
                        "${cooperativeContinuation.continuationIdentifier}: " +
                        "id=${coroutineState.message.id} with $result"
                )
            }
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
    ) = pool.withConnection { mark(scope, it, messageId, "ROLLING_BACK", exception) }

    private fun markRollbackFailedInSeparateTransaction(
        scope: CooperationScope,
        messageId: UUID,
        exception: Throwable? = null,
    ) = pool.withConnection { mark(scope, it, messageId, "ROLLBACK_FAILED", exception) }

    private fun mark(
        scope: CooperationScope,
        client: SqlClient,
        messageId: UUID,
        messageEventType: String,
        exception: Throwable? = null,
    ): Uni<Unit> {
        val cooperationFailure =
            exception?.let {
                CooperationFailure.Companion.fromThrowable(
                    it,
                    scope.continuation.continuationIdentifier.distributedCoroutineIdentifier
                        .renderAsString(),
                )
            }

        return messageEventRepository.insertMessageEvent(
            client,
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
