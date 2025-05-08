package io.github.gabrielshanahan.structmess.coroutine

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.domain.CooperationException
import io.github.gabrielshanahan.structmess.domain.CooperationFailure
import io.github.gabrielshanahan.structmess.domain.Message
import io.github.gabrielshanahan.structmess.everyJittered
import io.github.gabrielshanahan.structmess.flatMapNonNull
import io.github.gabrielshanahan.structmess.mapNonNull
import io.github.gabrielshanahan.structmess.messaging.MessageEventRepository
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.infrastructure.Infrastructure
import io.vertx.core.json.JsonArray
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
    private val objectMapper: ObjectMapper,
    private val messageEventRepository: MessageEventRepository,
) {
    private val logger = Logger.getLogger(javaClass)

    private fun markSeenOrRollingBack(topic: String, coroutine: Coroutine) =
        pool.withConnection { connection ->
            messageEventRepository.markSeenOrRollingBack(
                connection,
                coroutine.identifier.name,
                coroutine.identifier.instance,
                topic,
            )
        }

    fun run(topic: String, coroutine: Coroutine, recursionCount: Int = 0): Uni<Unit> =
        markSeenOrRollingBack(topic, coroutine)
            .flatMap {
                // The following Uni evaluates to either Unit (if there was something to do,
                // and we did it) or null (if there was nothing to do)
                pool
                    .withTransaction { connection ->
                        fetchPendingCoroutineRun(connection, coroutine)
                            // TODO: move this to coroutine as a parameter! Ideally should be part
                            // of step definition
                            .emitOn(Infrastructure.getDefaultWorkerPool())
                            .flatMapNonNull { coroutineState ->
                                resumeCoroutine(connection, coroutine, coroutineState)
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
                                if (continuationResult is Continuation.Result.Failure) {
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
                            .mapNonNull { _ -> }
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
                logger.info(
                    "Recursing for ${recursionCount + 1} time(s) for topic $topic and coroutine ${coroutine.identifier}"
                )
                // Run until there's nothing to do. We can do this without fear of stack overflow,
                // because this just produces a value and doesn't recurse. While the result is
                // effectively recursion, it's unrolled via Mutiny's event loop. In effect, what
                // we're doing is trampolining the code
                // (https://marmelab.com/blog/2018/02/12/understanding-recursion.html#trampoline-optimization).
                // In essence, it's similar to what Kotlin's DeepRecursiveFunction does over
                // Kotlin's coroutine event loop.
                // https://elizarov.medium.com/deep-recursion-with-coroutines-7c53e15993e3
                run(topic, coroutine, recursionCount + 1)
            }

    fun runPeriodically(
        topic: String,
        coroutine: Coroutine,
        runApproximatelyEvery: Duration,
    ): AutoCloseable {
        val eventLoop =
            Multi.createFrom()
                .ticks()
                .everyJittered(runApproximatelyEvery)
                .onItem()
                .transformToUniAndConcatenate {
                    run(topic, coroutine)
                        .onFailure()
                        .invoke { e ->
                            logger.error(
                                "Event loop iteration for ${coroutine.identifier} failed",
                                e,
                            )
                        }
                        .onFailure()
                        .recoverWithItem(Unit)
                }
                .subscribe()
                .with({}, { e -> logger.error("Event loop for ${coroutine.identifier} failed", e) })

        return AutoCloseable { eventLoop.cancel() }
    }

    private fun fetchPendingCoroutineRun(
        connection: SqlConnection,
        coroutine: Coroutine,
    ): Uni<CoroutineState> =
        messageEventRepository
            .fetchPendingCoroutineRun(
                connection,
                coroutine.identifier.name,
                coroutine.cooperationHierarchyStrategy,
            )
            .flatMapNonNull { result ->
                if (result == null) {
                    logger.info(
                        "[${(connection.delegate as PgConnectionImpl).processId()}] No messages for coroutine ${coroutine.identifier}"
                    )
                    Uni.createFrom().nullItem()
                } else {
                    logger.info(
                        "[${(connection.delegate as PgConnectionImpl).processId()}] Processing message for coroutine ${coroutine.identifier}: id=${result.messageId}"
                    )

                    val message =
                        Message(
                            id = result.messageId,
                            topic = result.topic,
                            payload = objectMapper.readTree(result.payload),
                            createdAt = result.createdAt,
                        )

                    val childRolledBackExceptions =
                        result.childRolledBackExceptions.asCooperationExceptions()
                    val childRollbackFailedExceptions =
                        result.childRollbackFailedExceptions.asCooperationExceptions()
                    val rollingBackException: CooperationException? =
                        result.rollingBackException?.asCooperationException()
                    val latestScopeContext =
                        result.latestScopeContext?.let(objectMapper::readCooperationContext)
                            ?: emptyContext()
                    val latestContext =
                        result.latestContext?.let(objectMapper::readCooperationContext)
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
                                result.step,
                                result.cooperationLineage,
                                latestScopeContext + latestContext,
                                rollbackState,
                            )
                        )
                }
            }

    fun String.asCooperationException() =
        CooperationFailure.toCooperationException(
            objectMapper.readValue(this, CooperationFailure::class.java)
        )

    fun JsonArray.asCooperationExceptions() = map { exceptionJson ->
        (exceptionJson as String).asCooperationException()
    }

    private fun resumeCoroutine(
        connection: SqlConnection,
        coroutine: Coroutine,
        coroutineState: CoroutineState,
    ): Uni<Continuation.Result> {

        val (message, stepName, cooperationLineage, cooperationContext, rollbackState) =
            coroutineState
        val cooperativeContinuation =
            coroutine.buildContinuation(
                connection,
                cooperationContext,
                stepName,
                CooperationRoot(cooperationLineage, message),
                rollbackState is RollbackState.Me.RollingBack,
                this::emitRollbacksForEmissions,
                this::fetchCancellationExceptions,
            )

        val input =
            when (rollbackState) {
                is RollbackState.Gucci -> Continuation.LastStepResult.SuccessfullyInvoked(message)
                is RollbackState.SuccessfullyRolledBackLastStep ->
                    Continuation.LastStepResult.SuccessfullyRolledBack(
                        message,
                        rollbackState.throwable,
                    )

                is RollbackState.Children.Rollbacks ->
                    Continuation.LastStepResult.Failure(message, rollbackState.throwable)
            }
        return cooperativeContinuation
            .resumeWith(input)
            .flatMap { continuationResult ->
                when (continuationResult) {
                    is Continuation.Result.Success ->
                        when (rollbackState) {
                            is RollbackState.Me.NotRollingBack ->
                                markCommited(cooperativeContinuation, connection, message.id)

                            is RollbackState.Me.RollingBack ->
                                markRolledBack(cooperativeContinuation, connection, message.id)
                        }

                    is Continuation.Result.Failure ->
                        when (rollbackState) {
                            is RollbackState.Me.NotRollingBack ->
                                markRollingBackInSeparateTransaction(
                                    cooperativeContinuation,
                                    message.id,
                                    continuationResult.exception,
                                )

                            is RollbackState.Me.RollingBack -> {
                                markRollbackFailedInSeparateTransaction(
                                    cooperativeContinuation,
                                    message.id,
                                    continuationResult.exception,
                                )
                            }
                        }

                    is Continuation.Result.Suspend ->
                        markSuspended(cooperativeContinuation, connection, message.id)
                }.replaceWith(continuationResult)
            }
            .invoke { result ->
                logger.info(
                    "[${(connection.delegate as PgConnectionImpl).processId()}] Finished processing message for continuation ${cooperativeContinuation.identifier}: id=${coroutineState.message.id} with $result"
                )
            }
    }

    private fun emitRollbacksForEmissions(
        scope: CooperationScope,
        stepName: String,
        throwable: Throwable,
    ): Uni<Unit> {
        val cooperationFailure =
            CooperationFailure.fromThrowable(
                ParentSaidSoException(throwable),
                scope.identifier.coroutineIdentifier.renderAsString(),
            )

        val throwableJson = objectMapper.writeValueAsString(cooperationFailure)

        return messageEventRepository.insertRollbackEmittedEventsForStep(
            scope.connection,
            stepName,
            scope.cooperationRoot.cooperationLineage,
            scope.identifier.coroutineIdentifier.name,
            scope.identifier.coroutineIdentifier.instance,
            scope.identifier.stepName,
            throwableJson,
            scope.context,
        )
    }

    private fun fetchCancellationExceptions(
        scope: CooperationScope
    ): Uni<List<CooperationException>> =
        messageEventRepository
            .fetchCancellationExceptions(scope.connection, scope.cooperationRoot.cooperationLineage)
            .map {
                it.exceptionJsons.map { exceptionJson ->
                    CooperationFailure.toCooperationException(
                        objectMapper.readValue(exceptionJson, CooperationFailure::class.java)
                    )
                }
            }

    private fun markCommited(scope: CooperationScope, connection: SqlConnection, messageId: UUID) =
        mark(scope, connection, messageId, "COMMITTED")

    private fun markRolledBack(
        scope: CooperationScope,
        connection: SqlConnection,
        messageId: UUID,
    ) = mark(scope, connection, messageId, "ROLLED_BACK")

    private fun markSuspended(scope: CooperationScope, connection: SqlConnection, messageId: UUID) =
        mark(scope, connection, messageId, "SUSPENDED")

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
        val exceptionJson =
            exception?.let {
                val cooperationFailure =
                    CooperationFailure.fromThrowable(
                        it,
                        scope.identifier.coroutineIdentifier.renderAsString(),
                    )
                objectMapper.writeValueAsString(cooperationFailure)
            }

        return messageEventRepository.insertMessageEvent(
            client,
            messageId,
            messageEventType,
            scope.identifier.coroutineIdentifier.name,
            scope.identifier.coroutineIdentifier.instance,
            scope.identifier.stepName,
            scope.cooperationRoot.cooperationLineage,
            exceptionJson,
            scope.context,
        )
    }
}

data class CoroutineState(
    val message: Message,
    val stepName: String?,
    val cooperationLineage: List<UUID>,
    val cooperationContext: CooperationContext,
    val rollbackState: RollbackState,
)

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
        Me.RollingBack, Children.NoRollbacks

    data class ChildrenFailedWhileRollingBackLastStep(
        override val throwable: ChildRollbackFailedException
    ) : Me.RollingBack, Children.Rollbacks.Failed {
        constructor(
            rollbackFailures: List<CooperationException>,
            originalRollbackCause: CooperationException,
        ) : this(
            ChildRollbackFailedException(
                // Prevent exceptions pointlessly multiplying ad absurdum
                rollbackFailures +
                    listOfNotNull(
                        originalRollbackCause.takeIf { !rollbackFailures.containsRecursively(it) }
                    )
            )
        )

        companion object {
            private fun List<CooperationException>.containsRecursively(
                exception: CooperationException
            ): Boolean = any { it == exception || it.causes.containsRecursively(exception) }
        }
    }

    class ChildrenFailedAndSuccessfullyRolledBack(
        override val throwable: ChildRolledBackException
    ) : Me.NotRollingBack, Children.Rollbacks.Successful {

        constructor(
            childrenFailures: List<CooperationException>
        ) : this(ChildRolledBackException(childrenFailures))
    }

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
