package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.f4b6a3.uuid.UuidCreator
import io.github.gabrielshanahan.structmess.domain.CancellationRequestedException
import io.github.gabrielshanahan.structmess.domain.ChildRollbackFailedException
import io.github.gabrielshanahan.structmess.domain.ChildRolledBackException
import io.github.gabrielshanahan.structmess.domain.ContinuationResult
import io.github.gabrielshanahan.structmess.domain.CooperationContext
import io.github.gabrielshanahan.structmess.domain.CooperationException
import io.github.gabrielshanahan.structmess.domain.CooperationFailure
import io.github.gabrielshanahan.structmess.domain.CooperationHierarchyStrategy
import io.github.gabrielshanahan.structmess.domain.CooperationRoot
import io.github.gabrielshanahan.structmess.domain.CooperationScope
import io.github.gabrielshanahan.structmess.domain.Coroutine
import io.github.gabrielshanahan.structmess.domain.CoroutineIdentifier
import io.github.gabrielshanahan.structmess.domain.LastStepResult.*
import io.github.gabrielshanahan.structmess.domain.Message
import io.github.gabrielshanahan.structmess.domain.ParentSaidSoException
import io.github.gabrielshanahan.structmess.domain.RollbackRequestedException
import io.github.gabrielshanahan.structmess.domain.buildContinuation
import io.github.gabrielshanahan.structmess.domain.emptyContext
import io.github.gabrielshanahan.structmess.domain.readCooperationContext
import io.github.gabrielshanahan.structmess.domain.renderAsString
import io.github.gabrielshanahan.structmess.domain.whenAllHandlersHaveCompleted
import io.github.gabrielshanahan.structmess.domain.writeCooperationContext
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.groups.MultiTimePeriod
import io.smallrye.mutiny.infrastructure.Infrastructure
import io.vertx.core.json.JsonArray
import io.vertx.mutiny.sqlclient.Pool
import io.vertx.mutiny.sqlclient.Row
import io.vertx.mutiny.sqlclient.RowSet
import io.vertx.mutiny.sqlclient.SqlClient
import io.vertx.mutiny.sqlclient.SqlConnection
import io.vertx.mutiny.sqlclient.Tuple
import io.vertx.pgclient.impl.PgConnectionImpl
import io.vertx.pgclient.pubsub.PgChannel
import io.vertx.pgclient.pubsub.PgSubscriber
import jakarta.enterprise.context.ApplicationScoped
import java.time.Duration
import java.time.ZoneId
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import org.jboss.logging.Logger

interface HandlerRegistry {
    fun listenersByTopic(): Map<String, List<String>>
}

fun HandlerRegistry.cooperationHierarchyStrategy() =
    CooperationHierarchyStrategy { emissionsAlias, seenAlias ->
        whenAllHandlersHaveCompleted(listenersByTopic())
            .anyMissingSeenSql(emissionsAlias, seenAlias)
    }

@ApplicationScoped
class PostgresMessageQueue(
    private val pool: Pool,
    private val pgSubscriber: PgSubscriber,
    private val objectMapper: ObjectMapper,
) : MessageQueue, HandlerRegistry {

    private val topicsToCoroutines =
        ConcurrentHashMap.newKeySet<Pair<String, CoroutineIdentifier>>()

    companion object {
        private val logger = Logger.getLogger(PostgresMessageQueue::class.java)
    }

    override fun fetch(connection: SqlConnection, messageId: UUID): Uni<Message?> =
        connection
            .preparedQuery("SELECT id, topic, payload, created_at FROM message WHERE id = $1")
            .execute(Tuple.of(messageId))
            .map { rowSet ->
                rowSet
                    .firstOrNull()
                    ?.let { row ->
                        Message(
                            id = row.getUUID("id"),
                            topic = row.getString("topic"),
                            payload = objectMapper.readTree(row.getString("payload")),
                            createdAt =
                                row.getLocalDateTime("created_at").atZone(ZoneId.systemDefault()),
                        )
                    }
                    ?.also { logger.info("Fetched message: id=$it.id, topic=$it.topic") }
            }

    override fun launchOnGlobalScope(
        connection: SqlConnection,
        topic: String,
        payload: JsonNode,
        context: CooperationContext?,
    ): Uni<CooperationRoot> =
        connection
            .preparedQuery(
                "INSERT INTO message (topic, payload) VALUES ($1, $2::jsonb) RETURNING id, created_at"
            )
            .execute(Tuple.of(topic, payload.toString()))
            .map { rowSet ->
                val row =
                    checkNotNull(rowSet.firstOrNull()) {
                        "Unable to publish message on topic $topic"
                    }
                Message(
                        id = row.getUUID("id"),
                        topic = topic,
                        payload = payload,
                        createdAt =
                            row.getLocalDateTime("created_at").atZone(ZoneId.systemDefault()),
                    )
                    .also { logger.info("Published message: id='${it.id}', topic='${it.topic}'") }
            }
            .flatMap { message ->
                val cooperationId = UuidCreator.getTimeOrderedEpoch()
                connection
                    .preparedQuery(
                        "INSERT INTO message_event (message_id, type, cooperation_lineage, context) VALUES ($1, 'EMITTED', $2, $3::jsonb)"
                    )
                    .execute(
                        Tuple.of(
                            message.id,
                            listOf(cooperationId).toTypedArray(),
                            context?.let(objectMapper::writeCooperationContext)?.takeIf {
                                it != "{}"
                            },
                        )
                    )
                    .replaceWith(CooperationRoot(listOf(cooperationId), message))
            }

    override fun launch(
        scope: CooperationScope,
        topic: String,
        payload: JsonNode,
        additionalContext: CooperationContext?,
    ): Uni<Message> =
        scope.connection
            .preparedQuery(
                "INSERT INTO message (topic, payload) VALUES ($1, $2::jsonb) RETURNING id, created_at"
            )
            .execute(Tuple.of(topic, payload.toString()))
            .map { rowSet ->
                val row =
                    checkNotNull(rowSet.firstOrNull()) {
                        "Unable to publish message on topic $topic"
                    }
                Message(
                        id = row.getUUID("id"),
                        topic = topic,
                        payload = payload,
                        createdAt =
                            row.getLocalDateTime("created_at").atZone(ZoneId.systemDefault()),
                    )
                    .also { logger.info("Published message: id='${it.id}', topic='${it.topic}'") }
            }
            .flatMap { message ->
                val context =
                    if (additionalContext != null) {
                        scope.context + additionalContext
                    } else {
                        scope.context
                    }
                scope.connection
                    .preparedQuery(
                        "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, context) VALUES ($1, 'EMITTED', $2, $3, $4, $5, $6::jsonb)"
                    )
                    .execute(
                        Tuple.of(
                            message.id,
                            scope.identifier.coroutineIdentifier.name,
                            scope.identifier.coroutineIdentifier.instance,
                            scope.identifier.stepName,
                            scope.cooperationRoot.cooperationLineage.toTypedArray(),
                            objectMapper.writeCooperationContext(context).takeIf { it != "{}" },
                        )
                    )
                    .invoke { _ -> scope.emitted(message) }
                    .replaceWith(message)
            }

    override fun cancel(
        connection: SqlConnection,
        cooperationRoot: CooperationRoot,
        source: String,
        reason: String,
    ): Uni<Unit> {
        val exception = CancellationRequestedException(reason)
        return markCancellationRequested(
                connection,
                null,
                cooperationRoot.cooperationLineage,
                cooperationRoot.message.id,
                CooperationFailure.fromThrowable(exception, source),
            )
            .replaceWith(Unit)
    }

    override fun cancel(scope: CooperationScope, reason: String): Uni<Unit> {
        val exception = CancellationRequestedException(reason)
        return markCancellationRequested(
                scope.connection,
                scope,
                scope.cooperationRoot.cooperationLineage,
                scope.cooperationRoot.message.id,
                CooperationFailure.fromThrowable(
                    exception,
                    scope.identifier.coroutineIdentifier.renderAsString(),
                ),
            )
            .replaceWith(Unit)
            .invoke { _ -> throw exception }
    }

    override fun rollback(
        connection: SqlConnection,
        cooperationRoot: CooperationRoot,
        source: String,
        reason: String,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                """
                    INSERT INTO message_event (
                        message_id, 
                        type,  
                        cooperation_lineage, 
                        exception
                    )
                    SELECT 
                        $1, 'ROLLBACK_EMITTED', $2, $3::jsonb
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM message_event seen
                        LEFT JOIN message_event committed
                          ON committed.cooperation_lineage = seen.cooperation_lineage
                             AND committed.type = 'COMMITTED'
                        WHERE seen.type = 'SEEN'
                          AND seen.cooperation_lineage <@ $2
                          AND committed.cooperation_lineage IS NULL
                          AND cardinality(seen.cooperation_lineage) > 1
                    )
                    ON CONFLICT (message_id, type) WHERE type = 'ROLLBACK_EMITTED' DO NOTHING;
                """
                    .trimIndent()
            )
            .execute(
                Tuple.tuple(
                    listOf(
                        cooperationRoot.message.id,
                        cooperationRoot.cooperationLineage.toTypedArray<UUID>(),
                        objectMapper.writeValueAsString(
                            CooperationFailure.fromThrowable(
                                RollbackRequestedException(reason),
                                source,
                            )
                        ),
                    )
                )
            )
            .replaceWith(Unit)

    override fun listenersByTopic(): Map<String, List<String>> =
        topicsToCoroutines
            .map { (topic, identifier) -> topic to identifier.name }
            .distinct()
            .groupBy({ it.first }, { it.second })

    override fun subscribe(topic: String, coroutine: Coroutine): MessageQueue.Subscription {
        val channel: PgChannel = pgSubscriber.channel(topic)
        topicsToCoroutines.add(topic to coroutine.identifier)
        channel.handler {
            iterateEventLoop(topic, coroutine)
                .subscribe()
                .with(
                    {},
                    { e ->
                        logger.error(
                            "Event loop for ${coroutine.identifier} failed when triggered from LISTEN handler",
                            e,
                        )
                    },
                )
        }

        val eventLoop =
            Multi.createFrom()
                .ticks()
                .everyJittered(Duration.ofMillis(50)) // TODO: Constant/configuration
                .onItem()
                .transformToUniAndConcatenate {
                    iterateEventLoop(topic, coroutine)
                        .replaceWith(Unit)
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

        return MessageQueue.Subscription {
            topicsToCoroutines.remove(topic to coroutine.identifier)
            eventLoop.cancel()
            channel.handler(null) // Executes UNLISTEN
        }
    }

    private fun iterateEventLoop(
        topic: String,
        coroutine: Coroutine,
        recursionCount: Int = 0,
    ): Uni<Unit> =
        markSeenOrRollingBack(topic, coroutine)
            .flatMap {
                // The following Uni evaluates to either Unit (if there was something to do,
                // and we did it) or null (if there was nothing to do)
                pool
                    .withTransaction { connection ->
                        fetchCoroutineForProcessing(connection, coroutine)
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
                                if (continuationResult is ContinuationResult.Failure) {
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
                // effectively recursion, it's run via Mutiny's event loop. In effect, what
                // we're doing is trampolining the code
                // (https://marmelab.com/blog/2018/02/12/understanding-recursion.html#trampoline-optimization)
                iterateEventLoop(topic, coroutine, recursionCount + 1)
            }

    private fun markSeenOrRollingBack(topic: String, coroutine: Coroutine) =
        pool
            .executePreparedQuery(
                """
                WITH
                -- Find EMITTED records missing a SEEN
                emitted_missing_seen AS (
                    SELECT message_event.message_id, message_event.cooperation_lineage, message_event.context 
                    FROM message_event
                    LEFT JOIN message_event AS seen_check
                        ON seen_check.message_id = message_event.message_id AND seen_check.type = 'SEEN' AND seen_check.coroutine_name = $1
                    JOIN message
                        ON message.id = message_event.message_id AND message.topic = $3
                    WHERE message_event.type = 'EMITTED' AND seen_check.id IS NULL
                ),
                -- Find ROLLBACK_EMITTED records missing a ROLLING_BACK
                rollback_emitted_missing_rolling_back AS (
                    SELECT message_event.message_id, seen.cooperation_lineage, message_event.exception, message_event.context
                    FROM message_event
                    LEFT JOIN message_event AS rolling_back_check
                        ON rolling_back_check.message_id = message_event.message_id AND rolling_back_check.type = 'ROLLING_BACK' AND rolling_back_check.coroutine_name = $1
                    JOIN message
                        ON message.id = message_event.message_id AND message.topic = $3
                    JOIN message_event AS seen
                         ON seen.message_id = message_event.message_id AND seen.type = 'SEEN' AND seen.coroutine_name = $1
                    WHERE message_event.type = 'ROLLBACK_EMITTED' AND rolling_back_check.id IS NULL
                ),
                -- Insert SEEN if EMITTED exists without SEEN
                seen_insert AS (
                    INSERT INTO message_event (
                        message_id, type, 
                        coroutine_name, coroutine_identifier, 
                        cooperation_lineage,
                        context
                    )
                    SELECT 
                        emitted_missing_seen.message_id, 
                        'SEEN', 
                        $1,
                        $2,
                        emitted_missing_seen.cooperation_lineage || gen_uuid_v7(),
                        emitted_missing_seen.context
                    FROM emitted_missing_seen
                    ON CONFLICT (coroutine_name, message_id, type) WHERE type = 'SEEN' DO NOTHING
                    RETURNING id
                ),
                -- Insert ROLLING_BACK if ROLLBACK_EMITTED exists without ROLLING_BACK
                rolling_back_insert AS (
                    INSERT INTO message_event (
                        message_id, type, 
                        coroutine_name, coroutine_identifier, 
                        cooperation_lineage, 
                        exception,
                        context
                    )
                    SELECT 
                        rollback_emitted_missing_rolling_back.message_id, 
                        'ROLLING_BACK', 
                        $1,
                        $2,
                        rollback_emitted_missing_rolling_back.cooperation_lineage,
                        rollback_emitted_missing_rolling_back.exception,
                        rollback_emitted_missing_rolling_back.context
                    FROM rollback_emitted_missing_rolling_back
                    ON CONFLICT (coroutine_name, message_id, type) WHERE type = 'ROLLING_BACK' DO NOTHING
                    RETURNING id
                )
                -- Just here to execute the CTEs
                SELECT 1;
            """,
                Tuple.tuple(listOf(coroutine.identifier.name, coroutine.identifier.instance, topic)),
            )
            .replaceWith(Unit)

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

        data object Gucci : Me.NotRollingBack, Children.NoRollbacks

        data class SuccessfullyRolledBackLastStep(override val throwable: CooperationException) :
            Me.RollingBack, Children.Rollbacks.Successful, Children.NoRollbacks

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
                            originalRollbackCause.takeIf {
                                !rollbackFailures.containsRecursively(it)
                            }
                        )
                )
            )

            companion object {
                private fun List<CooperationException>.containsRecursively(
                    exception: CooperationException
                ): Boolean = any { it == exception || it.causes.containsRecursively(exception) }
            }
        }

        data class ChildrenFailedAndSuccessfullyRolledBack(
            override val throwable: ChildRolledBackException
        ) : Me.NotRollingBack, Children.Rollbacks.Successful {
            constructor(
                childrenFailures: List<CooperationException>
            ) : this(ChildRolledBackException(childrenFailures))
        }

        data class ChildrenFailedAndFailedToRollBack(
            override val throwable: ChildRollbackFailedException
        ) : Me.NotRollingBack, Children.Rollbacks.Failed {
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

    private fun fetchCoroutineForProcessing(
        connection: SqlConnection,
        coroutine: Coroutine,
    ): Uni<CoroutineState> =
        connection
            .preparedQuery(finalSelect(coroutine.cooperationHierarchyStrategy).build())
            .execute(Tuple.of(coroutine.identifier.name))
            .flatMap { rowSet ->
                val row = rowSet.firstOrNull()
                if (row == null) {
                    logger.info(
                        "[${(connection.delegate as PgConnectionImpl).processId()}] No messages for coroutine ${coroutine.identifier}"
                    )
                    // Nothing to do -> we're done
                    Uni.createFrom().nullItem()
                } else {
                    // This is, in essence, the equivalent of double-checked locking
                    // (https://en.wikipedia.org/wiki/Double-checked_locking).
                    //
                    // The query in finalSelect() essentially works in two steps:
                    //    1. Evaluate which SEEN records are waiting to be processed
                    //    2. Pick one and lock it, skipping locked ones.
                    //
                    // An important refresher: FOR UPDATE SKIP LOCKED uses the state of locks
                    // at the time it's executed, not at the beginning of the transaction.
                    //
                    // A race condition can arise in the following way. During the evaluation of
                    // step 1, some other handler can already be processing a SEEN, but not have
                    // committed yet, so we still get "ready to be processed" for that SEEN. But
                    // before we get to step 2, where the lock would prevent us from picking it
                    // up, the handler commits and releases the lock. This causes us to pick up
                    // the SEEN, but still retain and use the old data we fetched earlier.
                    // Therefore, we proceed by rerunning the step which just ran.
                    //
                    // Postgres attempts to mitigate this to a certain extent by checking that the
                    // row being locked has not been modified during the time the query is run and
                    // refetches the record if that happens. Some solutions to this problem revolve
                    // around this, recommending you always modify the row being locked. However,
                    // Postgres only refetches that specific row and doesn't evaluate the rest of
                    // the
                    // conditions. This makes it unusable for our purposes, since, e.g., the latest
                    // SUSPEND record is one of the key things we use and need to refetch.
                    //
                    // There are various other approaches that could be taken, commonly referred to
                    // as
                    // "materializing the conflict", but the simplest solution for a POC application
                    // such as this is to just run the query again after we've acquired the lock,
                    // which is exactly how double-checked locking is implemented.
                    //
                    // For a different instance of this behavior, take a look at, e.g.,
                    // https://postgrespro.com/list/thread-id/2470837
                    connection
                        .preparedQuery(
                            finalSelect(coroutine.cooperationHierarchyStrategy, true).build()
                        )
                        .execute(Tuple.of(coroutine.identifier.name, row.getUUID("id")))
                }
            }
            .flatMapNonNull { rowSet ->
                val row = rowSet.firstOrNull()
                if (row == null) {
                    logger.info(
                        "[${(connection.delegate as PgConnectionImpl).processId()}] No messages for coroutine ${coroutine.identifier}"
                    )
                    // After the second fetch, we found out that the record is no longer ready for
                    // processing (i.e., the race condition described above happened)
                    Uni.createFrom().nullItem()
                } else {
                    logger.info(
                        "[${(connection.delegate as PgConnectionImpl).processId()}] Processing message for coroutine ${coroutine.identifier}: id=${row.getUUID("id")}"
                    )
                    val message =
                        Message(
                            id = row.getUUID("id"),
                            topic = row.getString("topic"),
                            payload = objectMapper.readTree(row.getString("payload")),
                            createdAt =
                                row.getLocalDateTime("created_at").atZone(ZoneId.systemDefault()),
                        )

                    val cooperationLineage = row.getArrayOfUUIDs("cooperation_lineage")
                    val step: String? = row.getString("step")
                    val childRolledBackExceptions =
                        row.getJsonArray("child_rolled_back_exceptions").asCooperationExceptions()
                    val childRollbackFailedExceptions =
                        row.getJsonArray("child_rollback_failed_exceptions")
                            .asCooperationExceptions()
                    val rollingBackException: CooperationException? =
                        row.getString("rolling_back_exception")?.asCooperationException()
                    val latestScopeContext =
                        row.getString("latest_scope_context")
                            ?.let(objectMapper::readCooperationContext) ?: emptyContext()
                    val latestContext =
                        row.getString("latest_context")?.let(objectMapper::readCooperationContext)
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
                                step,
                                cooperationLineage.toList(),
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
    ): Uni<ContinuationResult> {

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
                is RollbackState.Gucci -> SuccessfullyInvoked(message)
                is RollbackState.SuccessfullyRolledBackLastStep ->
                    SuccessfullyRolledBack(message, rollbackState.throwable)
                is RollbackState.Children.Rollbacks -> Failure(message, rollbackState.throwable)
            }
        return cooperativeContinuation
            .resumeWith(input)
            .flatMap { continuationResult ->
                when (continuationResult) {
                    is ContinuationResult.Success ->
                        when (rollbackState) {
                            is RollbackState.Me.NotRollingBack ->
                                markCommited(cooperativeContinuation, connection, message.id)
                            is RollbackState.Me.RollingBack ->
                                markRolledBack(cooperativeContinuation, connection, message.id)
                        }

                    is ContinuationResult.Failure ->
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

                    is ContinuationResult.Suspend ->
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

        return scope.connection
            .preparedQuery(
                """
                    WITH emitted_record AS (
                        SELECT cooperation_lineage, message_id
                        FROM message_event
                        WHERE type = 'EMITTED'
                            AND step = $1
                            AND cooperation_lineage = $2
                    )
                    INSERT INTO message_event (
                        message_id, type, 
                        cooperation_lineage, coroutine_name, coroutine_identifier, step,
                        exception, context
                    )
                    SELECT 
                        message_id, 'ROLLBACK_EMITTED', 
                        $2, $3, $4, $5, $6::jsonb, $7::jsonb
                    FROM emitted_record;
                """
            )
            .execute(
                Tuple.tuple(
                    listOf(
                        stepName,
                        scope.cooperationRoot.cooperationLineage.toTypedArray(),
                        scope.identifier.coroutineIdentifier.name,
                        scope.identifier.coroutineIdentifier.instance,
                        scope.identifier.stepName,
                        throwableJson,
                        scope.context.let(objectMapper::writeCooperationContext).takeIf {
                            it != "{}"
                        },
                    )
                )
            )
            .replaceWith(Uni.createFrom().item(Unit))
    }

    private fun fetchCancellationExceptions(
        scope: CooperationScope
    ): Uni<List<CooperationException>> =
        scope.connection
            .preparedQuery(
                "SELECT exception FROM message_event WHERE cooperation_lineage <@ $1 AND type = 'CANCELLATION_REQUESTED'"
            )
            .execute(Tuple.of(scope.cooperationRoot.cooperationLineage.toTypedArray()))
            .map { rowSet -> rowSet.map { it.getString("exception").asCooperationException() } }

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

    private fun markCancellationRequested(
        connection: SqlConnection,
        sourceScope: CooperationScope?,
        targetCooperationLineage: List<UUID>,
        messageId: UUID,
        exception: CooperationFailure,
    ): Uni<RowSet<Row>> =
        connection
            .preparedQuery(
                """
                    INSERT INTO message_event (
                        message_id, 
                        type, 
                        coroutine_name, 
                        coroutine_identifier, 
                        step, 
                        cooperation_lineage, 
                        exception) 
                    VALUES ($1, 'CANCELLATION_REQUESTED', $2, $3, $4, $5, $6::jsonb)
                    ON CONFLICT (cooperation_lineage, type) WHERE type = 'CANCELLATION_REQUESTED' DO NOTHING
                """
                    .trimIndent()
            )
            .execute(
                Tuple.tuple(
                    listOf(
                        messageId,
                        sourceScope?.identifier?.coroutineIdentifier?.name,
                        sourceScope?.identifier?.coroutineIdentifier?.instance,
                        sourceScope?.identifier?.stepName,
                        targetCooperationLineage.toTypedArray(),
                        objectMapper.writeValueAsString(exception),
                    )
                )
            )

    private fun mark(
        scope: CooperationScope,
        client: SqlClient,
        messageId: UUID,
        messageEventType: String,
        exception: Throwable? = null,
    ): Uni<RowSet<Row>> {
        val exceptionJson =
            exception?.let {
                val cooperationFailure =
                    CooperationFailure.fromThrowable(
                        it,
                        scope.identifier.coroutineIdentifier.renderAsString(),
                    )
                objectMapper.writeValueAsString(cooperationFailure)
            }

        return client
            .preparedQuery(
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, exception, context) VALUES ($1, '$messageEventType', $2, $3, $4, $5, $6::jsonb, $7::jsonb)"
            )
            .execute(
                Tuple.tuple(
                    listOf(
                        messageId,
                        scope.identifier.coroutineIdentifier.name,
                        scope.identifier.coroutineIdentifier.instance,
                        scope.identifier.stepName,
                        scope.cooperationRoot.cooperationLineage.toTypedArray(),
                        exceptionJson,
                        scope.context.let(objectMapper::writeCooperationContext).takeIf {
                            it != "{}"
                        },
                    )
                )
            )
    }
}

private fun MultiTimePeriod.everyJittered(duration: Duration): Multi<Long> =
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

private fun <T, R> Uni<T>.flatMapNonNull(mapper: (T) -> Uni<R>): Uni<R> = flatMap {
    if (it == null) {
        Uni.createFrom().nullItem()
    } else {
        mapper(it)
    }
}

private fun <T, R> Uni<T>.mapNonNull(mapper: (T) -> R): Uni<R> = map { it?.let(mapper) }
