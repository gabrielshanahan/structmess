package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.domain.ContinuationResult
import io.github.gabrielshanahan.structmess.domain.CooperationHierarchyStrategy
import io.github.gabrielshanahan.structmess.domain.CooperationScope
import io.github.gabrielshanahan.structmess.domain.CooperationScopeRolledBackException
import io.github.gabrielshanahan.structmess.domain.CooperativeContinuation
import io.github.gabrielshanahan.structmess.domain.Coroutine
import io.github.gabrielshanahan.structmess.domain.CoroutineIdentifier
import io.github.gabrielshanahan.structmess.domain.MessageEventType
import io.github.gabrielshanahan.structmess.domain.Message
import io.github.gabrielshanahan.structmess.domain.StepResult
import io.github.gabrielshanahan.structmess.domain.buildContinuation
import io.github.gabrielshanahan.structmess.domain.whenAllHandlersHaveCompleted
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.groups.MultiTimePeriod
import io.smallrye.mutiny.infrastructure.Infrastructure
import io.vertx.mutiny.sqlclient.Pool
import io.vertx.mutiny.sqlclient.Row
import io.vertx.mutiny.sqlclient.RowSet
import io.vertx.mutiny.sqlclient.SqlClient
import io.vertx.mutiny.sqlclient.SqlConnection
import io.vertx.mutiny.sqlclient.Tuple
import io.vertx.pgclient.pubsub.PgChannel
import io.vertx.pgclient.pubsub.PgSubscriber
import jakarta.enterprise.context.ApplicationScoped
import java.time.Duration
import java.time.ZoneId
import java.util.*
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap

interface HandlerRegistry {
    fun listenersByTopic(): Map<String, List<String>>
}

fun HandlerRegistry.cooperationHierarchyStrategy() = CooperationHierarchyStrategy {
    whenAllHandlersHaveCompleted(listenersByTopic()).sql()
}

@ApplicationScoped
class PostgresMessageQueue(
    private val pool: Pool,
    private val pgSubscriber: PgSubscriber,
    private val objectMapper: ObjectMapper,
) : MessageQueue, HandlerRegistry {

    private val topicsToCoroutines = ConcurrentHashMap.newKeySet<Pair<String, CoroutineIdentifier>>()

    companion object {
        private val logger = Logger.getLogger(PostgresMessageQueue::class.java)
        private const val MESSAGES_TABLE = "messages"
        private const val MESSAGE_EVENTS_TABLE = "message_events"
    }

    override fun fetch(client: SqlClient, messageId: UUID): Uni<Message?> =
        client
            .preparedQuery(
                "SELECT id, topic, payload, created_at FROM $MESSAGES_TABLE WHERE id = $1"
            )
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

    override fun launchAndForget(
        client: SqlClient,
        topic: String,
        payload: JsonNode,
    ): Uni<Message> =
        client
            .preparedQuery(
                "INSERT INTO $MESSAGES_TABLE (topic, payload) VALUES ($1, $2::jsonb) RETURNING id, created_at"
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
                client
                    .preparedQuery(
                        "INSERT INTO $MESSAGE_EVENTS_TABLE (message_id, type) VALUES ($1, $2)"
                    )
                    .execute(Tuple.tuple(listOf(message.id, MessageEventType.EMITTED)))
                    .replaceWith(message)
            }

    override fun launch(
        scope: CooperationScope,
        topic: String,
        payload: JsonNode,
    ): Uni<Message> =
        scope.connection
            .preparedQuery(
                "INSERT INTO $MESSAGES_TABLE (topic, payload) VALUES ($1, $2::jsonb) RETURNING id, created_at"
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
                scope.connection
                    .preparedQuery(
                        "INSERT INTO $MESSAGE_EVENTS_TABLE (message_id, type, handler_name, handler_identifier, step, cooperation_lineage) VALUES ($1, $2, $3, $4, $5, $6)"
                    )
                    .execute(
                        Tuple.tuple(
                            listOf(
                                message.id,
                                MessageEventType.EMITTED,
                                scope.continuationIdentifier.coroutineIdentifier.name,
                                scope.continuationIdentifier.coroutineIdentifier.instance,
                                scope.continuationIdentifier.stepName,
                                scope.cooperationLineage,
                            )
                        )
                    )
                    .replaceWith(message)
            }

    override fun listenersByTopic(): Map<String, List<String>> = topicsToCoroutines
        .map { (topic, identifier) -> topic to identifier.name }
        .distinct()
        .groupBy({ it.first }, { it.second })

    // TODO: Cancellation & yield
    override fun subscribe(topic: String, coroutine: Coroutine): MessageQueue.Subscription {
        val channel: PgChannel = pgSubscriber.channel(topic)
        topicsToCoroutines.add(topic to coroutine.identifier)
        channel.handler { messageId ->
            processMessage(messageId, coroutine)
                .subscribe()
                .with({}, { e -> logger.error("Failed processing message $messageId", e) })
        }

        val eventLoop =
            Multi.createFrom()
                .ticks()
                .everyJittered(Duration.ofSeconds(1))
                .onItem().transformToMultiAndMerge { iterateEventLoop(coroutine) }
                .onItem().transformToUniAndMerge { messageId ->
                    processMessage(messageId, coroutine)
                }
                .subscribe()
                .with({}, { e -> logger.error("Event loop for ${coroutine.identifier} failed", e) })

        return MessageQueue.Subscription {
            topicsToCoroutines.remove(topic to coroutine.identifier)
            eventLoop.cancel()
            channel.handler(null) // Executes UNLISTEN
        }
    }

    private fun processMessage(messageId: String, coroutine: Coroutine): Uni<ContinuationResult> {
        val messageUuid = UUID.fromString(messageId)
        // First, mark the message as SEEN by this handler, if it hasn't already been seen
        return markSeen(messageUuid, coroutine.identifier)
            .replaceWith(
                // Then, start the transaction and lock the processing of the message by this
                // handler instance
                pool.withTransaction { connection ->
                    tryLockSeenIfWaitingToBeProcessed(connection, coroutine, messageUuid)
                        .emitOn(Infrastructure.getDefaultWorkerPool())
                        // Basically flatmap
                        .onItem().ifNotNull().transformToUni { (cooperativeContinuation, input) ->
                            val continuationResult = cooperativeContinuation.resumeWith(input)
                            when (continuationResult) {
                                is ContinuationResult.Commit ->
                                    markCommited(cooperativeContinuation, connection, messageUuid)

                                is ContinuationResult.Rollback ->
                                    markRolledBackInSeparateTransaction(
                                        cooperativeContinuation,
                                        messageUuid,
                                    )

                                is ContinuationResult.Suspend ->
                                    markSuspended(cooperativeContinuation, connection, messageUuid)
                            }.replaceWith(continuationResult)
                        }
                        .invoke { continuationResult ->
                            if (continuationResult is ContinuationResult.Rollback) {
                                throw continuationResult.exception
                            }
                        }
                }
            )
    }

    /** This is done in its own transaction. */
    private fun markSeen(messageId: UUID, coroutineIdentifier: CoroutineIdentifier) =
        pool
            .preparedQuery(
                """
                    WITH emitted_record AS (
                        SELECT cooperation_lineage 
                        FROM $MESSAGE_EVENTS_TABLE 
                        WHERE message_id = $1 AND type = 'EMITTED'
                    )
                    INSERT INTO $MESSAGE_EVENTS_TABLE (
                        message_id, type, 
                        handler_name, handler_identifier, 
                        cooperation_lineage
                    ) 
                    SELECT 
                        $1, 'SEEN',
                        $2, $3,
                        emitted_record.cooperation_lineage
                    FROM emitted_record
                    ON CONFLICT (handler_name, message_id, type) WHERE type = 'SEEN' DO NOTHING
                """
            )
            .execute(
                Tuple.tuple(
                    listOf(messageId, coroutineIdentifier.name, coroutineIdentifier.instance)
                )
            )
            .replaceWithVoid()

    /**
     * Locks the SEEN record if a message is not already being processed by a different instance of
     * this handler, it's not COMMITTED/ROLLED_BACK, and, if there are any SUSPEND records, any
     * messages EMITTED from the last step within the same cooperation scope are completed
     * (i.e. a COMMITED/ROLLED_BACK exists for them) for all handlers.
     *
     * Which handlers to consider is determined by the [coroutine] [CooperationHierarchyStrategy].
     */
    private fun tryLockSeenIfWaitingToBeProcessed(
        connection: SqlConnection,
        coroutine: Coroutine,
        messageId: UUID,
    ): Uni<Pair<CooperativeContinuation, StepResult>> =
        connection
            .preparedQuery(
                """
                    -- Step 1: Lock the SEEN event for the given handler_name + message_id
                    -- only if it hasn't already been committed or rolled back
                    WITH candidate_seen AS (
                        SELECT *
                        FROM message_events
                        WHERE type = 'SEEN'
                          AND handler_name = $1
                          AND message_id = $2
                          AND NOT EXISTS (
                            SELECT
                                1
                            FROM message_events
                            WHERE handler_name = $1
                              AND message_id = $2
                              AND type IN ('COMMITTED', 'ROLLED_BACK')
                        )
                            FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    ),
                    
                    -- Step 2: If it exists, find the most recent SUSPENDED record for this
                    -- handler/message and extract the step name and emitted messages from
                    -- that step
                    latest_suspended AS (
                        SELECT * 
                        FROM message_events
                        WHERE type = 'SUSPENDED'
                          AND handler_name = $1
                          AND message_id = $2
                        ORDER BY created_at DESC
                        LIMIT 1
                    ),
                    
                    -- Step 3: Identify all EMITTED messages from that suspended step, which
                    -- are part of the same cooperation scope
                    ---TODO: Doc that this means that the >emitter< decides if you participate in cooperation or not!
                    emitted_in_step AS (
                        SELECT latest_suspended.*
                        FROM latest_suspended
                        JOIN message_events AS emitted
                             ON emitted.type = 'EMITTED'
                             AND emitted.handler_name = $1
                             AND emitted.step = latest_suspended.step
                             AND emitted.cooperation_lineage = latest_suspended.cooperation_lineage
                    ),
                    
                    -- Step 4: For each EMITTED message, check if:
                    --   * all SEENs have been COMMITTED or ROLLED_BACK
                    --   * any SEENs have been ROLLED_BACK
                    emitted_seen_analysis AS (
                        SELECT
                            COUNT(*) FILTER (
                                WHERE NOT EXISTS (
                                    SELECT 1
                                    FROM message_events AS confirmation
                                    WHERE confirmation.message_id = seen.message_id
                                      AND confirmation.handler_name = seen.handler_name
                                      AND confirmation.type IN ('COMMITTED', 'ROLLED_BACK')
                                )
                            ) AS pending_seen_count,
                    
                            COUNT(*) FILTER (
                                WHERE EXISTS (
                                    SELECT 1
                                    FROM message_events AS confirmation
                                    WHERE confirmation.message_id = seen.message_id
                                      AND confirmation.handler_name = seen.handler_name
                                      AND confirmation.type = 'ROLLED_BACK'
                                )
                            ) AS rolled_back_count
                    
                        FROM emitted_in_step
                        JOIN message_events AS seen
                          ON seen.type = 'SEEN'
                         AND seen.message_id = emitted_in_step.message_id
                        JOIN messages
                          ON messages.id = seen.message_id
                         AND ${coroutine.cooperationHierarchyStrategy.sql()}
                    )
                    
                    -- Step 5: Only return the candidate SEEN if all emitted messages have been finalized
                    SELECT
                        messages.*,
                        candidate_seen.cooperation_lineage,
                        latest_suspended.step,
                        COALESCE(emitted_seen_analysis.rolled_back_count > 0, false) AS rolled_back_present
                    FROM candidate_seen
                    LEFT JOIN latest_suspended ON true
                    LEFT JOIN emitted_seen_analysis ON true
                    LEFT JOIN messages ON candidate_seen.message_id = messages.id
                    WHERE COALESCE(emitted_seen_analysis.pending_seen_count = 0, true)
                    LIMIT 1;
                """
            )
            .execute(Tuple.of(coroutine.identifier.name, messageId))
            .flatMap { rowSet ->
                val row = rowSet.firstOrNull()
                if (row == null) {
                    // Couldn't acquire lock -> we're done
                    Uni.createFrom().nullItem()
                } else {
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
                    val anyRolledBack = row.getBoolean("rolled_back_present")

                    val continuation =
                        coroutine.buildContinuation(connection, step, cooperationLineage.toList())
                    val input =
                        if (!anyRolledBack) StepResult.Success(message)
                        else StepResult.Failure(message, CooperationScopeRolledBackException())

                    Uni.createFrom().item(continuation to input)
                }
            }

    private fun markCommited(scope: CooperationScope, connection: SqlConnection, messageId: UUID) =
        mark(scope, connection, messageId, "COMMITTED")

    private fun markSuspended(scope: CooperationScope, connection: SqlConnection, messageId: UUID) =
        mark(scope, connection, messageId, "SUSPENDED")

    private fun markRolledBackInSeparateTransaction(scope: CooperationScope, messageId: UUID) =
        mark(scope, pool, messageId, "ROLLED_BACK")

    private fun mark(
        scope: CooperationScope,
        client: SqlClient,
        messageId: UUID,
        messageEventType: String,
    ): Uni<RowSet<Row>> =
        client
            .preparedQuery(
                "INSERT INTO $MESSAGE_EVENTS_TABLE (message_id, type, handler_name, handler_identifier, step, cooperation_lineage) VALUES ($1, '$messageEventType', $2, $3, $4, $5)"
            )
            .execute(
                Tuple.tuple(
                    listOf(
                        messageId,
                        scope.continuationIdentifier.coroutineIdentifier.name,
                        scope.continuationIdentifier.coroutineIdentifier.instance,
                        scope.continuationIdentifier.stepName,
                        scope.cooperationLineage.toTypedArray(),
                    )
                )
            )

    /**
     * Works in similar fashion to [tryLockSeenIfWaitingToBeProcessed], except fetches all
     * messageIds that are ready for processing by the [coroutine].
     */
    private fun iterateEventLoop(coroutine: Coroutine): Multi<String> = pool
        .preparedQuery(
            """
                -- Identify messages with a SEEN event for the given handler
                -- that can be considered for further processing
                WITH candidate_seen AS (
                    SELECT me.*
                    FROM message_events me
                    LEFT JOIN message_events me2
                        ON me.message_id = me2.message_id
                        AND me2.handler_name = $1
                        AND me2.type IN ('COMMITTED', 'ROLLED_BACK')
                    WHERE me.type = 'SEEN'
                      AND me.handler_name = $1
                      AND me2.message_id IS NULL
                ),

                -- For each candidate SEEN, find the most recent SUSPENDED step
                latest_suspended AS (
                    SELECT DISTINCT ON (message_id) *
                    FROM message_events
                    WHERE type = 'SUSPENDED'
                      AND handler_name = $1
                    ORDER BY message_id, created_at DESC
                ),

                -- Find all EMITTED messages from the suspended step
                emitted_in_step AS (
                    SELECT latest_suspended.*
                    FROM latest_suspended
                    JOIN message_events AS emitted
                      ON emitted.type = 'EMITTED'
                         AND emitted.handler_name = $1
                         AND emitted.step = latest_suspended.step
                         AND emitted.cooperation_lineage = latest_suspended.cooperation_lineage
                ),

                -- Analyze SEEN confirmations for each EMITTED message
                emitted_seen_analysis AS (
                    SELECT
                        emitted_in_step.message_id,
                        COUNT(*) FILTER (
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM message_events
                                WHERE message_id = seen.message_id
                                  AND handler_name = seen.handler_name
                                  AND type IN ('COMMITTED', 'ROLLED_BACK')
                            )
                        ) AS pending_seen_count,
                        
                        COUNT(*) FILTER (
                            WHERE EXISTS (
                                SELECT 1
                                FROM message_events
                                WHERE message_id = seen.message_id
                                  AND handler_name = seen.handler_name
                                  AND type = 'ROLLED_BACK'
                            )
                        ) AS rolled_back_count
                        
                    FROM emitted_in_step
                    JOIN message_events AS seen
                      ON seen.type = 'SEEN'
                     AND seen.message_id = emitted_in_step.message_id
                    JOIN messages
                     ON messages.id = seen.message_id
                    AND ${coroutine.cooperationHierarchyStrategy.sql()}
                    GROUP BY emitted_in_step.message_id
                ),

                -- Final selection: only include messages where all emitted SEENs are finalized
                final_candidates AS (
                    SELECT candidate_seen.id
                    FROM candidate_seen
                    LEFT JOIN emitted_seen_analysis
                      ON emitted_seen_analysis.message_id = candidate_seen.message_id
                    WHERE COALESCE(emitted_seen_analysis.pending_seen_count = 0, true)
                )
                
                -- Final selection and locking
                SELECT message_id
                FROM message_events
                WHERE id IN (SELECT id FROM final_candidates)
                FOR UPDATE SKIP LOCKED;
            """
        )
        .execute(Tuple.of(coroutine.identifier.name))
        .map { rowSet -> rowSet.map { it.getUUID("message_id").toString() } }
        .onItem().transformToMulti { messageIds ->
            Multi.createFrom().iterable(messageIds)
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
