package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.domain.MessageEventType
import io.github.gabrielshanahan.structmess.domain.coroutine
import io.quarkus.test.junit.QuarkusTest
import io.vertx.mutiny.sqlclient.Pool
import io.vertx.mutiny.sqlclient.Tuple
import jakarta.inject.Inject
import java.util.UUID
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresMessageQueueMessageEventsTest {

    @Inject lateinit var messageQueue: MessageQueue

    @Inject lateinit var handlerRegistry: HandlerRegistry

    @Inject lateinit var objectMapper: ObjectMapper

    @Inject lateinit var pool: Pool

    private val testTopic = "test-events-topic"

    /** Cleans up the database after test execution */
    @AfterEach
    fun cleanupDatabase() {
        pool
            .preparedQuery("TRUNCATE TABLE message_events, messages CASCADE")
            .execute()
            .await()
            .indefinitely()
    }

    /** Helper function to count message events by message ID and type */
    private fun countMessageEvents(messageId: UUID, type: MessageEventType): Int {
        return pool
            .preparedQuery("SELECT COUNT(*) FROM message_events WHERE message_id = $1 AND type = $2")
            .execute(Tuple.of(messageId, type))
            .await()
            .indefinitely()
            .first()
            .getInteger(0)
    }

    /** Helper function to count message events by handler, message ID and type */
    private fun countMessageEvents(handlerName: String, messageId: UUID, type: MessageEventType): Int {
        return pool
            .preparedQuery(
                "SELECT COUNT(*) FROM message_events WHERE handler_name = $1 AND message_id = $2 AND type = $3"
            )
            .execute(Tuple.of(handlerName, messageId, type))
            .await()
            .indefinitely()
            .first()
            .getInteger(0)
    }

    @Test
    fun `should write EMITTED event when message is published`() {
        // Create a test payload
        val payload = objectMapper.createObjectNode().put("text", "Testing EMITTED event")

        // Publish a message
        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchAndForget(connection, testTopic, payload)
                }
                .await()
                .indefinitely()

        // Verify an EMITTED event was created
        val emittedCount = countMessageEvents(message.id, MessageEventType.EMITTED)
        assertEquals(1, emittedCount, "There should be exactly one EMITTED event message")

        // Verify the handler details were recorded correctly
        val handlerRecorded =
            pool
                .preparedQuery(
                    "SELECT type FROM message_events WHERE message_id = $1"
                )
                .execute(Tuple.of(message.id))
                .await()
                .indefinitely()
                .single()

        assertEquals(
            MessageEventType.EMITTED.toString(),
            handlerRecorded.getString("type"),
        )
    }

    @Test
    fun `should write one SEEN event per handler`() {
        // Create a test payload and handlers
        val payload = objectMapper.createObjectNode().put("text", "Testing SEEN event")
        val handlerName1 = "test-handler-1"
        val handlerName2 = "test-handler-2"
        val messageIdRef = AtomicReference<UUID>()
        val latch = CountDownLatch(2)

        // Subscribe with first handler
        val subscription1 =
            messageQueue.subscribe(testTopic, coroutine(handlerName1, handlerRegistry.cooperationHierarchyStrategy()) {
                step { scope, message ->
                    latch.countDown()
                }
            })

        // Subscribe with second handler
        val subscription2 =
            messageQueue.subscribe(testTopic, coroutine(handlerName2, handlerRegistry.cooperationHierarchyStrategy()) {
                step { scope, message ->
                    latch.countDown()
                }
            })

        // Publish a message
        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchAndForget(connection, testTopic, payload)
                }
                .await()
                .indefinitely()

        messageIdRef.set(message.id)

        // Wait for handlers to process the message
        assertTrue(latch.await(1, TimeUnit.SECONDS), "Handlers should process the message")

        // Verify one SEEN even per handler
        val seenCount1 = countMessageEvents(handlerName1, message.id, MessageEventType.SEEN)
        val seenCount2 = countMessageEvents(handlerName2, message.id, MessageEventType.SEEN)

        assertEquals(1, seenCount1, "Handler 1 should have exactly one SEEN event")
        assertEquals(1, seenCount2, "Handler 2 should have exactly one SEEN event")

        subscription1.close()
        subscription2.close()
    }

    @Test
    fun `should synchronize multiple instances of the same handler using message event records`() {
        // Create test payload
        val payload = objectMapper.createObjectNode().put("text", "Testing handler synchronization")
        val handlerName = "sync-test-handler"
        val processedCount = AtomicInteger(0)
        val messageIdRef = AtomicReference<UUID>()
        val latch = CountDownLatch(1) // Only one handler instance should process

        // Subscribe with two instances of the same handler
        val subscription1 =
            messageQueue.subscribe(testTopic, coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                step { scope, message ->
                    processedCount.incrementAndGet()
                    latch.countDown()
                }
            })

        val subscription2 =
            messageQueue.subscribe(testTopic, coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                step { scope, message ->
                    processedCount.incrementAndGet()
                    latch.countDown()
                }
            })

        // Publish a message
        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchAndForget(connection, testTopic, payload)
                }
                .await()
                .indefinitely()

        messageIdRef.set(message.id)

        // Wait for processing
        assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed")

        // Even though we have two instances subscribed, only one should process the message
        assertEquals(
            1,
            processedCount.get(),
            "Only one handler instance should process the message",
        )

        // Verify SEEN records
        val seenCount = countMessageEvents(handlerName, message.id, MessageEventType.SEEN)
        assertEquals(1, seenCount, "There should be exactly one SEEN event entry for this handler")

        subscription1.close()
        subscription2.close()
    }

    @Test
    fun `should write COMMITTED event on successful transaction`() {
        // Create test payload
        val payload = objectMapper.createObjectNode().put("text", "Testing COMMITTED event")
        val handlerName = "commit-test-handler"
        val messageIdRef = AtomicReference<UUID>()
        val latch = CountDownLatch(1)

        // Subscribe with handler that will succeed
        val subscription =
            messageQueue.subscribe(testTopic, coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                step { scope, message ->
                    latch.countDown()
                }
            })

        // Publish a message
        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchAndForget(connection, testTopic, payload)
                }
                .await()
                .indefinitely()

        messageIdRef.set(message.id)

        // Wait for processing
        assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed")

        // Give some time for the COMMITTED event to be written
        Thread.sleep(200)

        // Verify COMMITTED event
        val committedCount = countMessageEvents(handlerName, message.id, MessageEventType.COMMITTED)
        assertEquals(1, committedCount, "There should be exactly one COMMITTED event")
        subscription.close()
    }

    @Test
    fun `should write ROLLED_BACK event when exception is thrown`() {
        // Create test payload
        val payload = objectMapper.createObjectNode().put("text", "Testing ROLLED_BACK event")
        val handlerName = "rollback-test-handler"
        val messageIdRef = AtomicReference<UUID>()
        val latch = CountDownLatch(1)

        // Subscribe with handler that will fail
        val subscription =
            messageQueue.subscribe(testTopic, coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                step { scope, message ->
                    latch.countDown()
                    throw RuntimeException("Simulated failure to test rollback")
                }
            })

        // Publish a message
        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchAndForget(connection, testTopic, payload)
                }
                .await()
                .indefinitely()

        messageIdRef.set(message.id)

        // Wait for processing
        assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed")

        // Give some time for the ROLLED_BACK event to be written
        Thread.sleep(200)

        // Verify ROLLED_BACK event
        val rolledBackCount = countMessageEvents(handlerName, message.id, MessageEventType.ROLLED_BACK)
        assertEquals(1, rolledBackCount, "There should be exactly one ROLLED_BACK event")
        subscription.close()
    }

    @Test
    fun `should follow complete message event writing sequence on successful processing`() {
        // Create test payload
        val payload = objectMapper.createObjectNode().put("text", "Testing full event sequence")
        val handlerName = "sequence-test-handler"
        val latch = CountDownLatch(1)

        // Subscribe with handler that will succeed
        val subscription =
            messageQueue.subscribe(testTopic, coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                step { scope, message ->
                    latch.countDown()
                }
            })

        // Publish a message
        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchAndForget(connection, testTopic, payload)
                }
                .await()
                .indefinitely()

        // Wait for processing
        assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed")

        // Check the entire sequence
        val emittedCount = countMessageEvents(message.id, MessageEventType.EMITTED)
        val seenCount = countMessageEvents(handlerName, message.id, MessageEventType.SEEN)
        val committedCount = countMessageEvents(handlerName, message.id, MessageEventType.COMMITTED)
        val rolledBackCount = countMessageEvents(handlerName, message.id, MessageEventType.ROLLED_BACK)

        assertEquals(1, emittedCount, "There should be exactly one EMITTED event")
        assertEquals(1, seenCount, "There should be exactly one SEEN event")
        assertEquals(1, committedCount, "There should be exactly one COMMITTED event")
        assertEquals(0, rolledBackCount, "There should be no ROLLED_BACK event")

        // Verify the chronological order of events
        val events =
            pool
                .preparedQuery(
                    """
                SELECT type, created_at
                FROM message_events
                WHERE message_id = $1 AND (handler_name = $2 OR type = 'EMITTED')
                ORDER BY created_at ASC
                """
                )
                .execute(Tuple.of(message.id, handlerName))
                .await()
                .indefinitely()
                .map { row -> row.getString("type") }
                .toList()

        assertEquals(3, events.size, "There should be three events in total")
        assertEquals(MessageEventType.EMITTED.name, events[0], "First event should be EMITTED")
        assertEquals(MessageEventType.SEEN.name, events[1], "Second event should be SEEN")
        assertEquals(MessageEventType.COMMITTED.name, events[2], "Third event should be COMMITTED")
        subscription.close()
    }

    @Test
    fun `should follow complete message event writing sequence on failed processing`() {
        // Create test payload
        val payload = objectMapper.createObjectNode().put("text", "Testing failed event sequence")
        val handlerName = "failed-sequence-handler"
        val latch = CountDownLatch(1)

        // Subscribe with handler that will fail
        val subscription =
            messageQueue.subscribe(testTopic, coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                step { scope, message ->
                    latch.countDown()
                    throw RuntimeException("Simulated failure for event sequence test")
                }
            })

        // Publish a message
        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchAndForget(connection, testTopic, payload)
                }
                .await()
                .indefinitely()

        // Wait for processing
        assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed (and fail)")

        // Give some time for all event entries to be written
        Thread.sleep(200)

        // Check the entire sequence
        val emittedCount = countMessageEvents(message.id, MessageEventType.EMITTED)
        val seenCount = countMessageEvents(handlerName, message.id, MessageEventType.SEEN)
        val committedCount = countMessageEvents(handlerName, message.id, MessageEventType.COMMITTED)
        val rolledBackCount = countMessageEvents(handlerName, message.id, MessageEventType.ROLLED_BACK)

        assertEquals(1, emittedCount, "There should be exactly one EMITTED event entry")
        assertEquals(1, seenCount, "There should be exactly one SEEN event entry")
        assertEquals(0, committedCount, "There should be no COMMITTED event entries")
        assertEquals(1, rolledBackCount, "There should be exactly one ROLLED_BACK event entry")

        // Verify the chronological order of entries
        val events =
            pool
                .preparedQuery(
                    """
                SELECT type, created_at
                FROM message_events
                WHERE message_id = $1 AND (handler_name = $2 OR type = 'EMITTED')
                ORDER BY created_at ASC
                """
                )
                .execute(Tuple.of(message.id, handlerName))
                .await()
                .indefinitely()
                .map { row -> row.getString("type") }
                .toList()

        assertEquals(3, events.size, "There should be three event entries in total")
        assertEquals(MessageEventType.EMITTED.name, events[0], "First entry should be EMITTED")
        assertEquals(MessageEventType.SEEN.name, events[1], "Second entry should be SEEN")
        assertEquals(
            MessageEventType.ROLLED_BACK.name,
            events[2],
            "Third entry should be ROLLED_BACK",
        )
        subscription.close()
    }

    @Test
    fun `multiple handler instances should coordinate using message events for multiple messages`() {
        // Create multiple messages
        val messageCount = 5
        val handlerName = "concurrent-handler"
        val processedMessages = ConcurrentHashMap.newKeySet<UUID>()
        val countDownLatch = CountDownLatch(messageCount)

        // Subscribe with two instances of the same handler
        val subscription1 =
            messageQueue.subscribe(testTopic, coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                step { scope, message ->
                    processedMessages.add(message.id)
                    countDownLatch.countDown()
                }
            })

        val subscription2 =
            messageQueue.subscribe(testTopic, coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                step { scope, message ->
                    processedMessages.add(message.id)
                    countDownLatch.countDown()
                }
            })

        // Publish multiple messages
        val messages = mutableListOf<UUID>()
        for (i in 1..messageCount) {
            val payload = objectMapper.createObjectNode().put("text", "Concurrent message $i")
            val message =
                pool
                    .withTransaction { connection ->
                        messageQueue.launchAndForget(connection, testTopic, payload)
                    }
                    .await()
                    .indefinitely()
            messages.add(message.id)
        }

        // Wait for all messages to be processed
        assertTrue(countDownLatch.await(1, TimeUnit.SECONDS), "All messages should be processed")

        // Verify each message was handled exactly once
        assertEquals(
            messageCount,
            processedMessages.size,
            "All messages should be handled exactly once",
        )
        assertTrue(
            processedMessages.containsAll(messages),
            "All published messages should be processed",
        )

        // Verify event entries for each message
        for (messageId in messages) {
            val emittedCount = countMessageEvents(messageId, MessageEventType.EMITTED)
            val seenCount = countMessageEvents(handlerName, messageId, MessageEventType.SEEN)
            val committedCount = countMessageEvents(handlerName, messageId, MessageEventType.COMMITTED)

            assertEquals(
                1,
                emittedCount,
                "Each message should have exactly one EMITTED event",
            )
            assertEquals(
                1,
                seenCount,
                "Each message should have exactly one SEEN event for this handler",
            )
            assertEquals(
                1,
                committedCount,
                "Each message should have exactly one COMMITTED event for this handler",
            )
        }

        subscription1.close()
        subscription2.close()
    }
}
