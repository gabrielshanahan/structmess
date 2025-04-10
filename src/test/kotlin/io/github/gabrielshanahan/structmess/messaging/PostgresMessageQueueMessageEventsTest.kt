package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.domain.coroutine
import io.quarkus.test.junit.QuarkusTest
import io.vertx.mutiny.sqlclient.Pool
import io.vertx.mutiny.sqlclient.Tuple
import jakarta.inject.Inject
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresMessageQueueMessageEventsTest {

    @Inject lateinit var messageQueue: MessageQueue

    @Inject lateinit var handlerRegistry: HandlerRegistry

    @Inject lateinit var objectMapper: ObjectMapper

    @Inject lateinit var pool: Pool

    private val testTopic = "test-events-topic"

    @BeforeEach
    fun cleanupDatabase() {
        pool.executeAndAwaitPreparedQuery("TRUNCATE TABLE message_event, message CASCADE")
    }

    private fun countMessageEvents(messageId: UUID, type: String): Int {
        return pool
            .executeAndAwaitPreparedQuery(
                "SELECT COUNT(*) FROM message_event WHERE message_id = $1 AND type = $2",
                Tuple.of(messageId, type),
            )
            .first()
            .getInteger(0)
    }

    private fun countMessageEvents(handlerName: String, messageId: UUID, type: String): Int {
        return pool
            .executeAndAwaitPreparedQuery(
                "SELECT COUNT(*) FROM message_event WHERE coroutine_name = $1 AND message_id = $2 AND type = $3",
                Tuple.of(handlerName, messageId, type),
            )
            .first()
            .getInteger(0)
    }

    @Test
    fun `should write EMITTED event when message is published`() {
        val payload = objectMapper.createObjectNode().put("text", "Testing EMITTED event")

        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchOnGlobalScope(connection, testTopic, payload)
                }
                .await()
                .indefinitely()
                .message

        val emittedCount = countMessageEvents(message.id, "EMITTED")
        assertEquals(1, emittedCount, "There should be exactly one EMITTED event message")

        val handlerRecorded =
            pool
                .executeAndAwaitPreparedQuery(
                    "SELECT type FROM message_event WHERE message_id = $1",
                    Tuple.of(message.id),
                )
                .single()

        assertEquals("EMITTED".toString(), handlerRecorded.getString("type"))
    }

    @Test
    fun `should write one SEEN event per handler`() {
        val payload = objectMapper.createObjectNode().put("text", "Testing SEEN event")
        val handlerName1 = "test-handler-1"
        val handlerName2 = "test-handler-2"
        val messageIdRef = AtomicReference<UUID>()
        val latch = CountDownLatch(2)

        val subscription1 =
            messageQueue.subscribe(
                testTopic,
                coroutine(handlerName1, handlerRegistry.cooperationHierarchyStrategy()) {
                    step { scope, message -> latch.countDown() }
                },
            )

        val subscription2 =
            messageQueue.subscribe(
                testTopic,
                coroutine(handlerName2, handlerRegistry.cooperationHierarchyStrategy()) {
                    step { scope, message -> latch.countDown() }
                },
            )

        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchOnGlobalScope(connection, testTopic, payload)
                }
                .await()
                .indefinitely()
                .message

        messageIdRef.set(message.id)

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Handlers should process the message")

        val seenCount1 = countMessageEvents(handlerName1, message.id, "SEEN")
        val seenCount2 = countMessageEvents(handlerName2, message.id, "SEEN")

        assertEquals(1, seenCount1, "Handler 1 should have exactly one SEEN event")
        assertEquals(1, seenCount2, "Handler 2 should have exactly one SEEN event")

        subscription1.close()
        subscription2.close()
    }

    @Test
    fun `should synchronize multiple instances of the same handler using message event records`() {
        val payload = objectMapper.createObjectNode().put("text", "Testing handler synchronization")
        val handlerName = "sync-test-handler"
        val processedCount = AtomicInteger(0)
        val messageIdRef = AtomicReference<UUID>()
        val latch = CountDownLatch(1)

        val subscription1 =
            messageQueue.subscribe(
                testTopic,
                coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                    step { scope, message ->
                        processedCount.incrementAndGet()
                        latch.countDown()
                    }
                },
            )

        val subscription2 =
            messageQueue.subscribe(
                testTopic,
                coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                    step { scope, message ->
                        processedCount.incrementAndGet()
                        latch.countDown()
                    }
                },
            )

        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchOnGlobalScope(connection, testTopic, payload)
                }
                .await()
                .indefinitely()
                .message

        messageIdRef.set(message.id)

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed")

        assertEquals(
            1,
            processedCount.get(),
            "Only one handler instance should process the message",
        )

        val seenCount = countMessageEvents(handlerName, message.id, "SEEN")
        assertEquals(1, seenCount, "There should be exactly one SEEN event entry for this handler")

        subscription1.close()
        subscription2.close()
    }

    @Test
    fun `should write COMMITTED event on successful transaction`() {
        val payload = objectMapper.createObjectNode().put("text", "Testing COMMITTED event")
        val handlerName = "commit-test-handler"
        val messageIdRef = AtomicReference<UUID>()
        val latch = CountDownLatch(1)

        val subscription =
            messageQueue.subscribe(
                testTopic,
                coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                    step { scope, message -> latch.countDown() }
                },
            )

        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchOnGlobalScope(connection, testTopic, payload)
                }
                .await()
                .indefinitely()
                .message

        messageIdRef.set(message.id)

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed")

        Thread.sleep(150)

        val committedCount = countMessageEvents(handlerName, message.id, "COMMITTED")
        assertEquals(1, committedCount, "There should be exactly one COMMITTED event")
        subscription.close()
    }

    @Test
    fun `should write ROLLED_BACK event when exception is thrown`() {
        val payload = objectMapper.createObjectNode().put("text", "Testing ROLLED_BACK event")
        val handlerName = "rollback-test-handler"
        val messageIdRef = AtomicReference<UUID>()
        val latch = CountDownLatch(1)

        val subscription =
            messageQueue.subscribe(
                testTopic,
                coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                    step { scope, message ->
                        latch.countDown()
                        throw RuntimeException("Simulated failure to test rollback")
                    }
                },
            )

        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchOnGlobalScope(connection, testTopic, payload)
                }
                .await()
                .indefinitely()
                .message

        messageIdRef.set(message.id)

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed")

        Thread.sleep(150)

        val rolledBackCount = countMessageEvents(handlerName, message.id, "ROLLED_BACK")
        assertEquals(1, rolledBackCount, "There should be exactly one ROLLED_BACK event")
        subscription.close()
    }

    @Test
    fun `should follow complete message event writing sequence on successful processing`() {
        val payload = objectMapper.createObjectNode().put("text", "Testing full event sequence")
        val handlerName = "sequence-test-handler"
        val latch = CountDownLatch(1)

        val subscription =
            messageQueue.subscribe(
                testTopic,
                coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                    step { scope, message -> latch.countDown() }
                },
            )

        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchOnGlobalScope(connection, testTopic, payload)
                }
                .await()
                .indefinitely()
                .message

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed")

        Thread.sleep(150)

        val emittedCount = countMessageEvents(message.id, "EMITTED")
        val seenCount = countMessageEvents(handlerName, message.id, "SEEN")
        val committedCount = countMessageEvents(handlerName, message.id, "COMMITTED")
        val rolledBackCount = countMessageEvents(handlerName, message.id, "ROLLED_BACK")

        assertEquals(1, emittedCount, "There should be exactly one EMITTED event")
        assertEquals(1, seenCount, "There should be exactly one SEEN event")
        assertEquals(1, committedCount, "There should be exactly one COMMITTED event")
        assertEquals(0, rolledBackCount, "There should be no ROLLED_BACK event")

        val events =
            pool
                .executeAndAwaitPreparedQuery(
                    """
                SELECT type, created_at
                FROM message_event
                WHERE message_id = $1 AND (coroutine_name = $2 OR type = 'EMITTED')
                ORDER BY created_at ASC
                """,
                    Tuple.of(message.id, handlerName),
                )
                .map { row -> row.getString("type") }
                .toList()

        assertEquals(4, events.size, "There should be three events in total")
        assertEquals("EMITTED", events[0], "First event should be EMITTED")
        assertEquals("SEEN", events[1], "Second event should be SEEN")
        assertEquals("SUSPENDED", events[2], "Third event should be SUSPENDED")
        assertEquals("COMMITTED", events[3], "Fourth event should be COMMITTED")
        subscription.close()
    }

    @Test
    fun `should follow complete message event writing sequence on failed processing`() {
        val payload = objectMapper.createObjectNode().put("text", "Testing failed event sequence")
        val handlerName = "failed-sequence-handler"
        val latch = CountDownLatch(1)

        val subscription =
            messageQueue.subscribe(
                testTopic,
                coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                    step { scope, message ->
                        latch.countDown()
                        throw RuntimeException("Simulated failure for event sequence test")
                    }
                },
            )

        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchOnGlobalScope(connection, testTopic, payload)
                }
                .await()
                .indefinitely()
                .message

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed (and fail)")

        Thread.sleep(150)

        val emittedCount = countMessageEvents(message.id, "EMITTED")
        val seenCount = countMessageEvents(handlerName, message.id, "SEEN")
        val committedCount = countMessageEvents(handlerName, message.id, "COMMITTED")
        val rollingBackCount = countMessageEvents(handlerName, message.id, "ROLLING_BACK")
        val rolledBackCount = countMessageEvents(handlerName, message.id, "ROLLED_BACK")

        assertEquals(1, emittedCount, "There should be exactly one EMITTED event entry")
        assertEquals(1, seenCount, "There should be exactly one SEEN event entry")
        assertEquals(0, committedCount, "There should be no COMMITTED event entries")
        assertEquals(1, rollingBackCount, "There should be exactly one ROLLING_BACK event entry")
        assertEquals(1, rolledBackCount, "There should be exactly one ROLLED_BACK event entry")

        val events =
            pool
                .executeAndAwaitPreparedQuery(
                    """
                SELECT type, created_at
                FROM message_event
                WHERE message_id = $1 AND (coroutine_name = $2 OR type = 'EMITTED')
                ORDER BY created_at ASC
                """,
                    Tuple.of(message.id, handlerName),
                )
                .map { row -> row.getString("type") }
                .toList()

        assertEquals(4, events.size, "There should be three event entries in total")
        assertEquals("EMITTED", events[0], "First entry should be EMITTED")
        assertEquals("SEEN", events[1], "Second entry should be SEEN")
        assertEquals("ROLLING_BACK", events[2], "Third entry should be ROLLING_BACK")
        assertEquals("ROLLED_BACK", events[3], "Fourth entry should be ROLLED_BACK")
        subscription.close()
    }

    @Test
    fun `multiple handler instances should coordinate using message events for multiple messages`() {
        val messageCount = 5
        val handlerName = "concurrent-handler"
        val processedMessages = ConcurrentHashMap.newKeySet<UUID>()
        val countDownLatch = CountDownLatch(messageCount)

        val subscription1 =
            messageQueue.subscribe(
                testTopic,
                coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                    step { scope, message ->
                        processedMessages.add(message.id)
                        countDownLatch.countDown()
                    }
                },
            )

        val subscription2 =
            messageQueue.subscribe(
                testTopic,
                coroutine(handlerName, handlerRegistry.cooperationHierarchyStrategy()) {
                    step { scope, message ->
                        processedMessages.add(message.id)
                        countDownLatch.countDown()
                    }
                },
            )

        val messages = mutableListOf<UUID>()
        for (i in 1..messageCount) {
            val payload = objectMapper.createObjectNode().put("text", "Concurrent message $i")
            val message =
                pool
                    .withTransaction { connection ->
                        messageQueue.launchOnGlobalScope(connection, testTopic, payload)
                    }
                    .await()
                    .indefinitely()
                    .message
            messages.add(message.id)
        }

        assertTrue(countDownLatch.await(1, TimeUnit.SECONDS), "All messages should be processed")
        Thread.sleep(100)

        assertEquals(
            messageCount,
            processedMessages.size,
            "All messages should be handled exactly once",
        )
        assertTrue(
            processedMessages.containsAll(messages),
            "All published messages should be processed",
        )

        for (messageId in messages) {
            val emittedCount = countMessageEvents(messageId, "EMITTED")
            val seenCount = countMessageEvents(handlerName, messageId, "SEEN")
            val committedCount = countMessageEvents(handlerName, messageId, "COMMITTED")

            assertEquals(1, emittedCount, "Each message should have exactly one EMITTED event")
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
