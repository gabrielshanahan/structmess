package io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.blocking.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.blocking.messaging.eventLoopStrategy
import io.github.gabrielshanahan.scoop.blocking.transactional
import io.quarkus.test.junit.QuarkusTest
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MessageEventsTest : StructuredCooperationTest() {
    private val testTopic = "test-events-topic"

    private fun countMessageEvents(messageId: UUID, type: String): Int =
        fluentJdbc
            .query()
            .select(
                "SELECT COUNT(*) FROM message_event WHERE message_id = :message_id AND type = :type::message_event_type"
            )
            .namedParam("message_id", messageId)
            .namedParam("type", type)
            .singleResult { it.getInt("count") }

    private fun countMessageEvents(handlerName: String, messageId: UUID, type: String): Int =
        fluentJdbc
            .query()
            .select(
                "SELECT COUNT(*) FROM message_event WHERE coroutine_name = :coroutine_name AND message_id = :message_id AND type = :type::message_event_type"
            )
            .namedParam("coroutine_name", handlerName)
            .namedParam("message_id", messageId)
            .namedParam("type", type)
            .singleResult { it.getInt("count") }

    @Test
    fun `should write EMITTED event when message is published`() {
        val payload = jsonbHelper.toPGobject(mapOf("text" to "Testing EMITTED event"))

        val message =
            fluentJdbc
                .transactional { connection -> messageQueue.launch(connection, testTopic, payload) }
                .message

        val emittedCount = countMessageEvents(message.id, "EMITTED")
        Assertions.assertEquals(
            1,
            emittedCount,
            "There should be exactly one EMITTED event message",
        )

        val handlerRecorded =
            fluentJdbc
                .query()
                .select("SELECT type FROM message_event WHERE message_id = :message_id")
                .namedParam("message_id", message.id)
                .singleResult { it.getString("type") }

        Assertions.assertEquals("EMITTED", handlerRecorded)
    }

    @Test
    fun `should write one SEEN event per handler`() {
        val payload = jsonbHelper.toPGobject(mapOf("text" to "Testing SEEN event"))
        val handlerName1 = "test-handler-1"
        val handlerName2 = "test-handler-2"
        val messageIdRef = AtomicReference<UUID>()
        val latch = CountDownLatch(2)

        val subscription1 =
            messageQueue.subscribe(
                testTopic,
                saga(handlerName1, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message -> latch.countDown() }
                },
            )

        val subscription2 =
            messageQueue.subscribe(
                testTopic,
                saga(handlerName2, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message -> latch.countDown() }
                },
            )

        val message =
            fluentJdbc
                .transactional { connection -> messageQueue.launch(connection, testTopic, payload) }
                .message

        messageIdRef.set(message.id)

        Assertions.assertTrue(
            latch.await(1, TimeUnit.SECONDS),
            "Handlers should process the message",
        )

        val seenCount1 = countMessageEvents(handlerName1, message.id, "SEEN")
        val seenCount2 = countMessageEvents(handlerName2, message.id, "SEEN")

        Assertions.assertEquals(1, seenCount1, "Handler 1 should have exactly one SEEN event")
        Assertions.assertEquals(1, seenCount2, "Handler 2 should have exactly one SEEN event")

        subscription1.close()
        subscription2.close()
    }

    @Test
    fun `should synchronize multiple instances of the same handler using message event records`() {
        val payload = jsonbHelper.toPGobject(mapOf("text" to "Testing handler synchronization"))
        val handlerName = "sync-test-handler"
        val processedCount = AtomicInteger(0)
        val messageIdRef = AtomicReference<UUID>()
        val latch = CountDownLatch(1)

        val subscription1 =
            messageQueue.subscribe(
                testTopic,
                saga(handlerName, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        processedCount.incrementAndGet()
                        latch.countDown()
                    }
                },
            )

        val subscription2 =
            messageQueue.subscribe(
                testTopic,
                saga(handlerName, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        processedCount.incrementAndGet()
                        latch.countDown()
                    }
                },
            )

        val message =
            fluentJdbc
                .transactional { connection -> messageQueue.launch(connection, testTopic, payload) }
                .message

        messageIdRef.set(message.id)

        Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed")

        Assertions.assertEquals(
            1,
            processedCount.get(),
            "Only one handler instance should process the message",
        )

        val seenCount = countMessageEvents(handlerName, message.id, "SEEN")
        Assertions.assertEquals(
            1,
            seenCount,
            "There should be exactly one SEEN event entry for this handler",
        )

        subscription1.close()
        subscription2.close()
    }

    @Test
    fun `should write COMMITTED event on successful transaction`() {
        val payload = jsonbHelper.toPGobject(mapOf("text" to "Testing COMMITTED event"))
        val handlerName = "commit-test-handler"
        val messageIdRef = AtomicReference<UUID>()
        val latch = CountDownLatch(1)

        val subscription =
            messageQueue.subscribe(
                testTopic,
                saga(handlerName, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message -> latch.countDown() }
                },
            )

        val message =
            fluentJdbc
                .transactional { connection -> messageQueue.launch(connection, testTopic, payload) }
                .message

        messageIdRef.set(message.id)

        Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed")

        Thread.sleep(150)

        val committedCount = countMessageEvents(handlerName, message.id, "COMMITTED")
        Assertions.assertEquals(1, committedCount, "There should be exactly one COMMITTED event")
        subscription.close()
    }

    @Test
    fun `should write ROLLED_BACK event when exception is thrown`() {
        val payload = jsonbHelper.toPGobject(mapOf("text" to "Testing ROLLED_BACK event"))
        val handlerName = "rollback-test-handler"
        val messageIdRef = AtomicReference<UUID>()
        val latch = CountDownLatch(1)

        val subscription =
            messageQueue.subscribe(
                testTopic,
                saga(handlerName, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        latch.countDown()
                        throw RuntimeException("Simulated failure to test rollback")
                    }
                },
            )

        val message =
            fluentJdbc
                .transactional { connection -> messageQueue.launch(connection, testTopic, payload) }
                .message

        messageIdRef.set(message.id)

        Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed")

        Thread.sleep(150)

        val rolledBackCount = countMessageEvents(handlerName, message.id, "ROLLED_BACK")
        Assertions.assertEquals(1, rolledBackCount, "There should be exactly one ROLLED_BACK event")
        subscription.close()
    }

    @Test
    fun `should follow complete message event writing sequence on successful processing`() {
        val payload = jsonbHelper.toPGobject(mapOf("text" to "Testing full event sequence"))
        val handlerName = "sequence-test-handler"
        val latch = CountDownLatch(1)

        val subscription =
            messageQueue.subscribe(
                testTopic,
                saga(handlerName, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message -> latch.countDown() }
                },
            )

        val message =
            fluentJdbc
                .transactional { connection -> messageQueue.launch(connection, testTopic, payload) }
                .message

        Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "Message should be processed")

        Thread.sleep(150)

        val emittedCount = countMessageEvents(message.id, "EMITTED")
        val seenCount = countMessageEvents(handlerName, message.id, "SEEN")
        val committedCount = countMessageEvents(handlerName, message.id, "COMMITTED")
        val rolledBackCount = countMessageEvents(handlerName, message.id, "ROLLED_BACK")

        Assertions.assertEquals(1, emittedCount, "There should be exactly one EMITTED event")
        Assertions.assertEquals(1, seenCount, "There should be exactly one SEEN event")
        Assertions.assertEquals(1, committedCount, "There should be exactly one COMMITTED event")
        Assertions.assertEquals(0, rolledBackCount, "There should be no ROLLED_BACK event")

        val events =
            fluentJdbc
                .query()
                .select(
                    """
                SELECT type
                FROM message_event
                WHERE message_id = :message_id AND (coroutine_name = :handler_name OR type = 'EMITTED')
                ORDER BY created_at ASC
                """
                )
                .namedParam("message_id", message.id)
                .namedParam("handler_name", handlerName)
                .listResult { it.getString("type") }

        Assertions.assertEquals(4, events.size, "There should be three events in total")
        Assertions.assertEquals("EMITTED", events[0], "First event should be EMITTED")
        Assertions.assertEquals("SEEN", events[1], "Second event should be SEEN")
        Assertions.assertEquals("SUSPENDED", events[2], "Third event should be SUSPENDED")
        Assertions.assertEquals("COMMITTED", events[3], "Fourth event should be COMMITTED")
        subscription.close()
    }

    @Test
    fun `should follow complete message event writing sequence on failed processing`() {
        val payload = jsonbHelper.toPGobject(mapOf("text" to "Testing failed event sequence"))
        val handlerName = "failed-sequence-handler"
        val latch = CountDownLatch(1)

        val subscription =
            messageQueue.subscribe(
                testTopic,
                saga(handlerName, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        latch.countDown()
                        throw RuntimeException("Simulated failure for event sequence test")
                    }
                },
            )

        val message =
            fluentJdbc
                .transactional { connection -> messageQueue.launch(connection, testTopic, payload) }
                .message

        Assertions.assertTrue(
            latch.await(1, TimeUnit.SECONDS),
            "Message should be processed (and fail)",
        )

        Thread.sleep(1000)

        val emittedCount = countMessageEvents(message.id, "EMITTED")
        val seenCount = countMessageEvents(handlerName, message.id, "SEEN")
        val committedCount = countMessageEvents(handlerName, message.id, "COMMITTED")
        val rollingBackCount = countMessageEvents(handlerName, message.id, "ROLLING_BACK")
        val rolledBackCount = countMessageEvents(handlerName, message.id, "ROLLED_BACK")

        Assertions.assertEquals(1, emittedCount, "There should be exactly one EMITTED event entry")
        Assertions.assertEquals(1, seenCount, "There should be exactly one SEEN event entry")
        Assertions.assertEquals(0, committedCount, "There should be no COMMITTED event entries")
        Assertions.assertEquals(
            1,
            rollingBackCount,
            "There should be exactly one ROLLING_BACK event entry",
        )
        Assertions.assertEquals(
            1,
            rolledBackCount,
            "There should be exactly one ROLLED_BACK event entry",
        )

        val events =
            fluentJdbc
                .query()
                .select(
                    """
                SELECT type
                FROM message_event
                WHERE message_id = :message_id AND (coroutine_name = :handler_name OR type = 'EMITTED')
                ORDER BY created_at ASC
                """
                )
                .namedParam("message_id", message.id)
                .namedParam("handler_name", handlerName)
                .listResult { it.getString("type") }

        Assertions.assertEquals(4, events.size, "There should be three event entries in total")
        Assertions.assertEquals("EMITTED", events[0], "First entry should be EMITTED")
        Assertions.assertEquals("SEEN", events[1], "Second entry should be SEEN")
        Assertions.assertEquals("ROLLING_BACK", events[2], "Third entry should be ROLLING_BACK")
        Assertions.assertEquals("ROLLED_BACK", events[3], "Fourth entry should be ROLLED_BACK")
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
                saga(handlerName, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        processedMessages.add(message.id)
                        countDownLatch.countDown()
                    }
                },
            )

        val subscription2 =
            messageQueue.subscribe(
                testTopic,
                saga(handlerName, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        processedMessages.add(message.id)
                        countDownLatch.countDown()
                    }
                },
            )

        val messages = mutableListOf<UUID>()
        for (i in 1..messageCount) {
            val payload = jsonbHelper.toPGobject(mapOf("text" to "Concurrent message $i"))
            val message =
                fluentJdbc
                    .transactional { connection ->
                        messageQueue.launch(connection, testTopic, payload)
                    }
                    .message
            messages.add(message.id)
        }

        Assertions.assertTrue(
            countDownLatch.await(1, TimeUnit.SECONDS),
            "All messages should be processed",
        )
        Thread.sleep(100)

        Assertions.assertEquals(
            messageCount,
            processedMessages.size,
            "All messages should be handled exactly once",
        )
        Assertions.assertTrue(
            processedMessages.containsAll(messages),
            "All published messages should be processed",
        )

        for (messageId in messages) {
            val emittedCount = countMessageEvents(messageId, "EMITTED")
            val seenCount = countMessageEvents(handlerName, messageId, "SEEN")
            val committedCount = countMessageEvents(handlerName, messageId, "COMMITTED")

            Assertions.assertEquals(
                1,
                emittedCount,
                "Each message should have exactly one EMITTED event",
            )
            Assertions.assertEquals(
                1,
                seenCount,
                "Each message should have exactly one SEEN event for this handler",
            )
            Assertions.assertEquals(
                1,
                committedCount,
                "Each message should have exactly one COMMITTED event for this handler",
            )
        }

        subscription1.close()
        subscription2.close()
    }
}
