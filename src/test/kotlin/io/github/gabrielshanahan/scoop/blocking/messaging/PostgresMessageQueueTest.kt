package io.github.gabrielshanahan.scoop.blocking.messaging

import io.agroal.pool.wrapper.ConnectionWrapper
import io.github.gabrielshanahan.scoop.blocking.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.blocking.transactional
import io.quarkus.test.junit.QuarkusTest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.postgresql.jdbc.PgConnection

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresMessageQueueTest : StructuredCooperationTest() {

    private val testTopic = "test-topic"
    private val testHandler = "test-handler"

    @Test
    fun `should publish a message`() {
        val testPayload =
            jsonbHelper.toPGobject(mapOf("text" to "Hello, World!", "priority" to "HIGH"))
        val message =
            fluentJdbc
                .transactional { connection ->
                    messageQueue.launch(connection, testTopic, testPayload)
                }
                .message

        val persistedMessage =
            fluentJdbc.transactional { connection -> messageQueue.fetch(connection, message.id) }!!

        val testPayloadMap = jsonbHelper.fromPGobjectToMap<String, String>(testPayload)
        val persistedMessageMap =
            jsonbHelper.fromPGobjectToMap<String, String>(persistedMessage.payload)
        assertNotNull(persistedMessage.id)
        assertEquals(testTopic, persistedMessage.topic)
        assertEquals(testPayloadMap["text"], persistedMessageMap["text"])
        assertEquals(testPayloadMap["priority"], persistedMessageMap["priority"])
        assertNotNull(persistedMessage.createdAt)
        Thread.sleep(200)
    }

    @Test
    fun `should subscribe to messages`() {
        val messageCount = 5
        val receivedCount = AtomicInteger(0)
        val latch = CountDownLatch(messageCount)

        messageQueue
            .subscribe(
                testTopic,
                saga(testHandler, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        receivedCount.incrementAndGet()
                        latch.countDown()
                    }
                },
            )
            .use {
                for (i in 1..messageCount) {
                    val payload = jsonbHelper.toPGobject(mapOf("text" to "Message $i"))
                    fluentJdbc.transactional { connection ->
                        messageQueue.launch(connection, testTopic, payload)
                    }
                }

                val received = latch.await(1, TimeUnit.SECONDS)
                assertTrue(received)
                assertEquals(messageCount, receivedCount.get())
            }
        Thread.sleep(200)
    }

    @Test
    fun `subscribe should isolate transactions between messages and correctly roll back failures`() {
        val latch = CountDownLatch(2)
        val failedMessageIndex = AtomicInteger(-1)
        val successMessageIndex = AtomicInteger(-1)

        val otherTopic = "otherTopic"
        val otherPayload = jsonbHelper.toPGobject(mapOf("otherIndex" to 1))

        messageQueue
            .subscribe(
                testTopic,
                saga(testHandler, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        val root = messageQueue.launch(scope.connection, otherTopic, otherPayload)
                        println(
                            "[${(scope.connection as ConnectionWrapper).unwrap(PgConnection::class.java).backendPID}]Root: $root"
                        )

                        val index =
                            jsonbHelper.fromPGobjectToMap<String, Int>(message.payload)["index"]

                        if (index == 2) {
                            successMessageIndex.set(index)
                            latch.countDown()
                        } else if (index == 1) {
                            failedMessageIndex.set(index)
                            latch.countDown()
                            // Throwing an exception to simulate a failure
                            error("Simulated failure for message $index")
                        }
                    }
                },
            )
            .use {
                fluentJdbc.transactional { connection ->
                    messageQueue.launch(
                        connection,
                        testTopic,
                        jsonbHelper.toPGobject(mapOf("index" to 1)),
                    )
                    messageQueue.launch(
                        connection,
                        testTopic,
                        jsonbHelper.toPGobject(mapOf("index" to 2)),
                    )
                }

                assertTrue(latch.await(1, TimeUnit.SECONDS))

                Thread.sleep(100)

                assertEquals(2, successMessageIndex.get())
                assertEquals(1, failedMessageIndex.get())

                val otherTopicMessageCount =
                    fluentJdbc
                        .query()
                        .select("SELECT count(*) FROM message WHERE topic = :topic")
                        .namedParam("topic", otherTopic)
                        .singleResult { it.getInt("count") }

                assertEquals(
                    1,
                    otherTopicMessageCount,
                    "Only one message should have been published to otherTopic",
                )
            }
        Thread.sleep(200)
    }
}
