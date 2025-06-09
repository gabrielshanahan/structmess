package io.github.gabrielshanahan.scoop.reactive.messaging

import io.github.gabrielshanahan.scoop.reactive.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.reactive.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.reactive.coroutine.executeAndAwaitPreparedQuery
import io.quarkus.test.junit.QuarkusTest
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.Tuple
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresMessageQueueTest : StructuredCooperationTest() {

    private val testTopic = "test-topic"
    private val testHandler = "test-handler"

    @Test
    fun `should publish a message`() {
        val testPayload = JsonObject().put("text", "Hello, World!").put("priority", "HIGH")
        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launch(connection, testTopic, testPayload)
                }
                .await()
                .indefinitely()
                .message

        val persistedMessage =
            pool
                .withTransaction { connection -> messageQueue.fetch(connection, message.id) }
                .await()
                .indefinitely()!!

        assertNotNull(persistedMessage.id)
        assertEquals(testTopic, persistedMessage.topic)
        assertEquals(testPayload.getString("text"), persistedMessage.payload.getString("text"))
        assertEquals(
            testPayload.getString("priority"),
            persistedMessage.payload.getString("priority"),
        )
        assertNotNull(persistedMessage.createdAt)
    }

    @Test
    fun `should subscribe to messages`() {
        val messageCount = 5
        val receivedCount = AtomicInteger(0)
        val latch = CountDownLatch(messageCount)

        messageQueue.subscribe(
            testTopic,
            saga(testHandler, handlerRegistry.eventLoopStrategy()) {
                step { scope, message ->
                    receivedCount.incrementAndGet()
                    latch.countDown()
                }
            },
        )

        for (i in 1..messageCount) {
            val payload = JsonObject().put("text", "Message $i")
            pool
                .withTransaction { connection ->
                    messageQueue.launch(connection, testTopic, payload)
                }
                .await()
                .indefinitely()
        }

        val received = latch.await(1, TimeUnit.SECONDS)
        assertTrue(received)
        assertEquals(messageCount, receivedCount.get())
    }

    @Test
    fun `subscribe should isolate transactions between messages and correctly roll back failures`() {
        val latch = CountDownLatch(2)
        val failedMessageIndex = AtomicInteger(-1)
        val successMessageIndex = AtomicInteger(-1)

        val otherTopic = "otherTopic"
        val otherPayload = JsonObject().put("otherIndex", 1)

        messageQueue
            .subscribe(
                testTopic,
                saga(testHandler, handlerRegistry.eventLoopStrategy()) {
                    uniStep { scope, message ->
                        messageQueue
                            .launch(scope.connection, otherTopic, otherPayload)
                            .map { _ ->
                                val index = message.payload.getInteger("index")

                                if (index == 2) {
                                    successMessageIndex.set(index)
                                } else if (index == 1) {
                                    failedMessageIndex.set(index)
                                    // Throwing an exception to simulate a failure
                                    error("Simulated failure for message $index")
                                }
                            }
                            .onTermination()
                            .invoke { latch.countDown() }
                    }
                },
            )
            .use {
                pool.withTransactionAndForget { connection ->
                    val payload1 = JsonObject().put("index", 1)
                    val publish1 = messageQueue.launch(connection, testTopic, payload1)

                    val payload2 = JsonObject().put("index", 2)
                    val publish2 = messageQueue.launch(connection, testTopic, payload2)

                    // We do it like this, as opposed to Uni.combine().all().unis(publish1,
                    // publish2).discardItems();
                    // so we can guarantee the order, and we also test that the failure (the first
                    // message) doesn't affect
                    // the processing of the second
                    publish1.replaceWith(publish2).replaceWithVoid()
                }

                assertTrue(latch.await(1, TimeUnit.SECONDS))

                Thread.sleep(100)

                assertEquals(2, successMessageIndex.get())
                assertEquals(1, failedMessageIndex.get())

                val otherTopicMessageCount =
                    pool
                        .executeAndAwaitPreparedQuery(
                            "SELECT count(*) FROM message WHERE topic = $1",
                            Tuple.of(otherTopic),
                        )
                        .first()
                        .getInteger(0)

                assertEquals(
                    1,
                    otherTopicMessageCount,
                    "Only one message should have been published to otherTopic",
                )
            }
    }
}
