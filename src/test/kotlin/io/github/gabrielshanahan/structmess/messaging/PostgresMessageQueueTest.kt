package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.domain.coroutine
import io.quarkus.test.junit.QuarkusTest
import io.vertx.mutiny.sqlclient.Pool
import io.vertx.mutiny.sqlclient.Tuple
import jakarta.inject.Inject
import org.junit.jupiter.api.AfterEach
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
class PostgresMessageQueueTest {

    @Inject lateinit var messageQueue: MessageQueue

    @Inject lateinit var handlerRegistry: HandlerRegistry

    @Inject lateinit var objectMapper: ObjectMapper

    @Inject lateinit var pool: Pool

    private val testTopic = "test-topic"
    private val testHandler = "test-handler"

    @AfterEach
    fun cleanupDatabase() {
        pool
            .preparedQuery("TRUNCATE TABLE message_events, messages CASCADE")
            .execute()
            .await()
            .indefinitely()
    }

    @Test
    fun `should publish a message`() {
        val testPayload =
            objectMapper.createObjectNode().put("text", "Hello, World!").put("priority", "HIGH")
        val message =
            pool
                .withTransaction { connection ->
                    messageQueue.launchAndForget(connection, testTopic, testPayload)
                }
                .await()
                .indefinitely()

        val persistedMessage =
            pool
                .withTransaction { connection -> messageQueue.fetch(connection, message.id) }
                .await()
                .indefinitely()!!

        // Verify the published message
        assertNotNull(persistedMessage.id)
        assertEquals(testTopic, persistedMessage.topic)
        assertEquals(
            testPayload.get("text").asText(),
            persistedMessage.payload.get("text").asText(),
        )
        assertEquals(
            testPayload.get("priority").asText(),
            persistedMessage.payload.get("priority").asText(),
        )
        assertNotNull(persistedMessage.createdAt)
    }

    @Test
    fun `should subscribe to messages`() {
        val messageCount = 5
        val receivedCount = AtomicInteger(0)
        val latch = CountDownLatch(messageCount)

        // Subscribe to the topic
        messageQueue.subscribe(testTopic, coroutine(testHandler, handlerRegistry.cooperationHierarchyStrategy()) {
            step { scope, message ->
                receivedCount.incrementAndGet()
                latch.countDown()
            }
        })

        // Publish messages
        for (i in 1..messageCount) {
            val payload = objectMapper.createObjectNode().put("text", "Message $i")
            pool
                .withTransaction { connection ->
                    messageQueue.launchAndForget(connection, testTopic, payload)
                }
                .await()
                .indefinitely()
        }

        // Wait for all messages to be received
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
        val otherPayload = objectMapper.createObjectNode().put("otherIndex", 1)

        // Subscribe with a handler that will fail for one message but succeed for another
        messageQueue
            .subscribe(testTopic, coroutine(testHandler, handlerRegistry.cooperationHierarchyStrategy()) {
                step { scope, message ->
                    messageQueue
                        .launchAndForget(scope.connection, otherTopic, otherPayload)
                        .map { _ ->
                            val index = message.payload.get("index").asInt()

                            if (index == 2) {
                                successMessageIndex.set(index)
                            } else if (index == 1) {
                                failedMessageIndex.set(index)
                                // Throwing an exception to simulate a failure
                                error("Simulated failure for message $index")
                            }
                        }
                        .onTermination()
                        .invoke(Runnable { latch.countDown() })
                        .await().indefinitely()
                }
            })
            .use {
                pool.withTransactionAndForget { connection ->
                    // Publish two messages
                    val payload1 = objectMapper.createObjectNode().put("index", 1)
                    val publish1 = messageQueue.launchAndForget(connection, testTopic, payload1)

                    val payload2 = objectMapper.createObjectNode().put("index", 2)
                    val publish2 = messageQueue.launchAndForget(connection, testTopic, payload2)

                    // We do it like this, as opposed to Uni.combine().all().unis(publish1,
                    // publish2).discardItems();
                    // so we can guarantee the order, and we also test that the failure (the first
                    // message) doesn't affect
                    // the processing of the second
                    publish1.replaceWith(publish2).replaceWithVoid()
                }

                // Wait for both messages to be processed
                assertTrue(latch.await(1, TimeUnit.SECONDS))

                // Verify the first message was processed successfully
                assertEquals(2, successMessageIndex.get())
                // Verify the second message attempted processing but failed
                assertEquals(1, failedMessageIndex.get())

                // Verify only one message was published to otherTopic
                val otherTopicMessageCount =
                    pool
                        .withTransactionAndAwait { connection ->
                            connection
                                .preparedQuery("SELECT count(*) FROM messages WHERE topic = $1")
                                .execute(Tuple.of(otherTopic))
                        }
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
