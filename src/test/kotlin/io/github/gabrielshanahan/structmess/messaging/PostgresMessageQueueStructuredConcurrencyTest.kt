package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.coroutine.CooperationContext
import io.github.gabrielshanahan.structmess.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.structmess.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.structmess.coroutine.HappyPathDeadline
import io.github.gabrielshanahan.structmess.coroutine.eventLoopStrategy
import io.github.gabrielshanahan.structmess.coroutine.happyPathTimeout
import io.github.gabrielshanahan.structmess.coroutine.has
import io.github.gabrielshanahan.structmess.coroutine.noHappyPathTimeout
import io.github.gabrielshanahan.structmess.coroutine.saga
import io.github.gabrielshanahan.structmess.domain.CooperationException
import io.github.gabrielshanahan.structmess.domain.CooperationFailure
import io.quarkus.test.junit.QuarkusTest
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.Pool
import io.vertx.mutiny.sqlclient.SqlClient
import io.vertx.mutiny.sqlclient.Tuple
import jakarta.inject.Inject
import java.time.format.DateTimeFormatter
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresMessageQueueStructuredConcurrencyTest {
    @Inject lateinit var messageQueue: MessageQueue

    @Inject lateinit var structuredCooperationManager: StructuredCooperationManager

    @Inject lateinit var handlerRegistry: HandlerRegistry

    @Inject lateinit var objectMapper: ObjectMapper

    @Inject lateinit var pool: Pool

    private val rootTopic = "root-topic"
    private val childTopic = "child-topic"
    private val grandchildTopic = "grandchild-topic"

    @BeforeEach
    fun cleanupDatabase() {
        pool.executeAndAwaitPreparedQuery("TRUNCATE TABLE message_event, message CASCADE")
    }

    @Nested
    inner class HappyPaths {
        @Test
        fun `handler should not complete until handlers listening to emitted messages complete - depth 1`() {
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val latch = CountDownLatch(4) // 2 root steps + 2 child steps

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            val childPayload = JsonObject().put("from", "root-handler")
                            scope.launch(childTopic, childPayload).await().indefinitely()
                            latch.countDown()
                            executionOrder.add("root-handler-step-1")
                        }

                        step { scope, message ->
                            latch.countDown()
                            executionOrder.add("root-handler-step-2")
                        }
                    },
                )

            val childSubscription =
                messageQueue.subscribe(
                    childTopic,
                    saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            Thread.sleep(100)
                            latch.countDown()
                            executionOrder.add("child-handler-step-1")
                        }
                        step { scope, message ->
                            latch.countDown()
                            executionOrder.add("child-handler-step-2")
                        }
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "Not everything completed correctly")
                Thread.sleep(100)

                assertEquals(4, executionOrder.size, "Not everything completed correctly")
                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "child-handler-step-1",
                        "child-handler-step-2",
                        "root-handler-step-2",
                    ),
                    executionOrder,
                    "Execution order obeys structured cooperation rules",
                )

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("EMITTED", "0", "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("SEEN", null, "child-handler"),
                        Triple("SUSPENDED", "0", "child-handler"),
                        Triple("SUSPENDED", "1", "child-handler"),
                        Triple("COMMITTED", "1", "child-handler"),
                        Triple("SUSPENDED", "1", "root-handler"),
                        Triple("COMMITTED", "1", "root-handler"),
                    ),
                    getEventSequence(),
                )
            } finally {
                rootSubscription.close()
                childSubscription.close()
            }
        }

        @Test
        fun `handler should not complete until handlers listening to emitted messages complete - depth 2`() {
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val latch = CountDownLatch(7) // 2 root steps + 3 child steps + 2 grandchild steps

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            val childPayload = JsonObject().put("from", "root-handler")
                            scope.launch(childTopic, childPayload).await().indefinitely()
                            latch.countDown()
                            executionOrder.add("root-handler-step-1")
                        }
                        step { scope, message ->
                            latch.countDown()
                            executionOrder.add("root-handler-step-2")
                        }
                    },
                )

            val childSubscription =
                messageQueue.subscribe(
                    childTopic,
                    saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            latch.countDown()
                            executionOrder.add("child-handler-step-1")
                        }
                        step { scope, message ->
                            val grandchildPayload = JsonObject().put("from", "child-handler")
                            scope.launch(grandchildTopic, grandchildPayload).await().indefinitely()
                            latch.countDown()
                            executionOrder.add("child-handler-step-2")
                        }
                        step { scope, message ->
                            latch.countDown()
                            executionOrder.add("child-handler-step-3")
                        }
                    },
                )

            val grandchildSubscription =
                messageQueue.subscribe(
                    grandchildTopic,
                    saga("grandchild-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            Thread.sleep(200)
                            executionOrder.add("grandchild-handler-step-1")
                            latch.countDown()
                        }
                        step { scope, message ->
                            latch.countDown()
                            executionOrder.add("grandchild-handler-step-2")
                        }
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")

                Thread.sleep(100)

                assertEquals(7, executionOrder.size, "Not everything completed correctly")
                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "child-handler-step-1",
                        "child-handler-step-2",
                        "grandchild-handler-step-1",
                        "grandchild-handler-step-2",
                        "child-handler-step-3",
                        "root-handler-step-2",
                    ),
                    executionOrder,
                    "Execution order obeys structured cooperation rules",
                )

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("EMITTED", "0", "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("SEEN", null, "child-handler"),
                        Triple("SUSPENDED", "0", "child-handler"),
                        Triple("EMITTED", "1", "child-handler"),
                        Triple("SUSPENDED", "1", "child-handler"),
                        Triple("SEEN", null, "grandchild-handler"),
                        Triple("SUSPENDED", "0", "grandchild-handler"),
                        Triple("SUSPENDED", "1", "grandchild-handler"),
                        Triple("COMMITTED", "1", "grandchild-handler"),
                        Triple("SUSPENDED", "2", "child-handler"),
                        Triple("COMMITTED", "2", "child-handler"),
                        Triple("SUSPENDED", "1", "root-handler"),
                        Triple("COMMITTED", "1", "root-handler"),
                    ),
                    getEventSequence(),
                )
            } finally {
                rootSubscription.close()
                childSubscription.close()
                grandchildSubscription.close()
            }
        }

        @Test
        fun `multiple handlers at same level should all complete before parent handler completes`() {
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val latch = CountDownLatch(6) // 2 root steps + 2 x 2 child steps

            val childTopic2 = "child-topic-2"

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            executionOrder.add("root-handler-step-1")
                            val childPayload1 = JsonObject().put("from", "root-handler")
                            val childPayload2 = JsonObject().put("from", "root-handler")

                            scope.launch(childTopic, childPayload1).await().indefinitely()
                            scope.launch(childTopic2, childPayload2).await().indefinitely()

                            latch.countDown()
                        }
                        step { scope, message ->
                            latch.countDown()
                            executionOrder.add("root-handler-step-2")
                        }
                    },
                )

            val childSubscription1 =
                messageQueue.subscribe(
                    childTopic,
                    saga("child-handler-1", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            Thread.sleep(100)
                            executionOrder.add("child-handler-1-step-1")
                            latch.countDown()
                        }
                        step { scope, message ->
                            executionOrder.add("child-handler-1-step-2")
                            latch.countDown()
                        }
                    },
                )

            val childSubscription2 =
                messageQueue.subscribe(
                    childTopic2,
                    saga("child-handler-2", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            executionOrder.add("child-handler-2-step-1")
                            latch.countDown()
                        }
                        step { scope, message ->
                            Thread.sleep(300)
                            executionOrder.add("child-handler-2-step-2")
                            latch.countDown()
                        }
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")

                Thread.sleep(100)

                assertEquals(6, executionOrder.size, "Not everything completed correctly")

                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "child-handler-1-step-1",
                        "child-handler-1-step-2",
                        "root-handler-step-2",
                    ),
                    executionOrder.keepOnlyPrefixedBy("root-handler", "child-handler-1"),
                    "Execution order obeys structured cooperation rules",
                )

                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "child-handler-2-step-1",
                        "child-handler-2-step-2",
                        "root-handler-step-2",
                    ),
                    executionOrder.keepOnlyPrefixedBy("root-handler", "child-handler-2"),
                    "Execution order obeys structured cooperation rules",
                )

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("EMITTED", "0", "root-handler"),
                        Triple("EMITTED", "0", "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("SEEN", null, "child-handler-1"),
                        Triple("SUSPENDED", "0", "child-handler-1"),
                        Triple("SUSPENDED", "1", "child-handler-1"),
                        Triple("COMMITTED", "1", "child-handler-1"),
                        Triple("SUSPENDED", "1", "root-handler"),
                        Triple("COMMITTED", "1", "root-handler"),
                    ),
                    getEventSequence().keepOnlyHandlers("root-handler", "child-handler-1"),
                )

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("EMITTED", "0", "root-handler"),
                        Triple("EMITTED", "0", "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("SEEN", null, "child-handler-2"),
                        Triple("SUSPENDED", "0", "child-handler-2"),
                        Triple("SUSPENDED", "1", "child-handler-2"),
                        Triple("COMMITTED", "1", "child-handler-2"),
                        Triple("SUSPENDED", "1", "root-handler"),
                        Triple("COMMITTED", "1", "root-handler"),
                    ),
                    getEventSequence().keepOnlyHandlers("root-handler", "child-handler-2"),
                )
            } finally {
                rootSubscription.close()
                childSubscription1.close()
                childSubscription2.close()
            }
        }

        @Test
        fun `parent should wait for multiple handlers listening to the same topic`() {
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val latch = CountDownLatch(6) // 2 root + 2 x 2 children listening to same topic

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            executionOrder.add("root-handler-step-1")
                            val childPayload = JsonObject().put("from", "root-handler")
                            scope.launch(childTopic, childPayload).await().indefinitely()
                            latch.countDown()
                        }
                        step { scope, message ->
                            latch.countDown()
                            executionOrder.add("root-handler-step-2")
                        }
                    },
                )

            val childSubscription1 =
                messageQueue.subscribe(
                    childTopic,
                    saga("child-handler-1", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            Thread.sleep(100)
                            executionOrder.add("child-handler-1-step-1")
                            latch.countDown()
                        }
                        step { scope, message ->
                            latch.countDown()
                            executionOrder.add("child-handler-1-step-2")
                        }
                    },
                )

            val childSubscription2 =
                messageQueue.subscribe(
                    childTopic,
                    saga("child-handler-2", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            Thread.sleep(300)
                            executionOrder.add("child-handler-2-step-1")
                            latch.countDown()
                        }
                        step { scope, message ->
                            latch.countDown()
                            executionOrder.add("child-handler-2-step-2")
                        }
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")

                Thread.sleep(100)

                assertEquals(6, executionOrder.size, "Not everything completed correctly")

                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "child-handler-1-step-1",
                        "child-handler-1-step-2",
                        "root-handler-step-2",
                    ),
                    executionOrder.keepOnlyPrefixedBy("root-handler", "child-handler-1"),
                    "Execution order obeys structured cooperation rules",
                )

                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "child-handler-2-step-1",
                        "child-handler-2-step-2",
                        "root-handler-step-2",
                    ),
                    executionOrder.keepOnlyPrefixedBy("root-handler", "child-handler-2"),
                    "Execution order obeys structured cooperation rules",
                )

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("EMITTED", "0", "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("SEEN", null, "child-handler-1"),
                        Triple("SUSPENDED", "0", "child-handler-1"),
                        Triple("SUSPENDED", "1", "child-handler-1"),
                        Triple("COMMITTED", "1", "child-handler-1"),
                        Triple("SUSPENDED", "1", "root-handler"),
                        Triple("COMMITTED", "1", "root-handler"),
                    ),
                    getEventSequence().keepOnlyHandlers("root-handler", "child-handler-1"),
                )

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("EMITTED", "0", "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("SEEN", null, "child-handler-2"),
                        Triple("SUSPENDED", "0", "child-handler-2"),
                        Triple("SUSPENDED", "1", "child-handler-2"),
                        Triple("COMMITTED", "1", "child-handler-2"),
                        Triple("SUSPENDED", "1", "root-handler"),
                        Triple("COMMITTED", "1", "root-handler"),
                    ),
                    getEventSequence().keepOnlyHandlers("root-handler", "child-handler-2"),
                )
            } finally {
                rootSubscription.close()
                childSubscription1.close()
                childSubscription2.close()
            }
        }
    }

    @Nested
    inner class Rollbacks {
        @Test
        fun `a handler failing in its first step should never emit what is in the step and not call rollback() (since the transaction wasn't committed)`() {
            val latch = CountDownLatch(1)
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val rootHandler = "root-handler"
            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga(rootHandler, handlerRegistry.eventLoopStrategy()) {
                        step(
                            { scope, message ->
                                val childPayload = JsonObject().put("from", rootHandler)
                                scope.launch(childTopic, childPayload).await().indefinitely()
                                executionOrder.add("root-handler-step-1")
                                latch.countDown()
                                throw RuntimeException("Simulated failure to test rollback")
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-handler-rollback-step-1")
                            },
                            handleChildFailures = { scope, message, throwable ->
                                executionOrder.add("root-handler-handleChildFailures-step-1")
                                throw throwable
                            },
                        )
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS))
                Thread.sleep(500)

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("ROLLING_BACK", "0", "root-handler"),
                        Triple("ROLLED_BACK", "Rollback of 0", "root-handler"),
                    ),
                    getEventSequence(),
                )
                assertEquals(executionOrder, listOf("root-handler-step-1"))
            } finally {
                rootSubscription.close()
            }
        }

        @Test
        fun `a handler failing in its second step should emit ROLLBACK_EMITTEDs for messages emitted in the first step, and then roll it back`() {
            val latch = CountDownLatch(3)
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val rootHandler = "root-handler"
            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga(rootHandler, handlerRegistry.eventLoopStrategy()) {
                        step(
                            { scope, message ->
                                val childPayload = JsonObject().put("from", rootHandler)
                                scope.launch(childTopic, childPayload).await().indefinitely()
                                executionOrder.add("root-handler-step-1")
                                latch.countDown()
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-handler-rollback-step-1")
                                latch.countDown()
                            },
                            handleChildFailures = { scope, message, throwable ->
                                executionOrder.add("root-handler-handleChildFailures-step-1")
                                throw throwable
                            },
                        )

                        step(
                            { scope, message ->
                                val childPayload = JsonObject().put("from", rootHandler)
                                scope.launch(childTopic, childPayload).await().indefinitely()
                                executionOrder.add("root-handler-step-2")
                                latch.countDown()
                                throw RuntimeException("Simulated failure to test rollback")
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-handler-rollback-step-2")
                            },
                            handleChildFailures = { scope, message, throwable ->
                                executionOrder.add("root-handler-handleChildFailures-step-2")
                                throw throwable
                            },
                        )
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(100, TimeUnit.SECONDS), "Not everything completed correctly")
                Thread.sleep(100)

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("EMITTED", "0", "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("ROLLING_BACK", "1", "root-handler"),
                        Triple(
                            "ROLLBACK_EMITTED",
                            "Rollback of 0 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple("SUSPENDED", "Rollback of 0", "root-handler"),
                        Triple("ROLLED_BACK", "Rollback of 0", "root-handler"),
                    ),
                    getEventSequence(),
                )

                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "root-handler-step-2",
                        "root-handler-rollback-step-1",
                    ),
                    executionOrder,
                )
            } finally {
                rootSubscription.close()
            }
        }

        @Test
        fun `when a child fails, rollbacks happen in reverse order`() {
            val latch = CountDownLatch(3)
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val rootHandler = "root-handler"
            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga(rootHandler, handlerRegistry.eventLoopStrategy()) {
                        step(
                            { scope, message ->
                                val childPayload = JsonObject().put("from", rootHandler)
                                scope.launch(childTopic, childPayload).await().indefinitely()
                                executionOrder.add("root-handler-step-1")
                                latch.countDown()
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-handler-rollback-step-1")
                                latch.countDown()
                            },
                            handleChildFailures = { scope, message, throwable ->
                                executionOrder.add("root-handler-handleChildFailures-step-1")
                                throw throwable
                            },
                        )
                    },
                )

            val childHandler = "child-handler"
            val childSubscription =
                messageQueue.subscribe(
                    childTopic,
                    saga(childHandler, handlerRegistry.eventLoopStrategy()) {
                        step(
                            { scope, message ->
                                executionOrder.add("child-handler-step-1")
                                latch.countDown()
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("child-handler-rollback-step-1")
                                latch.countDown()
                            },
                            handleChildFailures = { scope, message, throwable ->
                                executionOrder.add("child-handler-handleChildFailures-step-1")
                                throw throwable
                            },
                        )

                        step(
                            { scope, message ->
                                executionOrder.add("child-handler-step-2")
                                latch.countDown()
                                throw RuntimeException("Simulated failure to test rollback")
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("child-handler-rollback-step-2")
                            },
                            handleChildFailures = { scope, message, throwable ->
                                executionOrder.add("child-handler-handleChildFailures-step-2")
                                throw throwable
                            },
                        )
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "Not everything completed correctly")
                Thread.sleep(200)

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("EMITTED", "0", "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("SEEN", null, "child-handler"),
                        Triple("SUSPENDED", "0", "child-handler"),
                        Triple("ROLLING_BACK", "1", "child-handler"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "child-handler",
                        ),
                        Triple("SUSPENDED", "Rollback of 0", "child-handler"),
                        Triple("ROLLED_BACK", "Rollback of 0", "child-handler"),
                        Triple("ROLLING_BACK", "0", "root-handler"),
                        Triple(
                            "ROLLBACK_EMITTED",
                            "Rollback of 0 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple("SUSPENDED", "Rollback of 0", "root-handler"),
                        Triple("ROLLED_BACK", "Rollback of 0", "root-handler"),
                    ),
                    getEventSequence(),
                )

                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "child-handler-step-1",
                        "child-handler-step-2",
                        "child-handler-rollback-step-1",
                        "root-handler-handleChildFailures-step-1",
                        "root-handler-rollback-step-1",
                    ),
                    executionOrder,
                )
            } finally {
                childSubscription.close()
                rootSubscription.close()
            }
        }

        @Test
        fun `when a later step fails, previous emissions are rolled back`() {
            val latch = CountDownLatch(7)
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val rootHandler = "root-handler"
            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga(rootHandler, handlerRegistry.eventLoopStrategy()) {
                        step(
                            { scope, message ->
                                val childPayload = JsonObject().put("from", rootHandler)
                                scope.launch(childTopic, childPayload).await().indefinitely()
                                executionOrder.add("root-handler-step-1")
                                latch.countDown()
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-handler-rollback-step-1")
                                latch.countDown()
                            },
                            handleChildFailures = { scope, message, throwable ->
                                executionOrder.add("root-handler-handleChildFailures-step-1")
                                throw throwable
                            },
                        )

                        step(
                            { scope, message ->
                                executionOrder.add("root-handler-step-2")
                                latch.countDown()
                                throw RuntimeException("Simulated failure to test rollback")
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-handler-rollback-step-2")
                            },
                            handleChildFailures = { scope, message, throwable ->
                                executionOrder.add("root-handler-handleChildFailures-step-2")
                                throw throwable
                            },
                        )
                    },
                )

            val childHandler = "child-handler"
            val childSubscription =
                messageQueue.subscribe(
                    childTopic,
                    saga(childHandler, handlerRegistry.eventLoopStrategy()) {
                        step(
                            { scope, message ->
                                executionOrder.add("child-handler-step-1")
                                latch.countDown()
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("child-handler-rollback-step-1")
                                latch.countDown()
                            },
                            handleChildFailures = { scope, message, throwable ->
                                executionOrder.add("child-handler-handleChildFailures-step-1")
                                throw throwable
                            },
                        )

                        step(
                            { scope, message ->
                                executionOrder.add("child-handler-step-2")
                                latch.countDown()
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("child-handler-rollback-step-2")
                                latch.countDown()
                            },
                            handleChildFailures = { scope, message, throwable ->
                                executionOrder.add("child-handler-handleChildFailures-step-2")
                                throw throwable
                            },
                        )
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "Not everything completed correctly")
                Thread.sleep(200)

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("EMITTED", "0", "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("SEEN", null, "child-handler"),
                        Triple("SUSPENDED", "0", "child-handler"),
                        Triple("SUSPENDED", "1", "child-handler"),
                        Triple("COMMITTED", "1", "child-handler"),
                        Triple("ROLLING_BACK", "1", "root-handler"),
                        Triple(
                            "ROLLBACK_EMITTED",
                            "Rollback of 0 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple("ROLLING_BACK", null, "child-handler"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 1 (rolling back child scopes)",
                            "child-handler",
                        ),
                        Triple("SUSPENDED", "Rollback of 1", "child-handler"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "child-handler",
                        ),
                        Triple("SUSPENDED", "Rollback of 0", "child-handler"),
                        Triple("ROLLED_BACK", "Rollback of 0", "child-handler"),
                        Triple("SUSPENDED", "Rollback of 0", "root-handler"),
                        Triple("ROLLED_BACK", "Rollback of 0", "root-handler"),
                    ),
                    getEventSequence(),
                )

                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "child-handler-step-1",
                        "child-handler-step-2",
                        "root-handler-step-2",
                        "child-handler-rollback-step-2",
                        "child-handler-rollback-step-1",
                        "root-handler-rollback-step-1",
                    ),
                    executionOrder,
                )
            } finally {
                childSubscription.close()
                rootSubscription.close()
            }
        }

        @Test
        fun `rollbacks are well behaved n-deep`() {
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            // (2 root steps + 3 child1 steps + 2 child2 steps + 2 grandchild steps) * 2
            // rollbacks - 1 child2_step2_rollback + 1 root handleChildFailures
            val latch = CountDownLatch(18)

            val rootHandlerCoroutine =
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        { scope, message ->
                            latch.countDown()
                            executionOrder.add("root-handler-step-1")
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("root-handler-rollback-step-1")
                            latch.countDown()
                        },
                    )
                    step(
                        { scope, message ->
                            val childPayload = JsonObject().put("from", "root-handler")
                            scope.launch(childTopic, childPayload).await().indefinitely()
                            latch.countDown()
                            executionOrder.add("root-handler-step-2")
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("root-handler-rollback-step-2")
                            latch.countDown()
                        },
                        handleChildFailures = { scope, message, throwable ->
                            executionOrder.add("root-handler-handleChildFailures-step-2")
                            latch.countDown()
                            throw throwable
                        },
                    )
                }
            val rootSubscription = messageQueue.subscribe(rootTopic, rootHandlerCoroutine)

            val childHandler1Coroutine =
                saga("child-handler-1", handlerRegistry.eventLoopStrategy()) {
                    step(
                        { scope, message ->
                            latch.countDown()
                            executionOrder.add("child-handler-1-step-1")
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("child-handler-1-rollback-step-1")
                            latch.countDown()
                        },
                    )
                    step(
                        { scope, message ->
                            val grandchildPayload = JsonObject().put("from", "child-handler-1")
                            scope.launch(grandchildTopic, grandchildPayload).await().indefinitely()
                            latch.countDown()
                            executionOrder.add("child-handler-1-step-2")
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("child-handler-1-rollback-step-2")
                            latch.countDown()
                        },
                    )
                    step(
                        { scope, message ->
                            latch.countDown()
                            executionOrder.add("child-handler-1-step-3")
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("child-handler-1-rollback-step-3")
                            latch.countDown()
                        },
                    )
                }
            val childSubscription1 = messageQueue.subscribe(childTopic, childHandler1Coroutine)

            val childHandler2Coroutine =
                saga("child-handler-2", handlerRegistry.eventLoopStrategy()) {
                    step(
                        { scope, message ->
                            latch.countDown()
                            executionOrder.add("child-handler-2-step-1")
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("child-handler-2-rollback-step-1")
                            latch.countDown()
                        },
                    )
                    step { scope, message ->
                        latch.countDown()
                        executionOrder.add("child-handler-2-step-2")
                        throw RuntimeException("Simulated failure to test rollback")
                    }
                }
            val childSubscription2 = messageQueue.subscribe(childTopic, childHandler2Coroutine)

            val grandChildCoroutine =
                saga("grandchild-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        { scope, message ->
                            executionOrder.add("grandchild-handler-step-1")
                            latch.countDown()
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("grandchild-handler-rollback-step-1")
                            latch.countDown()
                        },
                    )
                    step(
                        { scope, message ->
                            latch.countDown()
                            Thread.sleep(200)
                            executionOrder.add("grandchild-handler-step-2")
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("grandchild-handler-rollback-step-2")
                            latch.countDown()
                        },
                    )
                }
            val grandchildSubscription =
                messageQueue.subscribe(grandchildTopic, grandChildCoroutine)

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(2, TimeUnit.SECONDS))

                Thread.sleep(750)

                assertEquals(18, executionOrder.size)

                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "root-handler-step-2",
                        "child-handler-1-step-1",
                        "child-handler-1-step-2",
                        "grandchild-handler-step-1",
                        "grandchild-handler-step-2",
                        "child-handler-1-step-3",
                        "root-handler-handleChildFailures-step-2",
                        "child-handler-1-rollback-step-3",
                        "grandchild-handler-rollback-step-2",
                        "grandchild-handler-rollback-step-1",
                        "child-handler-1-rollback-step-2",
                        "child-handler-1-rollback-step-1",
                        "root-handler-rollback-step-2",
                        "root-handler-rollback-step-1",
                    ),
                    executionOrder.keepOnlyPrefixedBy(
                        "root-handler",
                        "child-handler-1",
                        "grandchild-handler",
                    ),
                    "Execution order obeys structured cooperation rules",
                )
                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "root-handler-step-2",
                        "child-handler-2-step-1",
                        "child-handler-2-step-2",
                        "child-handler-2-rollback-step-1",
                        "root-handler-handleChildFailures-step-2",
                        "root-handler-rollback-step-2",
                        "root-handler-rollback-step-1",
                    ),
                    executionOrder.keepOnlyPrefixedBy("root-handler", "child-handler-2"),
                    "Execution order obeys structured cooperation rules",
                )

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("EMITTED", "1", "root-handler"),
                        Triple("SUSPENDED", "1", "root-handler"),
                        Triple("SEEN", null, "child-handler-1"),
                        Triple("SUSPENDED", "0", "child-handler-1"),
                        Triple("EMITTED", "1", "child-handler-1"),
                        Triple("SUSPENDED", "1", "child-handler-1"),
                        Triple("SEEN", null, "grandchild-handler"),
                        Triple("SUSPENDED", "0", "grandchild-handler"),
                        Triple("SUSPENDED", "1", "grandchild-handler"),
                        Triple("COMMITTED", "1", "grandchild-handler"),
                        Triple("SUSPENDED", "2", "child-handler-1"),
                        Triple("COMMITTED", "2", "child-handler-1"),
                        Triple("ROLLING_BACK", "1", "root-handler"),
                        Triple(
                            "ROLLBACK_EMITTED",
                            "Rollback of 1 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 1 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple("ROLLING_BACK", null, "child-handler-1"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 2 (rolling back child scopes)",
                            "child-handler-1",
                        ),
                        Triple("SUSPENDED", "Rollback of 2", "child-handler-1"),
                        Triple(
                            "ROLLBACK_EMITTED",
                            "Rollback of 1 (rolling back child scopes)",
                            "child-handler-1",
                        ),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 1 (rolling back child scopes)",
                            "child-handler-1",
                        ),
                        Triple("ROLLING_BACK", null, "grandchild-handler"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 1 (rolling back child scopes)",
                            "grandchild-handler",
                        ),
                        Triple("SUSPENDED", "Rollback of 1", "grandchild-handler"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "grandchild-handler",
                        ),
                        Triple("SUSPENDED", "Rollback of 0", "grandchild-handler"),
                        Triple("ROLLED_BACK", "Rollback of 0", "grandchild-handler"),
                        Triple("SUSPENDED", "Rollback of 1", "child-handler-1"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "child-handler-1",
                        ),
                        Triple("SUSPENDED", "Rollback of 0", "child-handler-1"),
                        Triple("ROLLED_BACK", "Rollback of 0", "child-handler-1"),
                        Triple("SUSPENDED", "Rollback of 1", "root-handler"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple("SUSPENDED", "Rollback of 0", "root-handler"),
                        Triple("ROLLED_BACK", "Rollback of 0", "root-handler"),
                    ),
                    getEventSequence()
                        .keepOnlyHandlers("root-handler", "child-handler-1", "grandchild-handler"),
                )

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("EMITTED", "1", "root-handler"),
                        Triple("SUSPENDED", "1", "root-handler"),
                        Triple("SEEN", null, "child-handler-2"),
                        Triple("SUSPENDED", "0", "child-handler-2"),
                        Triple("ROLLING_BACK", "1", "child-handler-2"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "child-handler-2",
                        ),
                        Triple("SUSPENDED", "Rollback of 0", "child-handler-2"),
                        Triple("ROLLED_BACK", "Rollback of 0", "child-handler-2"),
                        Triple("ROLLING_BACK", "1", "root-handler"),
                        Triple(
                            "ROLLBACK_EMITTED",
                            "Rollback of 1 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 1 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple("SUSPENDED", "Rollback of 1", "root-handler"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple("SUSPENDED", "Rollback of 0", "root-handler"),
                        Triple("ROLLED_BACK", "Rollback of 0", "root-handler"),
                    ),
                    getEventSequence().keepOnlyHandlers("root-handler", "child-handler-2"),
                )

                val childHandler2RollingBackExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLING_BACK", "child-handler-2")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                            "java.lang.RuntimeException",
                            childHandler2Coroutine.identifier.asSource(),
                        )
                    ),
                    childHandler2RollingBackExceptions,
                )

                val rootHandlerRollingBackExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLING_BACK", "root-handler")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                            rootHandlerCoroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                                    "java.lang.RuntimeException",
                                    childHandler2Coroutine.identifier.asSource(),
                                )
                            ),
                        )
                    ),
                    rootHandlerRollingBackExceptions,
                )

                val rootHandlerRollbackEmittedExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLBACK_EMITTED", "root-handler")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                            rootHandlerCoroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                                    "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                                    rootHandlerCoroutine.identifier.asSource(),
                                    listOf(
                                        CooperationExceptionData(
                                            "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                                            "java.lang.RuntimeException",
                                            childHandler2Coroutine.identifier.asSource(),
                                        )
                                    ),
                                )
                            ),
                        )
                    ),
                    rootHandlerRollbackEmittedExceptions,
                )

                val childHandler1RollingBackExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLING_BACK", "child-handler-1")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                            rootHandlerCoroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                                    "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                                    rootHandlerCoroutine.identifier.asSource(),
                                    listOf(
                                        CooperationExceptionData(
                                            "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                                            "java.lang.RuntimeException",
                                            childHandler2Coroutine.identifier.asSource(),
                                        )
                                    ),
                                )
                            ),
                        )
                    ),
                    childHandler1RollingBackExceptions,
                )

                val childHandler1RollbackEmittedExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLBACK_EMITTED", "child-handler-1")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${childHandler1Coroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                            childHandler1Coroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                                    "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                                    rootHandlerCoroutine.identifier.asSource(),
                                    listOf(
                                        CooperationExceptionData(
                                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                                            "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                                            rootHandlerCoroutine.identifier.asSource(),
                                            listOf(
                                                CooperationExceptionData(
                                                    "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                                                    "java.lang.RuntimeException",
                                                    childHandler2Coroutine.identifier.asSource(),
                                                )
                                            ),
                                        )
                                    ),
                                )
                            ),
                        )
                    ),
                    childHandler1RollbackEmittedExceptions,
                )

                val grandChildHandlerRollingBackExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLING_BACK", "grandchild-handler")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${childHandler1Coroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                            childHandler1Coroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                                    "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                                    rootHandlerCoroutine.identifier.asSource(),
                                    listOf(
                                        CooperationExceptionData(
                                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                                            "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                                            rootHandlerCoroutine.identifier.asSource(),
                                            listOf(
                                                CooperationExceptionData(
                                                    "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                                                    "java.lang.RuntimeException",
                                                    childHandler2Coroutine.identifier.asSource(),
                                                )
                                            ),
                                        )
                                    ),
                                )
                            ),
                        )
                    ),
                    grandChildHandlerRollingBackExceptions,
                )
            } finally {
                rootSubscription.close()
                childSubscription1.close()
                childSubscription2.close()
                grandchildSubscription.close()
            }
        }

        // We're only including the rollback/handlerChildFailure lambdas that actually get called
        // here, for brevity
        @Test
        fun `failed rollbacks are well behaved n-deep`() {
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val latch = CountDownLatch(16)

            val rootHandlerCoroutine =
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step({ scope, message ->
                        latch.countDown()
                        executionOrder.add("root-handler-step-1")
                    })
                    step(
                        { scope, message ->
                            val childPayload = JsonObject().put("from", "root-handler")
                            scope.launch(childTopic, childPayload).await().indefinitely()
                            latch.countDown()
                            executionOrder.add("root-handler-step-2")
                        },
                        handleChildFailures = { scope, message, throwable ->
                            // This will be called twice - once for the "normal" exception that
                            // starts
                            // the rollback process,
                            // and then for the additional exception that get's thrown during the
                            // rollback process
                            executionOrder.add("root-handler-handleChildFailures-step-2")
                            latch.countDown()
                            throw throwable
                        },
                    )
                }

            val rootSubscription = messageQueue.subscribe(rootTopic, rootHandlerCoroutine)

            val childHandler1Coroutine =
                saga("child-handler-1", handlerRegistry.eventLoopStrategy()) {
                    step({ scope, message ->
                        executionOrder.add("child-handler-1-step-1")
                        latch.countDown()
                    })
                    step(
                        { scope, message ->
                            val grandchildPayload = JsonObject().put("from", "child-handler-1")
                            scope.launch(grandchildTopic, grandchildPayload).await().indefinitely()
                            executionOrder.add("child-handler-1-step-2")
                            latch.countDown()
                        },
                        handleChildFailures = { scope, message, throwable ->
                            executionOrder.add("child-handler-1-handleChildFailures-step-2")
                            latch.countDown()
                            throw throwable
                        },
                    )
                    step(
                        { scope, message ->
                            executionOrder.add("child-handler-1-step-3")
                            latch.countDown()
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("child-handler-1-rollback-step-3")
                            latch.countDown()
                        },
                    )
                }
            val childSubscription1 = messageQueue.subscribe(childTopic, childHandler1Coroutine)

            val childHandler2Coroutine =
                saga("child-handler-2", handlerRegistry.eventLoopStrategy()) {
                    step(
                        { scope, message ->
                            executionOrder.add("child-handler-2-step-1")
                            latch.countDown()
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("child-handler-2-rollback-step-1")
                            latch.countDown()
                        },
                    )
                    step({ scope, message ->
                        executionOrder.add("child-handler-2-step-2")
                        latch.countDown()
                        throw RuntimeException("Simulated failure to test rollback")
                    })
                }
            val childSubscription2 = messageQueue.subscribe(childTopic, childHandler2Coroutine)

            val grandChildCoroutine =
                saga("grandchild-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        { scope, message ->
                            executionOrder.add("grandchild-handler-step-1")
                            latch.countDown()
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("grandchild-handler-rollback-step-1")
                            latch.countDown()
                            throw IllegalStateException("Rollback failure")
                        },
                    )
                    step(
                        { scope, message ->
                            Thread.sleep(200)
                            executionOrder.add("grandchild-handler-step-2")
                            latch.countDown()
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("grandchild-handler-rollback-step-2")
                            latch.countDown()
                        },
                    )
                }
            val grandchildSubscription =
                messageQueue.subscribe(grandchildTopic, grandChildCoroutine)

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(2, TimeUnit.SECONDS), "Latch has count ${latch.count}")

                Thread.sleep(750)

                assertEquals(16, executionOrder.size)

                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "root-handler-step-2",
                        "child-handler-1-step-1",
                        "child-handler-1-step-2",
                        "grandchild-handler-step-1",
                        "grandchild-handler-step-2",
                        "child-handler-1-step-3",
                        "root-handler-handleChildFailures-step-2",
                        "child-handler-1-rollback-step-3",
                        "grandchild-handler-rollback-step-2",
                        "grandchild-handler-rollback-step-1",
                        "child-handler-1-handleChildFailures-step-2",
                        "root-handler-handleChildFailures-step-2",
                    ),
                    executionOrder.keepOnlyPrefixedBy(
                        "root-handler",
                        "child-handler-1",
                        "grandchild-handler",
                    ),
                    "Execution order obeys structured cooperation rules",
                )
                assertEquals(
                    listOf(
                        "root-handler-step-1",
                        "root-handler-step-2",
                        "child-handler-2-step-1",
                        "child-handler-2-step-2",
                        "child-handler-2-rollback-step-1",
                        "root-handler-handleChildFailures-step-2",
                        "root-handler-handleChildFailures-step-2",
                    ),
                    executionOrder.keepOnlyPrefixedBy("root-handler", "child-handler-2"),
                    "Execution order obeys structured cooperation rules",
                )

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("EMITTED", "1", "root-handler"),
                        Triple("SUSPENDED", "1", "root-handler"),
                        Triple("SEEN", null, "child-handler-1"),
                        Triple("SUSPENDED", "0", "child-handler-1"),
                        Triple("EMITTED", "1", "child-handler-1"),
                        Triple("SUSPENDED", "1", "child-handler-1"),
                        Triple("SEEN", null, "grandchild-handler"),
                        Triple("SUSPENDED", "0", "grandchild-handler"),
                        Triple("SUSPENDED", "1", "grandchild-handler"),
                        Triple("COMMITTED", "1", "grandchild-handler"),
                        Triple("SUSPENDED", "2", "child-handler-1"),
                        Triple("COMMITTED", "2", "child-handler-1"),
                        Triple("ROLLING_BACK", "1", "root-handler"),
                        Triple(
                            "ROLLBACK_EMITTED",
                            "Rollback of 1 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 1 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple("ROLLING_BACK", null, "child-handler-1"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 2 (rolling back child scopes)",
                            "child-handler-1",
                        ),
                        Triple("SUSPENDED", "Rollback of 2", "child-handler-1"),
                        Triple(
                            "ROLLBACK_EMITTED",
                            "Rollback of 1 (rolling back child scopes)",
                            "child-handler-1",
                        ),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 1 (rolling back child scopes)",
                            "child-handler-1",
                        ),
                        Triple("ROLLING_BACK", null, "grandchild-handler"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 1 (rolling back child scopes)",
                            "grandchild-handler",
                        ),
                        Triple("SUSPENDED", "Rollback of 1", "grandchild-handler"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "grandchild-handler",
                        ),
                        Triple("ROLLBACK_FAILED", "Rollback of 0", "grandchild-handler"),
                        Triple(
                            "ROLLBACK_FAILED",
                            "Rollback of 1 (rolling back child scopes)",
                            "child-handler-1",
                        ),
                        Triple(
                            "ROLLBACK_FAILED",
                            "Rollback of 1 (rolling back child scopes)",
                            "root-handler",
                        ),
                    ),
                    getEventSequence()
                        .keepOnlyHandlers("root-handler", "child-handler-1", "grandchild-handler"),
                )

                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("EMITTED", "1", "root-handler"),
                        Triple("SUSPENDED", "1", "root-handler"),
                        Triple("SEEN", null, "child-handler-2"),
                        Triple("SUSPENDED", "0", "child-handler-2"),
                        Triple("ROLLING_BACK", "1", "child-handler-2"),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "child-handler-2",
                        ),
                        Triple("SUSPENDED", "Rollback of 0", "child-handler-2"),
                        Triple("ROLLED_BACK", "Rollback of 0", "child-handler-2"),
                        Triple("ROLLING_BACK", "1", "root-handler"),
                        Triple(
                            "ROLLBACK_EMITTED",
                            "Rollback of 1 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 1 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple(
                            "ROLLBACK_FAILED",
                            "Rollback of 1 (rolling back child scopes)",
                            "root-handler",
                        ),
                    ),
                    getEventSequence().keepOnlyHandlers("root-handler", "child-handler-2"),
                )

                val childHandler2RollingBackExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLING_BACK", "child-handler-2")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                            "java.lang.RuntimeException",
                            childHandler2Coroutine.identifier.asSource(),
                        )
                    ),
                    childHandler2RollingBackExceptions,
                )

                val rootHandlerRollingBackExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLING_BACK", "root-handler")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                            rootHandlerCoroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                                    "java.lang.RuntimeException",
                                    childHandler2Coroutine.identifier.asSource(),
                                )
                            ),
                        )
                    ),
                    rootHandlerRollingBackExceptions,
                )

                val rootHandlerRollbackEmittedExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLBACK_EMITTED", "root-handler")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                            rootHandlerCoroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                                    "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                                    rootHandlerCoroutine.identifier.asSource(),
                                    listOf(
                                        CooperationExceptionData(
                                            "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                                            "java.lang.RuntimeException",
                                            childHandler2Coroutine.identifier.asSource(),
                                        )
                                    ),
                                )
                            ),
                        )
                    ),
                    rootHandlerRollbackEmittedExceptions,
                )

                val childHandler1RollingBackExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLING_BACK", "child-handler-1")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                            rootHandlerCoroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                                    "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                                    rootHandlerCoroutine.identifier.asSource(),
                                    listOf(
                                        CooperationExceptionData(
                                            "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                                            "java.lang.RuntimeException",
                                            childHandler2Coroutine.identifier.asSource(),
                                        )
                                    ),
                                )
                            ),
                        )
                    ),
                    childHandler1RollingBackExceptions,
                )

                val childHandler1RollbackEmittedExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLBACK_EMITTED", "child-handler-1")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${childHandler1Coroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                            childHandler1Coroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                                    "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                                    rootHandlerCoroutine.identifier.asSource(),
                                    listOf(
                                        CooperationExceptionData(
                                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                                            "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                                            rootHandlerCoroutine.identifier.asSource(),
                                            listOf(
                                                CooperationExceptionData(
                                                    "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                                                    "java.lang.RuntimeException",
                                                    childHandler2Coroutine.identifier.asSource(),
                                                )
                                            ),
                                        )
                                    ),
                                )
                            ),
                        )
                    ),
                    childHandler1RollbackEmittedExceptions,
                )

                val grandChildHandlerRollingBackExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLING_BACK", "grandchild-handler")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${childHandler1Coroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                            childHandler1Coroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                                    "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                                    rootHandlerCoroutine.identifier.asSource(),
                                    listOf(
                                        CooperationExceptionData(
                                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                                            "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                                            rootHandlerCoroutine.identifier.asSource(),
                                            listOf(
                                                CooperationExceptionData(
                                                    "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                                                    "java.lang.RuntimeException",
                                                    childHandler2Coroutine.identifier.asSource(),
                                                )
                                            ),
                                        )
                                    ),
                                )
                            ),
                        )
                    ),
                    grandChildHandlerRollingBackExceptions,
                )

                val grandChildHandlerRollbackFailedExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLBACK_FAILED", "grandchild-handler")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${grandChildCoroutine.identifier.asSource()}] java.lang.IllegalStateException: Rollback failure",
                            "java.lang.IllegalStateException",
                            grandChildCoroutine.identifier.asSource(),
                        )
                    ),
                    grandChildHandlerRollbackFailedExceptions,
                )

                val childHandler1RollbackFailedExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLBACK_FAILED", "child-handler-1")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${childHandler1Coroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRollbackFailedException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ChildRollbackFailedException",
                            childHandler1Coroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${grandChildCoroutine.identifier.asSource()}] java.lang.IllegalStateException: Rollback failure",
                                    "java.lang.IllegalStateException",
                                    grandChildCoroutine.identifier.asSource(),
                                ),
                                CooperationExceptionData(
                                    "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                                    "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                                    rootHandlerCoroutine.identifier.asSource(),
                                    listOf(
                                        CooperationExceptionData(
                                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                                            "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                                            rootHandlerCoroutine.identifier.asSource(),
                                            listOf(
                                                CooperationExceptionData(
                                                    "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                                                    "java.lang.RuntimeException",
                                                    childHandler2Coroutine.identifier.asSource(),
                                                )
                                            ),
                                        )
                                    ),
                                ),
                            ),
                        )
                    ),
                    childHandler1RollbackFailedExceptions,
                )

                val rootHandlerRollbackFailedExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLBACK_FAILED", "root-handler")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRollbackFailedException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ChildRollbackFailedException",
                            rootHandlerCoroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${childHandler1Coroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRollbackFailedException: <no message>",
                                    "io.github.gabrielshanahan.structmess.coroutine.ChildRollbackFailedException",
                                    childHandler1Coroutine.identifier.asSource(),
                                    listOf(
                                        CooperationExceptionData(
                                            "[${grandChildCoroutine.identifier.asSource()}] java.lang.IllegalStateException: Rollback failure",
                                            "java.lang.IllegalStateException",
                                            grandChildCoroutine.identifier.asSource(),
                                        ),
                                        CooperationExceptionData(
                                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException: <no message>",
                                            "io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException",
                                            rootHandlerCoroutine.identifier.asSource(),
                                            listOf(
                                                CooperationExceptionData(
                                                    "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                                                    "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                                                    rootHandlerCoroutine.identifier.asSource(),
                                                    listOf(
                                                        CooperationExceptionData(
                                                            "[${childHandler2Coroutine.identifier.asSource()}] java.lang.RuntimeException: Simulated failure to test rollback",
                                                            "java.lang.RuntimeException",
                                                            childHandler2Coroutine.identifier
                                                                .asSource(),
                                                        )
                                                    ),
                                                )
                                            ),
                                        ),
                                    ),
                                )
                            ),
                        )
                    ),
                    rootHandlerRollbackFailedExceptions,
                )
            } finally {
                rootSubscription.close()
                childSubscription1.close()
                childSubscription2.close()
                grandchildSubscription.close()
            }
        }

        @Nested
        inner class HandleChildFailures {

            @Test
            fun `when stuff is emitted in handleChildFailures and then a rollback happens, all things that haven't already been rolled back are rolled back`() {
                val latch = CountDownLatch(1)
                val executionOrder = Collections.synchronizedList(mutableListOf<String>())

                val rootHandler = "root-handler"
                val rootSubscription =
                    messageQueue.subscribe(
                        rootTopic,
                        saga(rootHandler, handlerRegistry.eventLoopStrategy()) {
                            step(
                                { scope, message ->
                                    val childPayload =
                                        JsonObject()
                                            .put("from", rootHandler)
                                            .put("phase", "original")
                                    scope.launch(childTopic, childPayload).await().indefinitely()
                                    executionOrder.add("root-handler")
                                },
                                rollback = { scope, message, throwable ->
                                    executionOrder.add("root-handler-rollback")
                                    latch.countDown()
                                },
                                handleChildFailures = { scope, message, throwable ->
                                    executionOrder.add("root-handler-handleChildFailures")
                                    if (scope.context.has(TriedAgainKey)) {
                                        throw throwable
                                    } else {
                                        scope.context += TriedAgainValue
                                        val childPayload =
                                            JsonObject()
                                                .put("from", rootHandler)
                                                .put("phase", "retry")
                                        scope
                                            .launch(childTopic, childPayload)
                                            .await()
                                            .indefinitely()
                                    }
                                },
                            )
                        },
                    )

                val childHandler1 = "child-handler-1"
                val childSubscription1 =
                    messageQueue.subscribe(
                        childTopic,
                        saga(childHandler1, handlerRegistry.eventLoopStrategy()) {
                            step(
                                { scope, message ->
                                    val phase = message.payload.getString("phase")
                                    executionOrder.add("child-handler-1-$phase")
                                    throw RuntimeException("Simulated failure to test rollback")
                                },
                                rollback = { scope, message, throwable ->
                                    val phase = message.payload.getString("phase")
                                    executionOrder.add("child-handler-1-rollback-$phase")
                                },
                                handleChildFailures = { scope, message, throwable ->
                                    val phase = message.payload.getString("phase")
                                    executionOrder.add("child-handler-1-handleChildFailures-$phase")
                                    throw throwable
                                },
                            )
                        },
                    )

                val childHandler2 = "child-handler-2"
                val childSubscription2 =
                    messageQueue.subscribe(
                        childTopic,
                        saga(childHandler2, handlerRegistry.eventLoopStrategy()) {
                            step(
                                { scope, message ->
                                    val phase = message.payload.getString("phase")
                                    executionOrder.add("child-handler-2-$phase")
                                },
                                rollback = { scope, message, throwable ->
                                    val phase = message.payload.getString("phase")
                                    executionOrder.add("child-handler-2-rollback-$phase")
                                },
                                handleChildFailures = { scope, message, throwable ->
                                    val phase = message.payload.getString("phase")
                                    executionOrder.add("child-handler-2-handleChildFailures-$phase")
                                    throw throwable
                                },
                            )
                        },
                    )

                try {
                    val rootPayload = JsonObject().put("initial", "true")
                    pool
                        .withTransaction { connection ->
                            messageQueue.launch(connection, rootTopic, rootPayload)
                        }
                        .await()
                        .indefinitely()

                    assertTrue(
                        latch.await(1, TimeUnit.SECONDS),
                        "Not everything completed correctly",
                    )
                    Thread.sleep(200)

                    assertEquals(
                        listOf(
                            Triple("EMITTED", null, null),
                            Triple("SEEN", null, "root-handler"),
                            Triple("EMITTED", "0", "root-handler"),
                            Triple("SUSPENDED", "0", "root-handler"),
                            Triple("SEEN", null, "child-handler-1"),
                            Triple("ROLLING_BACK", "0", "child-handler-1"),
                            Triple("ROLLED_BACK", "Rollback of 0", "child-handler-1"),
                            Triple("EMITTED", "0", "root-handler"),
                            Triple("SUSPENDED", "0", "root-handler"),
                            Triple("SEEN", null, "child-handler-1"),
                            Triple("ROLLING_BACK", "0", "child-handler-1"),
                            Triple("ROLLED_BACK", "Rollback of 0", "child-handler-1"),
                            Triple("ROLLING_BACK", "0", "root-handler"),
                            Triple(
                                "ROLLBACK_EMITTED",
                                "Rollback of 0 (rolling back child scopes)",
                                "root-handler",
                            ),
                            Triple(
                                "ROLLBACK_EMITTED",
                                "Rollback of 0 (rolling back child scopes)",
                                "root-handler",
                            ),
                            Triple(
                                "SUSPENDED",
                                "Rollback of 0 (rolling back child scopes)",
                                "root-handler",
                            ),
                            Triple("SUSPENDED", "Rollback of 0", "root-handler"),
                            Triple("ROLLED_BACK", "Rollback of 0", "root-handler"),
                        ),
                        getEventSequence().keepOnlyHandlers("root-handler", "child-handler-1"),
                    )

                    assertEquals(
                        listOf(
                            Triple("EMITTED", null, null),
                            Triple("SEEN", null, "root-handler"),
                            Triple("EMITTED", "0", "root-handler"),
                            Triple("SUSPENDED", "0", "root-handler"),
                            Triple("SEEN", null, "child-handler-2"),
                            Triple("SUSPENDED", "0", "child-handler-2"),
                            Triple("COMMITTED", "0", "child-handler-2"),
                            Triple("EMITTED", "0", "root-handler"),
                            Triple("SUSPENDED", "0", "root-handler"),
                            Triple("SEEN", null, "child-handler-2"),
                            Triple("SUSPENDED", "0", "child-handler-2"),
                            Triple("COMMITTED", "0", "child-handler-2"),
                            Triple("ROLLING_BACK", "0", "root-handler"),
                            Triple(
                                "ROLLBACK_EMITTED",
                                "Rollback of 0 (rolling back child scopes)",
                                "root-handler",
                            ),
                            Triple(
                                "ROLLBACK_EMITTED",
                                "Rollback of 0 (rolling back child scopes)",
                                "root-handler",
                            ),
                            Triple(
                                "SUSPENDED",
                                "Rollback of 0 (rolling back child scopes)",
                                "root-handler",
                            ),
                            Triple("ROLLING_BACK", null, "child-handler-2"),
                            Triple("ROLLING_BACK", null, "child-handler-2"),
                            Triple(
                                "SUSPENDED",
                                "Rollback of 0 (rolling back child scopes)",
                                "child-handler-2",
                            ),
                            Triple("SUSPENDED", "Rollback of 0", "child-handler-2"),
                            Triple("ROLLED_BACK", "Rollback of 0", "child-handler-2"),
                            Triple(
                                "SUSPENDED",
                                "Rollback of 0 (rolling back child scopes)",
                                "child-handler-2",
                            ),
                            Triple("SUSPENDED", "Rollback of 0", "child-handler-2"),
                            Triple("ROLLED_BACK", "Rollback of 0", "child-handler-2"),
                            Triple("SUSPENDED", "Rollback of 0", "root-handler"),
                            Triple("ROLLED_BACK", "Rollback of 0", "root-handler"),
                        ),
                        getEventSequence().keepOnlyHandlers("root-handler", "child-handler-2"),
                    )

                    assertEquals(
                        listOf(
                            "root-handler",
                            "child-handler-1-original",
                            "root-handler-handleChildFailures",
                            "child-handler-1-retry",
                            "root-handler-handleChildFailures",
                            "root-handler-rollback",
                        ),
                        executionOrder.keepOnlyPrefixedBy("root-handler", "child-handler-1"),
                    )

                    assertEquals(
                        listOf(
                            "root-handler",
                            "child-handler-2-original",
                            "root-handler-handleChildFailures",
                            "child-handler-2-retry",
                            "root-handler-handleChildFailures",
                            // This ordering (first rolling back the original, and then the retry)
                            // is actually not guaranteed in the general case - here, it's only
                            // the case because we're running a single instance of the handler,
                            // so events are processed in the order they are created. However,
                            // in general, with multiple handler instances running, no guarantees
                            // can be made about the order in which the following two lines would
                            // appear.
                            "child-handler-2-rollback-original",
                            "child-handler-2-rollback-retry",
                            "root-handler-rollback",
                        ),
                        executionOrder.keepOnlyPrefixedBy("root-handler", "child-handler-2"),
                    )
                } finally {
                    childSubscription1.close()
                    childSubscription2.close()
                    rootSubscription.close()
                }
            }
        }

        @Nested
        inner class Cancellations {

            @Test
            fun `cancellation works`() {
                val executionOrder = Collections.synchronizedList(mutableListOf<String>())

                val latch = CountDownLatch(3)
                val childIsExecuting = CountDownLatch(1)
                val cancellation = CountDownLatch(1)

                val rootHandlerCoroutine =
                    saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                        step(
                            { scope, message ->
                                val childPayload = JsonObject().put("from", "root-handler")
                                scope.launch(childTopic, childPayload).await().indefinitely()
                                latch.countDown()
                                executionOrder.add("root-handler-step-1")
                            },
                            rollback = { scope, message, throwable ->
                                latch.countDown()
                                executionOrder.add("root-handler-rollback-step-1")
                            },
                        )
                    }
                val rootSubscription = messageQueue.subscribe(rootTopic, rootHandlerCoroutine)

                val childHandlerCoroutine =
                    saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            childIsExecuting.countDown()
                            Thread.sleep(100)
                            latch.countDown()
                            executionOrder.add("child-handler-step-1")
                            cancellation.await()
                        }
                    }
                val childSubscription = messageQueue.subscribe(childTopic, childHandlerCoroutine)

                try {
                    val rootPayload = JsonObject().put("initial", "true")
                    val cooperationRoot =
                        pool
                            .withTransaction { connection ->
                                messageQueue.launch(connection, rootTopic, rootPayload)
                            }
                            .await()
                            .indefinitely()

                    childIsExecuting.await()
                    pool
                        .withTransaction { connection ->
                            structuredCooperationManager.cancel(
                                connection,
                                cooperationRoot.cooperationScopeIdentifier,
                                "master-system",
                                "feelz",
                            )
                        }
                        .await()
                        .indefinitely()
                    cancellation.countDown()

                    assertTrue(latch.await(1, TimeUnit.SECONDS), "Latch count is ${latch.count}")
                    Thread.sleep(100)

                    assertEquals(3, executionOrder.size, "Not everything completed correctly")
                    assertEquals(
                        listOf(
                            "root-handler-step-1",
                            "child-handler-step-1",
                            "root-handler-rollback-step-1",
                        ),
                        executionOrder,
                        "Execution order obeys structured cooperation rules",
                    )

                    assertEquals(
                        listOf(
                            Triple("EMITTED", null, null),
                            Triple("SEEN", null, "root-handler"),
                            Triple("EMITTED", "0", "root-handler"),
                            Triple("SUSPENDED", "0", "root-handler"),
                            Triple("SEEN", null, "child-handler"),
                            Triple("CANCELLATION_REQUESTED", null, null),
                            Triple("ROLLING_BACK", "0", "child-handler"),
                            Triple("ROLLED_BACK", "Rollback of 0", "child-handler"),
                            Triple("ROLLING_BACK", "0", "root-handler"),
                            Triple(
                                "ROLLBACK_EMITTED",
                                "Rollback of 0 (rolling back child scopes)",
                                "root-handler",
                            ),
                            Triple(
                                "SUSPENDED",
                                "Rollback of 0 (rolling back child scopes)",
                                "root-handler",
                            ),
                            Triple("SUSPENDED", "Rollback of 0", "root-handler"),
                            Triple("ROLLED_BACK", "Rollback of 0", "root-handler"),
                        ),
                        getEventSequence(),
                    )

                    val cancellationExceptions =
                        pool
                            .withConnection { connection ->
                                fetchExceptions(connection, "CANCELLATION_REQUESTED", null)
                            }
                            .await()
                            .indefinitely()

                    assertEquivalent(
                        listOf(
                            CooperationExceptionData(
                                "[master-system] io.github.gabrielshanahan.structmess.coroutine.CancellationRequestedException: feelz",
                                "io.github.gabrielshanahan.structmess.coroutine.CancellationRequestedException",
                                "master-system",
                            )
                        ),
                        cancellationExceptions,
                    )

                    val childHandlerRollingBackExceptions =
                        pool
                            .withConnection { connection ->
                                fetchExceptions(connection, "ROLLING_BACK", "child-handler")
                            }
                            .await()
                            .indefinitely()

                    assertEquivalent(
                        listOf(
                            CooperationExceptionData(
                                "[${childHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.GaveUpException: <no message>",
                                "io.github.gabrielshanahan.structmess.coroutine.GaveUpException",
                                childHandlerCoroutine.identifier.asSource(),
                                listOf(
                                    CooperationExceptionData(
                                        "[master-system] io.github.gabrielshanahan.structmess.coroutine.CancellationRequestedException: feelz",
                                        "io.github.gabrielshanahan.structmess.coroutine.CancellationRequestedException",
                                        "master-system",
                                    )
                                ),
                            )
                        ),
                        childHandlerRollingBackExceptions,
                    )

                    val rootHandlerRollingBackExceptions =
                        pool
                            .withConnection { connection ->
                                fetchExceptions(connection, "ROLLING_BACK", "root-handler")
                            }
                            .await()
                            .indefinitely()

                    assertEquivalent(
                        listOf(
                            CooperationExceptionData(
                                "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.GaveUpException: <no message>",
                                "io.github.gabrielshanahan.structmess.coroutine.GaveUpException",
                                "${rootHandlerCoroutine.identifier.asSource()}",
                                listOf(
                                    CooperationExceptionData(
                                        "[master-system] io.github.gabrielshanahan.structmess.coroutine.CancellationRequestedException: feelz",
                                        "io.github.gabrielshanahan.structmess.coroutine.CancellationRequestedException",
                                        "master-system",
                                    )
                                ),
                            )
                        ),
                        rootHandlerRollingBackExceptions,
                    )
                } finally {
                    rootSubscription.close()
                    childSubscription.close()
                }
            }

            @Test
            fun `cancellation after everything has finished running has no effect`() {
                val executionOrder = Collections.synchronizedList(mutableListOf<String>())

                val latch = CountDownLatch(2)

                val rootSubscription =
                    messageQueue.subscribe(
                        rootTopic,
                        saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                            step { scope, message ->
                                val childPayload = JsonObject().put("from", "root-handler")
                                scope.launch(childTopic, childPayload).await().indefinitely()
                                latch.countDown()
                                executionOrder.add("root-handler-step-1")
                            }
                        },
                    )

                val childSubscription =
                    messageQueue.subscribe(
                        childTopic,
                        saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                            step { scope, message ->
                                Thread.sleep(100)
                                latch.countDown()
                                executionOrder.add("child-handler-step-1")
                            }
                        },
                    )

                try {
                    val rootPayload = JsonObject().put("initial", "true")
                    val cooperationRoot =
                        pool
                            .withTransaction { connection ->
                                messageQueue.launch(connection, rootTopic, rootPayload)
                            }
                            .await()
                            .indefinitely()

                    assertTrue(
                        latch.await(1, TimeUnit.SECONDS),
                        "Not everything completed correctly",
                    )
                    Thread.sleep(100)

                    pool
                        .withTransaction { connection ->
                            structuredCooperationManager.cancel(
                                connection,
                                cooperationRoot.cooperationScopeIdentifier,
                                "master-system",
                                "feelz",
                            )
                        }
                        .await()
                        .indefinitely()

                    assertEquals(2, executionOrder.size, "Not everything completed correctly")
                    assertEquals(
                        listOf("root-handler-step-1", "child-handler-step-1"),
                        executionOrder,
                        "Execution order obeys structured cooperation rules",
                    )

                    assertEquals(
                        listOf(
                            Triple("EMITTED", null, null),
                            Triple("SEEN", null, "root-handler"),
                            Triple("EMITTED", "0", "root-handler"),
                            Triple("SUSPENDED", "0", "root-handler"),
                            Triple("SEEN", null, "child-handler"),
                            Triple("SUSPENDED", "0", "child-handler"),
                            Triple("COMMITTED", "0", "child-handler"),
                            Triple("COMMITTED", "0", "root-handler"),
                            Triple("CANCELLATION_REQUESTED", null, null),
                        ),
                        getEventSequence(),
                    )
                } finally {
                    rootSubscription.close()
                    childSubscription.close()
                }
            }
        }

        @Nested
        inner class RollbackRequests {
            @Test
            fun `rolling back the entire hierarchy works`() {

                val latch = CountDownLatch(1)
                val rollbackLatch = CountDownLatch(1)

                val rootSubscription =
                    messageQueue.subscribe(
                        rootTopic,
                        saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                            step(
                                { scope, message ->
                                    val childPayload = JsonObject().put("from", "root-handler")
                                    scope.launch(childTopic, childPayload).await().indefinitely()
                                },
                                rollback = { scope, message, throwable ->
                                    rollbackLatch.countDown()
                                },
                            )
                        },
                    )

                val childSubscription =
                    messageQueue.subscribe(
                        childTopic,
                        saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                            step { scope, message -> latch.countDown() }
                        },
                    )

                try {
                    val rootPayload = JsonObject().put("initial", "true")
                    val cooperationRoot =
                        pool
                            .withTransaction { connection ->
                                messageQueue.launch(connection, rootTopic, rootPayload)
                            }
                            .await()
                            .indefinitely()

                    assertTrue(
                        latch.await(1, TimeUnit.SECONDS),
                        "Not everything completed correctly",
                    )
                    Thread.sleep(100)

                    pool
                        .withTransaction { connection ->
                            structuredCooperationManager.rollback(
                                connection,
                                cooperationRoot.cooperationScopeIdentifier,
                                "master-system",
                                "feelz",
                            )
                        }
                        .await()
                        .indefinitely()

                    assertTrue(
                        rollbackLatch.await(1, TimeUnit.SECONDS),
                        "Not everything rolled back correctly",
                    )
                    Thread.sleep(100)

                    assertEquals(
                        listOf(
                            Triple("EMITTED", null, null),
                            Triple("SEEN", null, "root-handler"),
                            Triple("EMITTED", "0", "root-handler"),
                            Triple("SUSPENDED", "0", "root-handler"),
                            Triple("SEEN", null, "child-handler"),
                            Triple("SUSPENDED", "0", "child-handler"),
                            Triple("COMMITTED", "0", "child-handler"),
                            Triple("COMMITTED", "0", "root-handler"),
                            Triple("ROLLBACK_EMITTED", null, null),
                            Triple("ROLLING_BACK", null, "root-handler"),
                            Triple(
                                "ROLLBACK_EMITTED",
                                "Rollback of 0 (rolling back child scopes)",
                                "root-handler",
                            ),
                            Triple(
                                "SUSPENDED",
                                "Rollback of 0 (rolling back child scopes)",
                                "root-handler",
                            ),
                            Triple("ROLLING_BACK", null, "child-handler"),
                            Triple(
                                "SUSPENDED",
                                "Rollback of 0 (rolling back child scopes)",
                                "child-handler",
                            ),
                            Triple("SUSPENDED", "Rollback of 0", "child-handler"),
                            Triple("ROLLED_BACK", "Rollback of 0", "child-handler"),
                            Triple("SUSPENDED", "Rollback of 0", "root-handler"),
                            Triple("ROLLED_BACK", "Rollback of 0", "root-handler"),
                        ),
                        getEventSequence(),
                    )
                } finally {
                    rootSubscription.close()
                    childSubscription.close()
                }
            }

            @Test
            fun `rolling back sub-hierarchy works (but should be done carefully, as you run the risk of bringing the state of the system into an inconsistent state from a business perspective)`() {

                val latch = CountDownLatch(1)
                val rollbackLatch = CountDownLatch(1)

                val rootSubscription =
                    messageQueue.subscribe(
                        rootTopic,
                        saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                            step { scope, message ->
                                val childPayload = JsonObject().put("from", "root-handler")
                                scope.launch(childTopic, childPayload).await().indefinitely()
                            }
                        },
                    )

                lateinit var cooperationScopeIdentifier: CooperationScopeIdentifier

                val childSubscription =
                    messageQueue.subscribe(
                        childTopic,
                        saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                            step(
                                { scope, message ->
                                    cooperationScopeIdentifier =
                                        scope.continuationCooperationScopeIdentifier
                                    latch.countDown()
                                },
                                rollback = { scope, message, throwable ->
                                    rollbackLatch.countDown()
                                },
                            )
                        },
                    )

                try {
                    val rootPayload = JsonObject().put("initial", "true")
                    pool
                        .withTransaction { connection ->
                            messageQueue.launch(connection, rootTopic, rootPayload)
                        }
                        .await()
                        .indefinitely()

                    assertTrue(
                        latch.await(1, TimeUnit.SECONDS),
                        "Not everything completed correctly",
                    )
                    Thread.sleep(100)

                    pool
                        .withTransaction { connection ->
                            structuredCooperationManager.rollback(
                                connection,
                                cooperationScopeIdentifier,
                                "master-system",
                                "feelz",
                            )
                        }
                        .await()
                        .indefinitely()

                    assertTrue(
                        rollbackLatch.await(1, TimeUnit.SECONDS),
                        "Not everything rolled back correctly",
                    )
                    Thread.sleep(100)

                    assertEquals(
                        listOf(
                            Triple("EMITTED", null, null),
                            Triple("SEEN", null, "root-handler"),
                            Triple("EMITTED", "0", "root-handler"),
                            Triple("SUSPENDED", "0", "root-handler"),
                            Triple("SEEN", null, "child-handler"),
                            Triple("SUSPENDED", "0", "child-handler"),
                            Triple("COMMITTED", "0", "child-handler"),
                            Triple("COMMITTED", "0", "root-handler"),
                            Triple("ROLLBACK_EMITTED", null, null),
                            Triple("ROLLING_BACK", null, "child-handler"),
                            Triple(
                                "SUSPENDED",
                                "Rollback of 0 (rolling back child scopes)",
                                "child-handler",
                            ),
                            Triple("SUSPENDED", "Rollback of 0", "child-handler"),
                            Triple("ROLLED_BACK", "Rollback of 0", "child-handler"),
                        ),
                        getEventSequence(),
                    )
                } finally {
                    rootSubscription.close()
                    childSubscription.close()
                }
            }

            @Test
            fun `rolling back while things are still running has no effect`() {

                val latch = CountDownLatch(1)
                val secondRootStepExecuting = CountDownLatch(1)
                val rollbackEmitted = CountDownLatch(1)

                val rootSubscription =
                    messageQueue.subscribe(
                        rootTopic,
                        saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                            step { scope, message ->
                                val childPayload = JsonObject().put("from", "root-handler")
                                scope.launch(childTopic, childPayload).await().indefinitely()
                            }
                            step { scope, message ->
                                secondRootStepExecuting.countDown()
                                rollbackEmitted.await()
                                latch.countDown()
                            }
                        },
                    )

                lateinit var cooperationScopeIdentifier: CooperationScopeIdentifier

                val childSubscription =
                    messageQueue.subscribe(
                        childTopic,
                        saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                            step { scope, message ->
                                cooperationScopeIdentifier =
                                    scope.continuationCooperationScopeIdentifier
                            }
                        },
                    )

                try {
                    val rootPayload = JsonObject().put("initial", "true")
                    pool
                        .withTransaction { connection ->
                            messageQueue.launch(connection, rootTopic, rootPayload)
                        }
                        .await()
                        .indefinitely()

                    assertTrue(
                        secondRootStepExecuting.await(1, TimeUnit.SECONDS),
                        "Second step didn't start executing",
                    )

                    pool
                        .withTransaction { connection ->
                            structuredCooperationManager.rollback(
                                connection,
                                cooperationScopeIdentifier,
                                "master-system",
                                "feelz",
                            )
                        }
                        .await()
                        .indefinitely()

                    rollbackEmitted.countDown()

                    assertTrue(
                        latch.await(1, TimeUnit.SECONDS),
                        "Not everything completed correctly",
                    )
                    Thread.sleep(100)

                    assertEquals(
                        listOf(
                            Triple("EMITTED", null, null),
                            Triple("SEEN", null, "root-handler"),
                            Triple("EMITTED", "0", "root-handler"),
                            Triple("SUSPENDED", "0", "root-handler"),
                            Triple("SEEN", null, "child-handler"),
                            Triple("SUSPENDED", "0", "child-handler"),
                            Triple("COMMITTED", "0", "child-handler"),
                            Triple("SUSPENDED", "1", "root-handler"),
                            Triple("COMMITTED", "1", "root-handler"),
                        ),
                        getEventSequence(),
                    )
                } finally {
                    rootSubscription.close()
                    childSubscription.close()
                }
            }
        }
    }

    data object ParentContextKey : CooperationContext.MappedKey<ParentContext>()

    data class ParentContext(val value: Int) : CooperationContext.MappedElement(ParentContextKey)

    data object ChildContextKey : CooperationContext.MappedKey<ChildContext>()

    data class ChildContext(val value: Int) : CooperationContext.MappedElement(ChildContextKey)

    @Nested
    inner class Context {

        @Test
        fun `context is propagated correctly`() {
            val contextValues = mutableListOf<String>()

            val latch = CountDownLatch(1)

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                        step(
                            { scope, message ->
                                contextValues.add(objectMapper.writeValueAsString(scope.context))
                                scope.context +=
                                    ParentContext(scope.context[ParentContextKey]!!.value + 1)
                                contextValues.add(objectMapper.writeValueAsString(scope.context))
                                val childPayload = JsonObject().put("from", "root-handler")
                                scope
                                    .launch(childTopic, childPayload, ChildContext(0))
                                    .await()
                                    .indefinitely()
                            },
                            rollback = { scope, message, throwable ->
                                scope.context +=
                                    ParentContext(scope.context[ParentContextKey]!!.value + 1)
                                contextValues.add(objectMapper.writeValueAsString(scope.context))
                                latch.countDown()
                            },
                        )

                        step { scope, message ->
                            scope.context +=
                                ParentContext(scope.context[ParentContextKey]!!.value + 1)
                            contextValues.add(objectMapper.writeValueAsString(scope.context))
                            throw RuntimeException("Failure")
                        }
                    },
                )

            val childSubscription =
                messageQueue.subscribe(
                    childTopic,
                    saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                        step(
                            { scope, message ->
                                scope.context +=
                                    ChildContext(scope.context[ChildContextKey]!!.value + 1)
                                contextValues.add(objectMapper.writeValueAsString(scope.context))
                            },
                            rollback = { scope, message, throwable ->
                                scope.context +=
                                    ChildContext(scope.context[ChildContextKey]!!.value + 1)
                                contextValues.add(objectMapper.writeValueAsString(scope.context))
                            },
                        )
                        step(
                            { scope, message ->
                                scope.context +=
                                    ChildContext(scope.context[ChildContextKey]!!.value + 1)
                                contextValues.add(objectMapper.writeValueAsString(scope.context))
                            },
                            rollback = { scope, message, throwable ->
                                scope.context +=
                                    ChildContext(scope.context[ChildContextKey]!!.value + 1)
                                contextValues.add(objectMapper.writeValueAsString(scope.context))
                            },
                        )
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload, ParentContext(0))
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "Not everything completed correctly")

                Thread.sleep(200)

                assertEquals(
                    listOf(
                        """{"ParentContextKey":{"value":0}}""",
                        """{"ParentContextKey":{"value":1}}""",
                        """{"ChildContextKey":{"value":1},"ParentContextKey":{"value":1}}""",
                        """{"ChildContextKey":{"value":2},"ParentContextKey":{"value":1}}""",
                        """{"ParentContextKey":{"value":2}}""",
                        """{"ChildContextKey":{"value":3},"ParentContextKey":{"value":2}}""",
                        """{"ChildContextKey":{"value":4},"ParentContextKey":{"value":2}}""",
                        """{"ParentContextKey":{"value":3}}""",
                    ),
                    contextValues,
                    "Context values should match",
                )
            } finally {
                rootSubscription.close()
                childSubscription.close()
            }
        }
    }

    @Nested
    inner class TryFinally {

        @Test
        fun `finally is executed on success`() {
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val finallyTopic = "finally-topic"
            val latch = CountDownLatch(1)

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                        tryFinallyStep(
                            invoke = { scope, message ->
                                executionOrder.add("root-try")
                                val childPayload = JsonObject().put("from", "root-handler")

                                scope.launch(childTopic, childPayload).await().indefinitely()
                            },
                            finally = { scope, message ->
                                executionOrder.add("root-finally")
                                val finallyPayload = JsonObject().put("from", "root-handler")

                                scope.launch(finallyTopic, finallyPayload).await().indefinitely()
                            },
                        )
                        step { scope, message ->
                            executionOrder.add("root-end")
                            latch.countDown()
                        }
                    },
                )

            val childSubscription =
                messageQueue.subscribe(
                    childTopic,
                    saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message -> executionOrder.add("child-handler") }
                    },
                )

            val finallySubscription =
                messageQueue.subscribe(
                    finallyTopic,
                    saga("finally-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message -> executionOrder.add("finally-handler") }
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
                Thread.sleep(200)

                assertEquals(
                    listOf(
                        "root-try",
                        "child-handler",
                        "root-finally",
                        "finally-handler",
                        "root-end",
                    ),
                    executionOrder,
                    "Execution order obeys structured cooperation rules",
                )
            } finally {
                rootSubscription.close()
                childSubscription.close()
                finallySubscription.close()
            }
        }

        @Test
        fun `finally is executed on root failure but messages are not emitted, because neither were those in the 'try' step`() {
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val finallyTopic = "finally-topic"
            val latch = CountDownLatch(1)

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                        tryFinallyStep(
                            invoke = { scope, message ->
                                executionOrder.add("root-try")
                                throw RuntimeException("Simulated failure to test rollback")
                            },
                            finally = { scope, message ->
                                executionOrder.add("root-finally")
                                val finallyPayload = JsonObject().put("from", "root-handler")

                                scope.launch(finallyTopic, finallyPayload).await().indefinitely()
                                latch.countDown()
                            },
                        )
                    },
                )

            val finallySubscription =
                messageQueue.subscribe(
                    finallyTopic,
                    saga("finally-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message -> executionOrder.add("finally-handler") }
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
                Thread.sleep(200)

                assertEquals(
                    listOf("root-try", "root-finally"),
                    executionOrder,
                    "Execution order obeys structured cooperation rules",
                )
            } finally {
                rootSubscription.close()
                finallySubscription.close()
            }
        }

        @Test
        fun `finally is executed on child failure`() {
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val finallyTopic = "finally-topic"
            val latch = CountDownLatch(1)

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                        tryFinallyStep(
                            invoke = { scope, message ->
                                executionOrder.add("root-try")
                                val childPayload = JsonObject().put("from", "root-handler")

                                scope.launch(childTopic, childPayload).await().indefinitely()
                            },
                            finally = { scope, message ->
                                executionOrder.add("root-finally")
                                val finallyPayload = JsonObject().put("from", "root-handler")

                                scope.launch(finallyTopic, finallyPayload).await().indefinitely()
                            },
                        )
                        step { scope, message -> executionOrder.add("root-end") }
                    },
                )

            val childSubscription =
                messageQueue.subscribe(
                    childTopic,
                    saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            executionOrder.add("child-handler")
                            throw RuntimeException("Simulated failure to test rollback")
                        }
                    },
                )

            val finallySubscription =
                messageQueue.subscribe(
                    finallyTopic,
                    saga("finally-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            executionOrder.add("finally-handler")
                            latch.countDown()
                        }
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
                Thread.sleep(200)

                assertEquals(
                    listOf("root-try", "child-handler", "root-finally", "finally-handler"),
                    executionOrder,
                    "Execution order obeys structured cooperation rules",
                )
            } finally {
                rootSubscription.close()
                childSubscription.close()
                finallySubscription.close()
            }
        }

        @Test
        fun `finally is executed, once, on subsequent step failure`() {
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val finallyTopic = "finally-topic"
            val latch = CountDownLatch(1)

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                        step(
                            invoke = { scope, message -> executionOrder.add("root-start") },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-rollback")
                                latch.countDown()
                            },
                        )
                        tryFinallyStep(
                            invoke = { scope, message -> executionOrder.add("root-try") },
                            finally = { scope, message ->
                                executionOrder.add("root-finally")
                                val finallyPayload = JsonObject().put("from", "root-handler")

                                scope.launch(finallyTopic, finallyPayload).await().indefinitely()
                            },
                        )
                        step { scope, message ->
                            executionOrder.add("root-failure")
                            throw RuntimeException("Simulated failure to test rollback")
                        }
                    },
                )

            val finallySubscription =
                messageQueue.subscribe(
                    finallyTopic,
                    saga("finally-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message -> executionOrder.add("finally-handler") }
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
                Thread.sleep(200)

                assertEquals(
                    listOf(
                        "root-start",
                        "root-try",
                        "root-finally",
                        "finally-handler",
                        "root-failure",
                        "root-rollback",
                    ),
                    executionOrder,
                    "Execution order obeys structured cooperation rules",
                )
            } finally {
                rootSubscription.close()
                finallySubscription.close()
            }
        }

        @Test
        fun `finally is only executed once when its child causes a rollback`() {
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())

            val finallyTopic = "finally-topic"
            val latch = CountDownLatch(1)

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                        step(
                            invoke = { scope, message -> executionOrder.add("root-start") },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-rollback")
                                latch.countDown()
                            },
                        )
                        tryFinallyStep(
                            invoke = { scope, message -> executionOrder.add("root-try") },
                            finally = { scope, message ->
                                executionOrder.add("root-finally")
                                val finallyPayload = JsonObject().put("from", "root-handler")

                                scope.launch(finallyTopic, finallyPayload).await().indefinitely()
                            },
                        )
                    },
                )

            val finallySubscription =
                messageQueue.subscribe(
                    finallyTopic,
                    saga("finally-handler", handlerRegistry.eventLoopStrategy()) {
                        step { scope, message ->
                            executionOrder.add("finally-handler")
                            throw RuntimeException("Simulated failure to test rollback")
                        }
                    },
                )

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
                Thread.sleep(200)

                assertEquals(
                    listOf(
                        "root-start",
                        "root-try",
                        "root-finally",
                        "finally-handler",
                        "root-rollback",
                    ),
                    executionOrder,
                    "Execution order obeys structured cooperation rules",
                )
            } finally {
                rootSubscription.close()
                finallySubscription.close()
            }
        }
    }

    @Nested
    inner class Deadlines {

        @Test
        fun `happy path deadlines work`() {
            val latch = CountDownLatch(1)
            val childStarted = AtomicBoolean(false)
            val deadline = AtomicReference<HappyPathDeadline>()

            val rootHandlerCoroutine =
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        invoke = { scope, message ->
                            val payload = JsonObject().put("from", "root-handler")

                            deadline.set(happyPathTimeout(Duration.ZERO, "root handler"))
                            scope.launch(childTopic, payload, deadline.get()).await().indefinitely()
                        },
                        rollback = { _, _, _ -> latch.countDown() },
                    )
                }
            val rootSubscription = messageQueue.subscribe(rootTopic, rootHandlerCoroutine)

            val childHandlerCoroutine =
                saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        childStarted.set(true)
                        Thread.sleep(Duration.INFINITE.toJavaDuration())
                    }
                }
            val childSubscription = messageQueue.subscribe(childTopic, childHandlerCoroutine)

            try {
                val rootPayload = JsonObject().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launch(
                            connection,
                            rootTopic,
                            rootPayload,
                            noHappyPathTimeout("root system"),
                        )
                    }
                    .await()
                    .indefinitely()

                assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
                Thread.sleep(200)

                assertFalse(
                    childStarted.get(),
                    "Child shouldn't even start, since shouldGiveUp() is called before (and after) the handler actually runs",
                )
                assertEquals(
                    listOf(
                        Triple("EMITTED", null, null),
                        Triple("SEEN", null, "root-handler"),
                        Triple("EMITTED", "0", "root-handler"),
                        Triple("SUSPENDED", "0", "root-handler"),
                        Triple("SEEN", null, "child-handler"),
                        Triple("ROLLING_BACK", "0", "child-handler"),
                        Triple("ROLLED_BACK", "Rollback of 0", "child-handler"),
                        Triple("ROLLING_BACK", "0", "root-handler"),
                        Triple(
                            "ROLLBACK_EMITTED",
                            "Rollback of 0 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple(
                            "SUSPENDED",
                            "Rollback of 0 (rolling back child scopes)",
                            "root-handler",
                        ),
                        Triple("SUSPENDED", "Rollback of 0", "root-handler"),
                        Triple("ROLLED_BACK", "Rollback of 0", "root-handler"),
                    ),
                    getEventSequence(),
                )

                val childHandlerRollingBackExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLING_BACK", "child-handler")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${childHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.GaveUpException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.GaveUpException",
                            childHandlerCoroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[root handler] MissedHappyPathDeadline: Missed happy path deadline of root handler at ${deadline.get().deadline.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}. Deadline trace: [{\"HappyPathDeadlineKey\": {\"trace\": [], \"source\": \"root system\", \"deadline\": \"9999-12-31T23:59:59.999999Z\"}}]",
                                    "MissedHappyPathDeadline",
                                    "root handler",
                                )
                            ),
                        )
                    ),
                    childHandlerRollingBackExceptions,
                )

                val rootHandlerRollingBackExceptions =
                    pool
                        .withConnection { connection ->
                            fetchExceptions(connection, "ROLLING_BACK", "root-handler")
                        }
                        .await()
                        .indefinitely()

                assertEquivalent(
                    listOf(
                        CooperationExceptionData(
                            "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                            "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                            rootHandlerCoroutine.identifier.asSource(),
                            listOf(
                                CooperationExceptionData(
                                    "[${childHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.GaveUpException: <no message>",
                                    "io.github.gabrielshanahan.structmess.coroutine.GaveUpException",
                                    childHandlerCoroutine.identifier.asSource(),
                                    listOf(
                                        CooperationExceptionData(
                                            "[root handler] MissedHappyPathDeadline: Missed happy path deadline of root handler at ${deadline.get().deadline.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}. Deadline trace: [{\"HappyPathDeadlineKey\": {\"trace\": [], \"source\": \"root system\", \"deadline\": \"9999-12-31T23:59:59.999999Z\"}}]",
                                            "MissedHappyPathDeadline",
                                            "root handler",
                                        )
                                    ),
                                )
                            ),
                        )
                    ),
                    rootHandlerRollingBackExceptions,
                )
            } finally {
                rootSubscription.close()
                childSubscription.close()
            }
        }
    }

    private fun List<Triple<String, String?, String?>>.printEventSequenceCode():
        List<Triple<String, String?, String?>> = also {
        println(
            joinToString(prefix = "listOf(\n\t", postfix = "\n)", separator = ",\n\t") {
                "Triple(${it.first.let { "\"$it\""}}, ${it.second?.let { "\"$it\""}}, ${it.third?.let { "\"$it\""}})"
            }
        )
    }

    private fun getEventSequence(): List<Triple<String, String?, String?>> =
        pool
            .executeAndAwaitPreparedQuery(
                "SELECT type, step, coroutine_name FROM message_event ORDER BY created_at"
            )
            .map {
                Triple(it.getString("type"), it.getString("step"), it.getString("coroutine_name"))
            }

    fun List<Triple<String, String?, String?>>.keepOnlyHandlers(
        vararg handlers: String
    ): List<Triple<String, String?, String?>> = filter { it.third == null || it.third in handlers }

    fun List<String>.keepOnlyPrefixedBy(vararg elements: String): List<String> = filter {
        elements.any { prefix -> it.startsWith(prefix) }
    }

    fun fetchExceptions(
        client: SqlClient,
        type: String,
        coroutineName: String?,
    ): Uni<List<CooperationException>> =
        client
            .preparedQuery(
                "SELECT COALESCE(JSON_AGG(exception), '[]'::json) as exceptions FROM message_event WHERE type = $1 AND coroutine_name ${if (coroutineName == null) "IS NULL" else "= $2"}"
            )
            .execute(Tuple.from(listOfNotNull(type, coroutineName)))
            .map { rowSet ->
                rowSet.single().getJsonArray("exceptions").map { exceptionJson ->
                    CooperationFailure.toCooperationException(
                        (exceptionJson as JsonObject).mapTo(CooperationFailure::class.java)
                    )
                }
            }

    data class CooperationExceptionData(
        val message: String,
        val type: String,
        val source: String,
        val causes: List<CooperationExceptionData> = emptyList(),
    )

    fun List<CooperationException>.printExceptionDataCode() = apply {
        println(asExceptionData().constructorString())
    }

    fun assertEquivalent(
        expected: List<CooperationExceptionData>,
        actual: List<CooperationException>,
    ) {
        assertEquals(
            expected.size,
            actual.size,
            "Sizes don't match - expected $expected, but got $actual",
        )
        expected.zip(actual).forEach { (expected, actual) ->
            assertEquals(
                expected.message,
                actual.message,
                "Messages don't match - expected ${expected.message}, but got ${actual.message}",
            )
            assertEquals(
                expected.type,
                actual.type,
                "Types don't match - expected ${expected.type}, but got ${actual.type}",
            )
            assertEquals(
                expected.source,
                actual.source,
                "Sources don't match - expected ${expected.source}, but got ${actual.source}",
            )
            assertEquivalent(expected.causes, actual.causes)
        }
    }

    fun DistributedCoroutineIdentifier.asSource() = "$name[$instance]"

    fun List<CooperationException>.asExceptionData(): List<CooperationExceptionData> = map {
        CooperationExceptionData(it.message, it.type, it.source, it.causes.asExceptionData())
    }

    fun List<CooperationExceptionData>.constructorString(tabs: String = ""): String =
        """
        |${tabs}listOf(
        |${joinToString(",\n") { it.constructorString("$tabs\t") }}
        |${tabs}),
        |
    """
            .trimMargin()
            .takeIf { isNotEmpty() } ?: ""

    fun CooperationExceptionData.constructorString(tabs: String = ""): String =
        """
        |${tabs}CooperationExceptionData(
        |$tabs    "[${source.sourceAsCode()}]${message.substringAfterLast(']')}",
        |$tabs    "$type",
        |$tabs    "${source.sourceAsCode()}", 
        |${causes.constructorString("$tabs\t")}${tabs})
    """
            .trimMargin()

    fun String.sourceAsCode() =
        variableName.takeIf { it.isNotBlank() }?.let { "${'$'}{$it.identifier.asSource()}" } ?: this

    val String.variableName: String
        get() =
            when {
                contains("root-handler") -> "rootHandlerCoroutine"
                contains("child-handler-1") -> "childHandler1Coroutine"
                contains("child-handler-2") -> "childHandler2Coroutine"
                contains("child-handler") -> "childHandlerCoroutine"
                contains("grandchild") -> "grandChildCoroutine"
                else -> ""
            }
}

data object TriedAgainKey : CooperationContext.MappedKey<TriedAgainValue>()

data object TriedAgainValue : CooperationContext.MappedElement(TriedAgainKey)
