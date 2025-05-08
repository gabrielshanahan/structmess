package io.github.gabrielshanahan.structmess.messaging

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.coroutine.CooperationContext
import io.github.gabrielshanahan.structmess.coroutine.CooperationRoot
import io.github.gabrielshanahan.structmess.coroutine.CoroutineIdentifier
import io.github.gabrielshanahan.structmess.coroutine.coroutine
import io.github.gabrielshanahan.structmess.coroutine.writeCooperationContext
import io.github.gabrielshanahan.structmess.domain.CooperationException
import io.github.gabrielshanahan.structmess.domain.CooperationFailure
import io.quarkus.test.junit.QuarkusTest
import io.smallrye.mutiny.Uni
import io.vertx.mutiny.sqlclient.Pool
import io.vertx.mutiny.sqlclient.SqlClient
import io.vertx.mutiny.sqlclient.Tuple
import jakarta.inject.Inject
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresMessageQueueStructuredConcurrencyTest {
    @Inject lateinit var messageQueue: MessageQueue

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
            val executionOrder = mutableListOf<String>()

            val latch = CountDownLatch(4) // 2 root steps + 2 child steps

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    coroutine("root-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                        step { scope, message ->
                            val childPayload =
                                objectMapper.createObjectNode().put("from", "root-handler")
                            messageQueue
                                .launch(scope, childTopic, childPayload)
                                .await()
                                .indefinitely()
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
                    coroutine("child-handler", handlerRegistry.cooperationHierarchyStrategy()) {
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
                val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
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
            val executionOrder = mutableListOf<String>()

            val latch = CountDownLatch(7) // 2 root steps + 3 child steps + 2 grandchild steps

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    coroutine("root-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                        step { scope, message ->
                            val childPayload =
                                objectMapper.createObjectNode().put("from", "root-handler")
                            messageQueue
                                .launch(scope, childTopic, childPayload)
                                .await()
                                .indefinitely()
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
                    coroutine("child-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                        step { scope, message ->
                            latch.countDown()
                            executionOrder.add("child-handler-step-1")
                        }
                        step { scope, message ->
                            val grandchildPayload =
                                objectMapper.createObjectNode().put("from", "child-handler")
                            messageQueue
                                .launch(scope, grandchildTopic, grandchildPayload)
                                .await()
                                .indefinitely()
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
                    coroutine(
                        "grandchild-handler",
                        handlerRegistry.cooperationHierarchyStrategy(),
                    ) {
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
                val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
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
            val executionOrder = mutableListOf<String>()

            val latch = CountDownLatch(6) // 2 root steps + 2 x 2 child steps

            val childTopic2 = "child-topic-2"

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    coroutine("root-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                        step { scope, message ->
                            executionOrder.add("root-handler-step-1")
                            val childPayload1 =
                                objectMapper.createObjectNode().put("from", "root-handler")
                            val childPayload2 =
                                objectMapper.createObjectNode().put("from", "root-handler")

                            messageQueue
                                .launch(scope, childTopic, childPayload1)
                                .await()
                                .indefinitely()
                            messageQueue
                                .launch(scope, childTopic2, childPayload2)
                                .await()
                                .indefinitely()

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
                    coroutine("child-handler-1", handlerRegistry.cooperationHierarchyStrategy()) {
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
                    coroutine("child-handler-2", handlerRegistry.cooperationHierarchyStrategy()) {
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
                val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
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
            val executionOrder = mutableListOf<String>()

            val latch = CountDownLatch(6) // 2 root + 2 x 2 children listening to same topic

            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    coroutine("root-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                        step { scope, message ->
                            executionOrder.add("root-handler-step-1")
                            val childPayload =
                                objectMapper.createObjectNode().put("from", "root-handler")
                            messageQueue
                                .launch(scope, childTopic, childPayload)
                                .await()
                                .indefinitely()
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
                    coroutine("child-handler-1", handlerRegistry.cooperationHierarchyStrategy()) {
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
                    coroutine("child-handler-2", handlerRegistry.cooperationHierarchyStrategy()) {
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
                val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
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
            val executionOrder = mutableListOf<String>()

            val rootHandler = "root-handler"
            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    coroutine(rootHandler, handlerRegistry.cooperationHierarchyStrategy()) {
                        step(
                            { scope, message ->
                                val childPayload =
                                    objectMapper.createObjectNode().put("from", rootHandler)
                                messageQueue
                                    .launch(scope, childTopic, childPayload)
                                    .await()
                                    .indefinitely()
                                executionOrder.add("root-handler-step-1")
                                latch.countDown()
                                throw RuntimeException("Simulated failure to test rollback")
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-handler-rollback-step-1")
                            },
                            handleScopeFailure = { scope, throwable ->
                                executionOrder.add("root-handler-handleScopeFailure-step-1")
                                throw throwable
                            },
                        )
                    },
                )

            try {
                val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
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
            val executionOrder = mutableListOf<String>()

            val rootHandler = "root-handler"
            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    coroutine(rootHandler, handlerRegistry.cooperationHierarchyStrategy()) {
                        step(
                            { scope, message ->
                                val childPayload =
                                    objectMapper.createObjectNode().put("from", rootHandler)
                                messageQueue
                                    .launch(scope, childTopic, childPayload)
                                    .await()
                                    .indefinitely()
                                executionOrder.add("root-handler-step-1")
                                latch.countDown()
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-handler-rollback-step-1")
                                latch.countDown()
                            },
                            handleScopeFailure = { scope, throwable ->
                                executionOrder.add("root-handler-handleScopeFailure-step-1")
                                throw throwable
                            },
                        )

                        step(
                            { scope, message ->
                                val childPayload =
                                    objectMapper.createObjectNode().put("from", rootHandler)
                                messageQueue
                                    .launch(scope, childTopic, childPayload)
                                    .await()
                                    .indefinitely()
                                executionOrder.add("root-handler-step-2")
                                latch.countDown()
                                throw RuntimeException("Simulated failure to test rollback")
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-handler-rollback-step-2")
                            },
                            handleScopeFailure = { scope, throwable ->
                                executionOrder.add("root-handler-handleScopeFailure-step-2")
                                throw throwable
                            },
                        )
                    },
                )

            try {
                val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
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
            val executionOrder = mutableListOf<String>()

            val rootHandler = "root-handler"
            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    coroutine(rootHandler, handlerRegistry.cooperationHierarchyStrategy()) {
                        step(
                            { scope, message ->
                                val childPayload =
                                    objectMapper.createObjectNode().put("from", rootHandler)
                                messageQueue
                                    .launch(scope, childTopic, childPayload)
                                    .await()
                                    .indefinitely()
                                executionOrder.add("root-handler-step-1")
                                latch.countDown()
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-handler-rollback-step-1")
                                latch.countDown()
                            },
                            handleScopeFailure = { scope, throwable ->
                                executionOrder.add("root-handler-handleScopeFailure-step-1")
                                throw throwable
                            },
                        )
                    },
                )

            val childHandler = "child-handler"
            val childSubscription =
                messageQueue.subscribe(
                    childTopic,
                    coroutine(childHandler, handlerRegistry.cooperationHierarchyStrategy()) {
                        step(
                            { scope, message ->
                                executionOrder.add("child-handler-step-1")
                                latch.countDown()
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("child-handler-rollback-step-1")
                                latch.countDown()
                            },
                            handleScopeFailure = { scope, throwable ->
                                executionOrder.add("child-handler-handleScopeFailure-step-1")
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
                            handleScopeFailure = { scope, throwable ->
                                executionOrder.add("child-handler-handleScopeFailure-step-2")
                                throw throwable
                            },
                        )
                    },
                )

            try {
                val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
                    }
                    .await()
                    .indefinitely()

                assertTrue(
                    latch.await(1000, TimeUnit.SECONDS),
                    "Not everything completed correctly",
                )
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
                        "root-handler-handleScopeFailure-step-1",
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
            val executionOrder = mutableListOf<String>()

            val rootHandler = "root-handler"
            val rootSubscription =
                messageQueue.subscribe(
                    rootTopic,
                    coroutine(rootHandler, handlerRegistry.cooperationHierarchyStrategy()) {
                        step(
                            { scope, message ->
                                val childPayload =
                                    objectMapper.createObjectNode().put("from", rootHandler)
                                messageQueue
                                    .launch(scope, childTopic, childPayload)
                                    .await()
                                    .indefinitely()
                                executionOrder.add("root-handler-step-1")
                                latch.countDown()
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("root-handler-rollback-step-1")
                                latch.countDown()
                            },
                            handleScopeFailure = { scope, throwable ->
                                executionOrder.add("root-handler-handleScopeFailure-step-1")
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
                            handleScopeFailure = { scope, throwable ->
                                executionOrder.add("root-handler-handleScopeFailure-step-2")
                                throw throwable
                            },
                        )
                    },
                )

            val childHandler = "child-handler"
            val childSubscription =
                messageQueue.subscribe(
                    childTopic,
                    coroutine(childHandler, handlerRegistry.cooperationHierarchyStrategy()) {
                        step(
                            { scope, message ->
                                executionOrder.add("child-handler-step-1")
                                latch.countDown()
                            },
                            rollback = { scope, message, throwable ->
                                executionOrder.add("child-handler-rollback-step-1")
                                latch.countDown()
                            },
                            handleScopeFailure = { scope, throwable ->
                                executionOrder.add("child-handler-handleScopeFailure-step-1")
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
                            handleScopeFailure = { scope, throwable ->
                                executionOrder.add("child-handler-handleScopeFailure-step-2")
                                throw throwable
                            },
                        )
                    },
                )

            try {
                val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
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
            val executionOrder = mutableListOf<String>()

            // (2 root steps + 3 child1 steps + 2 child2 steps + 2 grandchild steps) * 2
            // rollbacks - 1 child2_step2_rollback + 1 root handleScopeFailure
            val latch = CountDownLatch(18)

            val rootHandlerCoroutine =
                coroutine("root-handler", handlerRegistry.cooperationHierarchyStrategy()) {
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
                            val childPayload =
                                objectMapper.createObjectNode().put("from", "root-handler")
                            messageQueue
                                .launch(scope, childTopic, childPayload)
                                .await()
                                .indefinitely()
                            latch.countDown()
                            executionOrder.add("root-handler-step-2")
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("root-handler-rollback-step-2")
                            latch.countDown()
                        },
                        handleScopeFailure = { scope, throwable ->
                            executionOrder.add("root-handler-handleScopeFailure-step-2")
                            latch.countDown()
                            throw throwable
                        },
                    )
                }
            val rootSubscription = messageQueue.subscribe(rootTopic, rootHandlerCoroutine)

            val childHandler1Coroutine =
                coroutine("child-handler-1", handlerRegistry.cooperationHierarchyStrategy()) {
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
                            val grandchildPayload =
                                objectMapper.createObjectNode().put("from", "child-handler-1")
                            messageQueue
                                .launch(scope, grandchildTopic, grandchildPayload)
                                .await()
                                .indefinitely()
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
                coroutine("child-handler-2", handlerRegistry.cooperationHierarchyStrategy()) {
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
                coroutine("grandchild-handler", handlerRegistry.cooperationHierarchyStrategy()) {
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
                val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
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
                        "root-handler-handleScopeFailure-step-2",
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
                        "root-handler-handleScopeFailure-step-2",
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

        // We're only including the rollback/handlerScopeFailure lambdas that actually get called
        // here, for brevity
        @Test
        fun `failed rollbacks are well behaved n-deep`() {
            val executionOrder = mutableListOf<String>()

            val latch = CountDownLatch(16)

            val rootHandlerCoroutine =
                coroutine("root-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                    step({ scope, message ->
                        latch.countDown()
                        executionOrder.add("root-handler-step-1")
                    })
                    step(
                        { scope, message ->
                            val childPayload =
                                objectMapper.createObjectNode().put("from", "root-handler")
                            messageQueue
                                .launch(scope, childTopic, childPayload)
                                .await()
                                .indefinitely()
                            latch.countDown()
                            executionOrder.add("root-handler-step-2")
                        },
                        handleScopeFailure = { scope, throwable ->
                            // This will be called twice - once for the "normal" exception that
                            // starts
                            // the rollback process,
                            // and then for the additional exception that get's thrown during the
                            // rollback process
                            executionOrder.add("root-handler-handleScopeFailure-step-2")
                            latch.countDown()
                            throw throwable
                        },
                    )
                }

            val rootSubscription = messageQueue.subscribe(rootTopic, rootHandlerCoroutine)

            val childHandler1Coroutine =
                coroutine("child-handler-1", handlerRegistry.cooperationHierarchyStrategy()) {
                    step({ scope, message ->
                        executionOrder.add("child-handler-1-step-1")
                        latch.countDown()
                    })
                    step(
                        { scope, message ->
                            val grandchildPayload =
                                objectMapper.createObjectNode().put("from", "child-handler-1")
                            messageQueue
                                .launch(scope, grandchildTopic, grandchildPayload)
                                .await()
                                .indefinitely()
                            executionOrder.add("child-handler-1-step-2")
                            latch.countDown()
                        },
                        handleScopeFailure = { scope, throwable ->
                            executionOrder.add("child-handler-1-handleScopeFailure-step-2")
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
                coroutine("child-handler-2", handlerRegistry.cooperationHierarchyStrategy()) {
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
                coroutine("grandchild-handler", handlerRegistry.cooperationHierarchyStrategy()) {
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
                val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
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
                        "root-handler-handleScopeFailure-step-2",
                        "child-handler-1-rollback-step-3",
                        "grandchild-handler-rollback-step-2",
                        "grandchild-handler-rollback-step-1",
                        "child-handler-1-handleScopeFailure-step-2",
                        "root-handler-handleScopeFailure-step-2",
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
                        "root-handler-handleScopeFailure-step-2",
                        "root-handler-handleScopeFailure-step-2",
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
        inner class Cancellations {

            @Test
            fun `cancellation works`() {
                val executionOrder = mutableListOf<String>()

                val latch = CountDownLatch(3)
                val childIsExecuting = CountDownLatch(1)
                val cancellation = CountDownLatch(1)

                val rootHandlerCoroutine =
                    coroutine("root-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                        step(
                            { scope, message ->
                                val childPayload =
                                    objectMapper.createObjectNode().put("from", "root-handler")
                                messageQueue
                                    .launch(scope, childTopic, childPayload)
                                    .await()
                                    .indefinitely()
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
                    coroutine("child-handler", handlerRegistry.cooperationHierarchyStrategy()) {
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
                    val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                    val cooperationRoot =
                        pool
                            .withTransaction { connection ->
                                messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
                            }
                            .await()
                            .indefinitely()

                    childIsExecuting.await()
                    pool
                        .withTransaction { connection ->
                            messageQueue.cancel(
                                connection,
                                cooperationRoot,
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
                                "[${childHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.CancellationRequestedException: Cancellation request received",
                                "io.github.gabrielshanahan.structmess.coroutine.CancellationRequestedException",
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
                                "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException: <no message>",
                                "io.github.gabrielshanahan.structmess.coroutine.ChildRolledBackException",
                                rootHandlerCoroutine.identifier.asSource(),
                                listOf(
                                    CooperationExceptionData(
                                        "[${childHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.structmess.coroutine.CancellationRequestedException: Cancellation request received",
                                        "io.github.gabrielshanahan.structmess.coroutine.CancellationRequestedException",
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
                val executionOrder = mutableListOf<String>()

                val latch = CountDownLatch(2)

                val rootSubscription =
                    messageQueue.subscribe(
                        rootTopic,
                        coroutine("root-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                            step { scope, message ->
                                val childPayload =
                                    objectMapper.createObjectNode().put("from", "root-handler")
                                messageQueue
                                    .launch(scope, childTopic, childPayload)
                                    .await()
                                    .indefinitely()
                                latch.countDown()
                                executionOrder.add("root-handler-step-1")
                            }
                        },
                    )

                val childSubscription =
                    messageQueue.subscribe(
                        childTopic,
                        coroutine("child-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                            step { scope, message ->
                                Thread.sleep(100)
                                latch.countDown()
                                executionOrder.add("child-handler-step-1")
                            }
                        },
                    )

                try {
                    val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                    val cooperationRoot =
                        pool
                            .withTransaction { connection ->
                                messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
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
                            messageQueue.cancel(
                                connection,
                                cooperationRoot,
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
                        coroutine("root-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                            step(
                                { scope, message ->
                                    val childPayload =
                                        objectMapper.createObjectNode().put("from", "root-handler")
                                    messageQueue
                                        .launch(scope, childTopic, childPayload)
                                        .await()
                                        .indefinitely()
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
                        coroutine("child-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                            step { scope, message -> latch.countDown() }
                        },
                    )

                try {
                    val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                    val cooperationRoot =
                        pool
                            .withTransaction { connection ->
                                messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
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
                            messageQueue.rollback(
                                connection,
                                cooperationRoot,
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
            fun `rolling back sub-hierarchy works (but should be done carefully, as you run the risk of bringing the state of the system into an inconsistent state from a business perspective))`() {

                val latch = CountDownLatch(1)
                val rollbackLatch = CountDownLatch(1)

                val rootSubscription =
                    messageQueue.subscribe(
                        rootTopic,
                        coroutine("root-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                            step { scope, message ->
                                val childPayload =
                                    objectMapper.createObjectNode().put("from", "root-handler")
                                messageQueue
                                    .launch(scope, childTopic, childPayload)
                                    .await()
                                    .indefinitely()
                            }
                        },
                    )

                lateinit var cooperationRoot: CooperationRoot

                val childSubscription =
                    messageQueue.subscribe(
                        childTopic,
                        coroutine("child-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                            step(
                                { scope, message ->
                                    cooperationRoot = scope.cooperationRoot
                                    latch.countDown()
                                },
                                rollback = { scope, message, throwable ->
                                    rollbackLatch.countDown()
                                },
                            )
                        },
                    )

                try {
                    val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                    pool
                        .withTransaction { connection ->
                            messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
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
                            messageQueue.rollback(
                                connection,
                                cooperationRoot,
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
                        coroutine("root-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                            step { scope, message ->
                                val childPayload =
                                    objectMapper.createObjectNode().put("from", "root-handler")
                                messageQueue
                                    .launch(scope, childTopic, childPayload)
                                    .await()
                                    .indefinitely()
                            }
                            step { scope, message ->
                                secondRootStepExecuting.countDown()
                                rollbackEmitted.await()
                                latch.countDown()
                            }
                        },
                    )

                lateinit var cooperationRoot: CooperationRoot

                val childSubscription =
                    messageQueue.subscribe(
                        childTopic,
                        coroutine("child-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                            step { scope, message -> cooperationRoot = scope.cooperationRoot }
                        },
                    )

                try {
                    val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                    pool
                        .withTransaction { connection ->
                            messageQueue.launchOnGlobalScope(connection, rootTopic, rootPayload)
                        }
                        .await()
                        .indefinitely()

                    assertTrue(
                        secondRootStepExecuting.await(1, TimeUnit.SECONDS),
                        "Second step didn't start executing",
                    )

                    pool
                        .withTransaction { connection ->
                            messageQueue.rollback(
                                connection,
                                cooperationRoot,
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

    data object ParentContextKey : CooperationContext.MappedKey<ChildContext>()

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
                    coroutine("root-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                        step(
                            { scope, message ->
                                contextValues.add(
                                    objectMapper.writeCooperationContext(scope.context)
                                )
                                scope.context +=
                                    ParentContext(scope.context[ParentContextKey]!!.value + 1)
                                contextValues.add(
                                    objectMapper.writeCooperationContext(scope.context)
                                )
                                val childPayload =
                                    objectMapper.createObjectNode().put("from", "root-handler")
                                messageQueue
                                    .launch(scope, childTopic, childPayload, ChildContext(0))
                                    .await()
                                    .indefinitely()
                            },
                            rollback = { scope, message, throwable ->
                                scope.context +=
                                    ParentContext(scope.context[ParentContextKey]!!.value + 1)
                                contextValues.add(
                                    objectMapper.writeCooperationContext(scope.context)
                                )
                                latch.countDown()
                            },
                        )

                        step { scope, message ->
                            scope.context +=
                                ParentContext(scope.context[ParentContextKey]!!.value + 1)
                            contextValues.add(objectMapper.writeCooperationContext(scope.context))
                            throw RuntimeException("Failure")
                        }
                    },
                )

            val childSubscription =
                messageQueue.subscribe(
                    childTopic,
                    coroutine("child-handler", handlerRegistry.cooperationHierarchyStrategy()) {
                        step(
                            { scope, message ->
                                scope.context +=
                                    ChildContext(scope.context[ChildContextKey]!!.value + 1)
                                contextValues.add(
                                    objectMapper.writeCooperationContext(scope.context)
                                )
                            },
                            rollback = { scope, message, throwable ->
                                scope.context +=
                                    ChildContext(scope.context[ChildContextKey]!!.value + 1)
                                contextValues.add(
                                    objectMapper.writeCooperationContext(scope.context)
                                )
                            },
                        )
                        step(
                            { scope, message ->
                                scope.context +=
                                    ChildContext(scope.context[ChildContextKey]!!.value + 1)
                                contextValues.add(
                                    objectMapper.writeCooperationContext(scope.context)
                                )
                            },
                            rollback = { scope, message, throwable ->
                                scope.context +=
                                    ChildContext(scope.context[ChildContextKey]!!.value + 1)
                                contextValues.add(
                                    objectMapper.writeCooperationContext(scope.context)
                                )
                            },
                        )
                    },
                )

            try {
                val rootPayload = objectMapper.createObjectNode().put("initial", "true")
                pool
                    .withTransaction { connection ->
                        messageQueue.launchOnGlobalScope(
                            connection,
                            rootTopic,
                            rootPayload,
                            ParentContext(0),
                        )
                    }
                    .await()
                    .indefinitely()

                assertTrue(
                    latch.await(1000, TimeUnit.SECONDS),
                    "Not everything completed correctly",
                )

                Thread.sleep(200)

                assertEquals(
                    listOf(
                        """{"ParentContextKey":{"value":0}}""",
                        """{"ParentContextKey":{"value":1}}""",
                        """{"ParentContextKey":{"value":1},"ChildContextKey":{"value":1}}""",
                        """{"ParentContextKey":{"value":1},"ChildContextKey":{"value":2}}""",
                        """{"ParentContextKey":{"value":2}}""",
                        """{"ParentContextKey":{"value":2},"ChildContextKey":{"value":3}}""",
                        """{"ParentContextKey":{"value":2},"ChildContextKey":{"value":4}}""",
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
                    val cooperationFailure =
                        objectMapper.readValue(
                            exceptionJson as String,
                            CooperationFailure::class.java,
                        )
                    CooperationFailure.toCooperationException(cooperationFailure)
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

    fun CoroutineIdentifier.asSource() = "$name[$instance]"

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
