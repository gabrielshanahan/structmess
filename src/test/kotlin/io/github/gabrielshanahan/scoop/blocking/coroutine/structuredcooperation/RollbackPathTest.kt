package io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationExceptionData
import io.github.gabrielshanahan.scoop.blocking.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.blocking.coroutine.asSource
import io.github.gabrielshanahan.scoop.blocking.coroutine.assertEquivalent
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.blocking.coroutine.fetchExceptions
import io.github.gabrielshanahan.scoop.blocking.coroutine.getEventSequence
import io.github.gabrielshanahan.scoop.blocking.coroutine.keepOnlyHandlers
import io.github.gabrielshanahan.scoop.blocking.coroutine.keepOnlyPrefixedBy
import io.github.gabrielshanahan.scoop.blocking.messaging.eventLoopStrategy
import io.github.gabrielshanahan.scoop.blocking.transactional
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.has
import io.quarkus.test.junit.QuarkusTest
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RollbackPathTest : StructuredCooperationTest() {
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
                            val childPayload = jsonbHelper.toPGobject(mapOf("from" to rootHandler))
                            scope.launch(childTopic, childPayload)
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
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS))
            Thread.sleep(500)

            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("ROLLING_BACK", "0", "root-handler"),
                    Triple("ROLLED_BACK", "Rollback of 0", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
            Assertions.assertEquals(executionOrder, listOf("root-handler-step-1"))
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
                            val childPayload = jsonbHelper.toPGobject(mapOf("from" to rootHandler))
                            scope.launch(childTopic, childPayload)
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
                            val childPayload = jsonbHelper.toPGobject(mapOf("from" to rootHandler))
                            scope.launch(childTopic, childPayload)
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
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(100, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )
            Thread.sleep(100)

            Assertions.assertEquals(
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
                fluentJdbc.getEventSequence(),
            )

            Assertions.assertEquals(
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
                            val childPayload = jsonbHelper.toPGobject(mapOf("from" to rootHandler))
                            scope.launch(childTopic, childPayload)
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
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(1, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )
            Thread.sleep(500)

            Assertions.assertEquals(
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
                fluentJdbc.getEventSequence(),
            )

            Assertions.assertEquals(
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
                            val childPayload = jsonbHelper.toPGobject(mapOf("from" to rootHandler))
                            scope.launch(childTopic, childPayload)
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
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(1, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )
            Thread.sleep(200)

            Assertions.assertEquals(
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
                fluentJdbc.getEventSequence(),
            )

            Assertions.assertEquals(
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
                        val childPayload = jsonbHelper.toPGobject(mapOf("from" to "root-handler"))
                        scope.launch(childTopic, childPayload)
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
                        val grandchildPayload =
                            jsonbHelper.toPGobject(mapOf("from" to "child-handler-1"))
                        scope.launch(grandchildTopic, grandchildPayload)
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
        val grandchildSubscription = messageQueue.subscribe(grandchildTopic, grandChildCoroutine)

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(latch.await(2, TimeUnit.SECONDS))

            Thread.sleep(750)

            Assertions.assertEquals(18, executionOrder.size)

            Assertions.assertEquals(
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
            Assertions.assertEquals(
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

            Assertions.assertEquals(
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
                fluentJdbc
                    .getEventSequence()
                    .keepOnlyHandlers("root-handler", "child-handler-1", "grandchild-handler"),
            )

            Assertions.assertEquals(
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
                fluentJdbc.getEventSequence().keepOnlyHandlers("root-handler", "child-handler-2"),
            )

            val childHandler2RollingBackExceptions =
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLING_BACK", "child-handler-2")

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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLING_BACK", "root-handler")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLBACK_EMITTED", "root-handler")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                        rootHandlerCoroutine.identifier.asSource(),
                        listOf(
                            CooperationExceptionData(
                                "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLING_BACK", "child-handler-1")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                        rootHandlerCoroutine.identifier.asSource(),
                        listOf(
                            CooperationExceptionData(
                                "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLBACK_EMITTED", "child-handler-1")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${childHandler1Coroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                        childHandler1Coroutine.identifier.asSource(),
                        listOf(
                            CooperationExceptionData(
                                "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                                rootHandlerCoroutine.identifier.asSource(),
                                listOf(
                                    CooperationExceptionData(
                                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                                        "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLING_BACK", "grandchild-handler")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${childHandler1Coroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                        childHandler1Coroutine.identifier.asSource(),
                        listOf(
                            CooperationExceptionData(
                                "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                                rootHandlerCoroutine.identifier.asSource(),
                                listOf(
                                    CooperationExceptionData(
                                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                                        "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
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
                        val childPayload = jsonbHelper.toPGobject(mapOf("from" to "root-handler"))
                        scope.launch(childTopic, childPayload)
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
                        val grandchildPayload =
                            jsonbHelper.toPGobject(mapOf("from" to "child-handler-1"))
                        scope.launch(grandchildTopic, grandchildPayload)
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
                        error("Rollback failure")
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
        val grandchildSubscription = messageQueue.subscribe(grandchildTopic, grandChildCoroutine)

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(2, TimeUnit.SECONDS),
                "Latch has count ${latch.count}",
            )

            Thread.sleep(750)

            Assertions.assertEquals(16, executionOrder.size)

            Assertions.assertEquals(
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
            Assertions.assertEquals(
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

            Assertions.assertEquals(
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
                fluentJdbc
                    .getEventSequence()
                    .keepOnlyHandlers("root-handler", "child-handler-1", "grandchild-handler"),
            )

            Assertions.assertEquals(
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
                fluentJdbc.getEventSequence().keepOnlyHandlers("root-handler", "child-handler-2"),
            )

            val childHandler2RollingBackExceptions =
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLING_BACK", "child-handler-2")

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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLING_BACK", "root-handler")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLBACK_EMITTED", "root-handler")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                        rootHandlerCoroutine.identifier.asSource(),
                        listOf(
                            CooperationExceptionData(
                                "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLING_BACK", "child-handler-1")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                        rootHandlerCoroutine.identifier.asSource(),
                        listOf(
                            CooperationExceptionData(
                                "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLBACK_EMITTED", "child-handler-1")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${childHandler1Coroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                        childHandler1Coroutine.identifier.asSource(),
                        listOf(
                            CooperationExceptionData(
                                "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                                rootHandlerCoroutine.identifier.asSource(),
                                listOf(
                                    CooperationExceptionData(
                                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                                        "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLING_BACK", "grandchild-handler")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${childHandler1Coroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                        childHandler1Coroutine.identifier.asSource(),
                        listOf(
                            CooperationExceptionData(
                                "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                                rootHandlerCoroutine.identifier.asSource(),
                                listOf(
                                    CooperationExceptionData(
                                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                                        "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLBACK_FAILED", "grandchild-handler")

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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLBACK_FAILED", "child-handler-1")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${childHandler1Coroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRollbackFailedException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRollbackFailedException",
                        childHandler1Coroutine.identifier.asSource(),
                        listOf(
                            CooperationExceptionData(
                                "[${grandChildCoroutine.identifier.asSource()}] java.lang.IllegalStateException: Rollback failure",
                                "java.lang.IllegalStateException",
                                grandChildCoroutine.identifier.asSource(),
                            ),
                            CooperationExceptionData(
                                "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                                rootHandlerCoroutine.identifier.asSource(),
                                listOf(
                                    CooperationExceptionData(
                                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                                        "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLBACK_FAILED", "root-handler")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRollbackFailedException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRollbackFailedException",
                        rootHandlerCoroutine.identifier.asSource(),
                        listOf(
                            CooperationExceptionData(
                                "[${childHandler1Coroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRollbackFailedException: <no message>",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRollbackFailedException",
                                childHandler1Coroutine.identifier.asSource(),
                                listOf(
                                    CooperationExceptionData(
                                        "[${grandChildCoroutine.identifier.asSource()}] java.lang.IllegalStateException: Rollback failure",
                                        "java.lang.IllegalStateException",
                                        grandChildCoroutine.identifier.asSource(),
                                    ),
                                    CooperationExceptionData(
                                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException: <no message>",
                                        "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException",
                                        rootHandlerCoroutine.identifier.asSource(),
                                        listOf(
                                            CooperationExceptionData(
                                                "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                                                "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
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
                                    jsonbHelper.toPGobject(
                                        mapOf("from" to rootHandler, "phase" to "original")
                                    )
                                scope.launch(childTopic, childPayload)
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
                                        jsonbHelper.toPGobject(
                                            mapOf("from" to rootHandler, "phase" to "retry")
                                        )
                                    scope.launch(childTopic, childPayload)
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
                                val phase =
                                    jsonbHelper
                                        .fromPGobjectToMap<String, String>(message.payload)["phase"]
                                executionOrder.add("child-handler-1-$phase")
                                throw RuntimeException("Simulated failure to test rollback")
                            },
                            rollback = { scope, message, throwable ->
                                val phase =
                                    jsonbHelper
                                        .fromPGobjectToMap<String, String>(message.payload)["phase"]
                                executionOrder.add("child-handler-1-rollback-$phase")
                            },
                            handleChildFailures = { scope, message, throwable ->
                                val phase =
                                    jsonbHelper
                                        .fromPGobjectToMap<String, String>(message.payload)["phase"]
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
                                val phase =
                                    jsonbHelper
                                        .fromPGobjectToMap<String, String>(message.payload)["phase"]
                                executionOrder.add("child-handler-2-$phase")
                            },
                            rollback = { scope, message, throwable ->
                                val phase =
                                    jsonbHelper
                                        .fromPGobjectToMap<String, String>(message.payload)["phase"]
                                executionOrder.add("child-handler-2-rollback-$phase")
                            },
                            handleChildFailures = { scope, message, throwable ->
                                val phase =
                                    jsonbHelper
                                        .fromPGobjectToMap<String, String>(message.payload)["phase"]
                                executionOrder.add("child-handler-2-handleChildFailures-$phase")
                                throw throwable
                            },
                        )
                    },
                )

            try {
                val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
                fluentJdbc.transactional { connection ->
                    messageQueue.launch(connection, rootTopic, rootPayload)
                }

                Assertions.assertTrue(
                    latch.await(1, TimeUnit.SECONDS),
                    "Not everything completed correctly",
                )
                Thread.sleep(200)

                Assertions.assertEquals(
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
                    fluentJdbc
                        .getEventSequence()
                        .keepOnlyHandlers("root-handler", "child-handler-1"),
                )

                Assertions.assertEquals(
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
                    fluentJdbc
                        .getEventSequence()
                        .keepOnlyHandlers("root-handler", "child-handler-2"),
                )

                Assertions.assertEquals(
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

                Assertions.assertEquals(
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
                                val childPayload =
                                    jsonbHelper.toPGobject(mapOf("from" to "root-handler"))
                                scope.launch(childTopic, childPayload)
                            },
                            rollback = { scope, message, throwable -> rollbackLatch.countDown() },
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
                val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
                val cooperationRoot =
                    fluentJdbc.transactional { connection ->
                        messageQueue.launch(connection, rootTopic, rootPayload)
                    }

                Assertions.assertTrue(
                    latch.await(1, TimeUnit.SECONDS),
                    "Not everything completed correctly",
                )
                Thread.sleep(100)

                fluentJdbc.transactional { connection ->
                    structuredCooperationManager.rollback(
                        connection,
                        cooperationRoot.cooperationScopeIdentifier,
                        "master-system",
                        "feelz",
                    )
                }

                Assertions.assertTrue(
                    rollbackLatch.await(1, TimeUnit.SECONDS),
                    "Not everything rolled back correctly",
                )
                Thread.sleep(100)

                Assertions.assertEquals(
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
                    fluentJdbc.getEventSequence(),
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
                            val childPayload =
                                jsonbHelper.toPGobject(mapOf("from" to "root-handler"))
                            scope.launch(childTopic, childPayload)
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
                                cooperationScopeIdentifier = scope.scopeIdentifier
                                latch.countDown()
                            },
                            rollback = { scope, message, throwable -> rollbackLatch.countDown() },
                        )
                    },
                )

            try {
                val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
                fluentJdbc.transactional { connection ->
                    messageQueue.launch(connection, rootTopic, rootPayload)
                }

                Assertions.assertTrue(
                    latch.await(1, TimeUnit.SECONDS),
                    "Not everything completed correctly",
                )
                Thread.sleep(100)

                fluentJdbc.transactional { connection ->
                    structuredCooperationManager.rollback(
                        connection,
                        cooperationScopeIdentifier,
                        "master-system",
                        "feelz",
                    )
                }

                Assertions.assertTrue(
                    rollbackLatch.await(1, TimeUnit.SECONDS),
                    "Not everything rolled back correctly",
                )
                Thread.sleep(100)

                Assertions.assertEquals(
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
                    fluentJdbc.getEventSequence(),
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
                            val childPayload =
                                jsonbHelper.toPGobject(mapOf("from" to "root-handler"))
                            scope.launch(childTopic, childPayload)
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
                            cooperationScopeIdentifier = scope.scopeIdentifier
                        }
                    },
                )

            try {
                val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
                fluentJdbc.transactional { connection ->
                    messageQueue.launch(connection, rootTopic, rootPayload)
                }

                Assertions.assertTrue(
                    secondRootStepExecuting.await(1, TimeUnit.SECONDS),
                    "Second step didn't start executing",
                )

                fluentJdbc.transactional { connection ->
                    structuredCooperationManager.rollback(
                        connection,
                        cooperationScopeIdentifier,
                        "master-system",
                        "feelz",
                    )
                }

                rollbackEmitted.countDown()

                Assertions.assertTrue(
                    latch.await(1, TimeUnit.SECONDS),
                    "Not everything completed correctly",
                )
                Thread.sleep(100)

                Assertions.assertEquals(
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
                    fluentJdbc.getEventSequence(),
                )
            } finally {
                rootSubscription.close()
                childSubscription.close()
            }
        }
    }
}

data object TriedAgainKey : CooperationContext.MappedKey<TriedAgainValue>()

data object TriedAgainValue : CooperationContext.MappedElement(TriedAgainKey)
