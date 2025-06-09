package io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.reactive.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.reactive.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.reactive.coroutine.getEventSequence
import io.github.gabrielshanahan.scoop.reactive.coroutine.keepOnlyHandlers
import io.github.gabrielshanahan.scoop.reactive.coroutine.keepOnlyPrefixedBy
import io.github.gabrielshanahan.scoop.reactive.messaging.eventLoopStrategy
import io.quarkus.test.junit.QuarkusTest
import io.vertx.core.json.JsonObject
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HappyPathTest : StructuredCooperationTest() {
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

            Assertions.assertTrue(
                latch.await(1, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )
            Thread.sleep(100)

            Assertions.assertEquals(4, executionOrder.size, "Not everything completed correctly")
            Assertions.assertEquals(
                listOf(
                    "root-handler-step-1",
                    "child-handler-step-1",
                    "child-handler-step-2",
                    "root-handler-step-2",
                ),
                executionOrder,
                "Execution order obeys structured cooperation rules",
            )

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
                    Triple("SUSPENDED", "1", "root-handler"),
                    Triple("COMMITTED", "1", "root-handler"),
                ),
                pool.getEventSequence(),
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

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")

            Thread.sleep(100)

            Assertions.assertEquals(7, executionOrder.size, "Not everything completed correctly")
            Assertions.assertEquals(
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

            Assertions.assertEquals(
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
                pool.getEventSequence(),
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

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")

            Thread.sleep(100)

            Assertions.assertEquals(6, executionOrder.size, "Not everything completed correctly")

            Assertions.assertEquals(
                listOf(
                    "root-handler-step-1",
                    "child-handler-1-step-1",
                    "child-handler-1-step-2",
                    "root-handler-step-2",
                ),
                executionOrder.keepOnlyPrefixedBy("root-handler", "child-handler-1"),
                "Execution order obeys structured cooperation rules",
            )

            Assertions.assertEquals(
                listOf(
                    "root-handler-step-1",
                    "child-handler-2-step-1",
                    "child-handler-2-step-2",
                    "root-handler-step-2",
                ),
                executionOrder.keepOnlyPrefixedBy("root-handler", "child-handler-2"),
                "Execution order obeys structured cooperation rules",
            )

            Assertions.assertEquals(
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
                pool.getEventSequence().keepOnlyHandlers("root-handler", "child-handler-1"),
            )

            Assertions.assertEquals(
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
                pool.getEventSequence().keepOnlyHandlers("root-handler", "child-handler-2"),
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

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")

            Thread.sleep(100)

            Assertions.assertEquals(6, executionOrder.size, "Not everything completed correctly")

            Assertions.assertEquals(
                listOf(
                    "root-handler-step-1",
                    "child-handler-1-step-1",
                    "child-handler-1-step-2",
                    "root-handler-step-2",
                ),
                executionOrder.keepOnlyPrefixedBy("root-handler", "child-handler-1"),
                "Execution order obeys structured cooperation rules",
            )

            Assertions.assertEquals(
                listOf(
                    "root-handler-step-1",
                    "child-handler-2-step-1",
                    "child-handler-2-step-2",
                    "root-handler-step-2",
                ),
                executionOrder.keepOnlyPrefixedBy("root-handler", "child-handler-2"),
                "Execution order obeys structured cooperation rules",
            )

            Assertions.assertEquals(
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
                pool.getEventSequence().keepOnlyHandlers("root-handler", "child-handler-1"),
            )

            Assertions.assertEquals(
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
                pool.getEventSequence().keepOnlyHandlers("root-handler", "child-handler-2"),
            )
        } finally {
            rootSubscription.close()
            childSubscription1.close()
            childSubscription2.close()
        }
    }
}
