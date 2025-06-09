package io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationExceptionData
import io.github.gabrielshanahan.scoop.blocking.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.blocking.coroutine.asSource
import io.github.gabrielshanahan.scoop.blocking.coroutine.assertEquivalent
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.blocking.coroutine.fetchExceptions
import io.github.gabrielshanahan.scoop.blocking.coroutine.getEventSequence
import io.github.gabrielshanahan.scoop.blocking.messaging.eventLoopStrategy
import io.github.gabrielshanahan.scoop.blocking.transactional
import io.quarkus.test.junit.QuarkusTest
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CancellationTest : StructuredCooperationTest() {

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
                        val childPayload = jsonbHelper.toPGobject(mapOf("from" to "root-handler"))
                        scope.launch(childTopic, childPayload)
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
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            val cooperationRoot =
                fluentJdbc.transactional { connection ->
                    messageQueue.launch(connection, rootTopic, rootPayload)
                }

            childIsExecuting.await()
            fluentJdbc.transactional { connection ->
                structuredCooperationManager.cancel(
                    connection,
                    cooperationRoot.cooperationScopeIdentifier,
                    "master-system",
                    "feelz",
                )
            }
            cancellation.countDown()

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "Latch count is ${latch.count}")
            Thread.sleep(100)

            Assertions.assertEquals(3, executionOrder.size, "Not everything completed correctly")
            Assertions.assertEquals(
                listOf(
                    "root-handler-step-1",
                    "child-handler-step-1",
                    "root-handler-rollback-step-1",
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
                fluentJdbc.getEventSequence(),
            )

            val cancellationExceptions =
                fluentJdbc.fetchExceptions(jsonbHelper, "CANCELLATION_REQUESTED", null)

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[master-system] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CancellationRequestedException: feelz",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CancellationRequestedException",
                        "master-system",
                    )
                ),
                cancellationExceptions,
            )

            val childHandlerRollingBackExceptions =
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLING_BACK", "child-handler")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${childHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.GaveUpException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.GaveUpException",
                        childHandlerCoroutine.identifier.asSource(),
                        listOf(
                            CooperationExceptionData(
                                "[master-system] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CancellationRequestedException: feelz",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CancellationRequestedException",
                                "master-system",
                            )
                        ),
                    )
                ),
                childHandlerRollingBackExceptions,
            )

            val rootHandlerRollingBackExceptions =
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLING_BACK", "root-handler")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.GaveUpException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.GaveUpException",
                        "${rootHandlerCoroutine.identifier.asSource()}",
                        listOf(
                            CooperationExceptionData(
                                "[master-system] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CancellationRequestedException: feelz",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CancellationRequestedException",
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
                        val childPayload = jsonbHelper.toPGobject(mapOf("from" to "root-handler"))
                        scope.launch(childTopic, childPayload)
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
                structuredCooperationManager.cancel(
                    connection,
                    cooperationRoot.cooperationScopeIdentifier,
                    "master-system",
                    "feelz",
                )
            }

            Assertions.assertEquals(2, executionOrder.size, "Not everything completed correctly")
            Assertions.assertEquals(
                listOf("root-handler-step-1", "child-handler-step-1"),
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
                    Triple("COMMITTED", "0", "child-handler"),
                    Triple("COMMITTED", "0", "root-handler"),
                    Triple("CANCELLATION_REQUESTED", null, null),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
            childSubscription.close()
        }
    }
}
