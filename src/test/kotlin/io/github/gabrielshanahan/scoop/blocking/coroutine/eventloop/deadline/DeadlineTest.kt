package io.github.gabrielshanahan.scoop.blocking.coroutine.eventloop.deadline

import io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationExceptionData
import io.github.gabrielshanahan.scoop.blocking.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.blocking.coroutine.asSource
import io.github.gabrielshanahan.scoop.blocking.coroutine.assertEquivalent
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.blocking.coroutine.fetchExceptions
import io.github.gabrielshanahan.scoop.blocking.coroutine.getEventSequence
import io.github.gabrielshanahan.scoop.blocking.messaging.eventLoopStrategy
import io.github.gabrielshanahan.scoop.blocking.transactional
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline.HappyPathDeadline
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline.happyPathTimeout
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline.noHappyPathTimeout
import io.quarkus.test.junit.QuarkusTest
import java.time.format.DateTimeFormatter
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeadlineTest : StructuredCooperationTest() {

    @Test
    fun `happy path deadlines work`() {
        val latch = CountDownLatch(1)
        val childStarted = AtomicBoolean(false)
        val deadline = AtomicReference<HappyPathDeadline>()

        val rootHandlerCoroutine =
            saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                step(
                    invoke = { scope, message ->
                        val payload = jsonbHelper.toPGobject(mapOf("from" to "root-handler"))

                        deadline.set(happyPathTimeout(Duration.Companion.ZERO, "root handler"))
                        scope.launch(childTopic, payload, deadline.get())
                    },
                    rollback = { _, _, _ -> latch.countDown() },
                )
            }
        val rootSubscription = messageQueue.subscribe(rootTopic, rootHandlerCoroutine)

        val childHandlerCoroutine =
            saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                step { scope, message ->
                    childStarted.set(true)
                    Thread.sleep(Duration.Companion.INFINITE.toJavaDuration())
                }
            }
        val childSubscription = messageQueue.subscribe(childTopic, childHandlerCoroutine)

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(
                    connection,
                    rootTopic,
                    rootPayload,
                    noHappyPathTimeout("root system"),
                )
            }

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
            Thread.sleep(200)

            Assertions.assertFalse(
                childStarted.get(),
                "Child shouldn't even start, since shouldGiveUp() is called before (and after) the handler actually runs",
            )
            Assertions.assertEquals(
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
                fluentJdbc.getEventSequence(),
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
                fluentJdbc.fetchExceptions(jsonbHelper, "ROLLING_BACK", "root-handler")

            assertEquivalent(
                listOf(
                    CooperationExceptionData(
                        "[${rootHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException: <no message>",
                        "io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.ChildRolledBackException",
                        rootHandlerCoroutine.identifier.asSource(),
                        listOf(
                            CooperationExceptionData(
                                "[${childHandlerCoroutine.identifier.asSource()}] io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.GaveUpException: <no message>",
                                "io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.GaveUpException",
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
