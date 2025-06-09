package io.github.gabrielshanahan.scoop.blocking.coroutine.builder

import io.github.gabrielshanahan.scoop.blocking.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.blocking.coroutine.getEventSequence
import io.github.gabrielshanahan.scoop.blocking.messaging.eventLoopStrategy
import io.github.gabrielshanahan.scoop.blocking.transactional
import io.quarkus.test.junit.QuarkusTest
import java.time.Instant
import java.time.OffsetDateTime
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SleepTest : StructuredCooperationTest() {

    @Test
    fun `sleep works`() {
        val latch = CountDownLatch(1)
        lateinit var step1Time: Instant
        lateinit var step3Time: Instant

        val rootHandlerCoroutine =
            saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                step(invoke = { scope, message -> step1Time = Instant.now() })
                sleepForStep(500.milliseconds)
                step(
                    invoke = { scope, message ->
                        step3Time = Instant.now()
                        latch.countDown()
                    }
                )
            }
        val rootSubscription = messageQueue.subscribe(rootTopic, rootHandlerCoroutine)

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
            Thread.sleep(200)

            Assertions.assertTrue(
                step3Time.toEpochMilli() - step1Time.toEpochMilli() > 500,
                "Sleep doesn't work",
            )
            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("SUSPENDED", "0", "root-handler"),
                    Triple("EMITTED", "1", "root-handler"),
                    Triple("SUSPENDED", "1", "root-handler"),
                    Triple("SEEN", null, "sleep-handler"),
                    Triple("SUSPENDED", "sleep", "sleep-handler"),
                    Triple("COMMITTED", "sleep", "sleep-handler"),
                    Triple("SUSPENDED", "2", "root-handler"),
                    Triple("COMMITTED", "2", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Test
    fun `scheduling works`() {
        val latch = CountDownLatch(1)
        lateinit var scheduledStepTime: OffsetDateTime
        val startAfter = OffsetDateTime.now().plus(500.milliseconds.toJavaDuration())

        val rootHandlerCoroutine =
            saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                scheduledStep(
                    startAfter,
                    { scope, message ->
                        scheduledStepTime = OffsetDateTime.now()
                        latch.countDown()
                    },
                )
            }
        val rootSubscription = messageQueue.subscribe(rootTopic, rootHandlerCoroutine)

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
            Thread.sleep(200)

            Assertions.assertTrue(scheduledStepTime > startAfter, "Scheduling doesn't work")
            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("EMITTED", "0 (waiting for scheduled time)", "root-handler"),
                    Triple("SUSPENDED", "0 (waiting for scheduled time)", "root-handler"),
                    Triple("SEEN", null, "sleep-handler"),
                    Triple("SUSPENDED", "sleep", "sleep-handler"),
                    Triple("COMMITTED", "sleep", "sleep-handler"),
                    Triple("SUSPENDED", "0", "root-handler"),
                    Triple("COMMITTED", "0", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Test
    fun `periodic scheduling works`() {
        val latch = CountDownLatch(3)
        val runEvery = 400.milliseconds
        val times = Collections.synchronizedList(mutableListOf<Instant>())

        val rootHandlerCoroutine =
            saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                periodic(runEvery, 3)
                step { scope, message ->
                    times.add(Instant.now())
                    latch.countDown()
                }
            }
        val rootSubscription = messageQueue.subscribe(rootTopic, rootHandlerCoroutine)

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(latch.await(2, TimeUnit.SECONDS), "All handlers should complete")
            Thread.sleep(500)
            Assertions.assertTrue(
                times.size == 3 &&
                    times[1].toEpochMilli() - times[0].toEpochMilli() >
                        runEvery.inWholeMilliseconds &&
                    times[2].toEpochMilli() - times[1].toEpochMilli() >
                        runEvery.inWholeMilliseconds,
                "Periodic scheduling doesn't work: $times",
            )
        } finally {
            rootSubscription.close()
        }
    }
}
