package io.github.gabrielshanahan.scoop.blocking.messaging

import io.github.gabrielshanahan.scoop.blocking.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.blocking.coroutine.fetchExceptions
import io.github.gabrielshanahan.scoop.blocking.transactional
import io.quarkus.test.junit.QuarkusTest
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresMessageQueueExceptionTest : StructuredCooperationTest() {

    @Test
    fun `test exception is stored and can be retrieved`() {
        val latch = CountDownLatch(1)
        lateinit var messageId: UUID
        val customExceptionMessage = "Custom exception for testing exception storage"

        val rootHandler = "exception-test-handler"
        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga(rootHandler, handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        messageId = message.id
                        latch.countDown()
                        throw CustomTestException(customExceptionMessage)
                    }
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("test" to "exception-storage"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            assertTrue(latch.await(1, TimeUnit.SECONDS), "Handler should complete")
            Thread.sleep(500)

            val exceptions = fluentJdbc.fetchExceptions(jsonbHelper, "ROLLING_BACK", rootHandler)

            assertEquals(1, exceptions.size, "We should have a single exception")
            val exception = exceptions.single()
            assertNotNull(exception, "Exception should be stored and retrieved")
            assertEquals(
                "io.github.gabrielshanahan.scoop.blocking.messaging.PostgresMessageQueueExceptionTest${'$'}CustomTestException",
                exception.type,
                "Exception should be of the correct type",
            )
            assertTrue(
                exception.message.endsWith(customExceptionMessage),
                "Exception should have the correct message",
            )
        } finally {
            rootSubscription.close()
        }
    }

    class CustomTestException(message: String) : Exception(message)
}
