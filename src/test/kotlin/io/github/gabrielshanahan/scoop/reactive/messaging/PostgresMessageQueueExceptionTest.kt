package io.github.gabrielshanahan.scoop.reactive.messaging

import io.github.gabrielshanahan.scoop.reactive.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.reactive.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.reactive.coroutine.executeAndAwaitPreparedQuery
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import io.quarkus.test.junit.QuarkusTest
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.SqlClient
import io.vertx.mutiny.sqlclient.Tuple
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresMessageQueueExceptionTest : StructuredCooperationTest() {

    @BeforeEach
    fun cleanup() {
        pool.executeAndAwaitPreparedQuery("TRUNCATE TABLE message_event, message CASCADE")
    }

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
            val rootPayload = JsonObject().put("test", "exception-storage")
            pool
                .withTransaction { connection ->
                    messageQueue.launch(connection, rootTopic, rootPayload)
                }
                .await()
                .indefinitely()

            assertTrue(latch.await(1, TimeUnit.SECONDS), "Handler should complete")
            Thread.sleep(500)

            val exceptions =
                pool
                    .withConnection { connection ->
                        fetchCancellationExceptions(
                            connection,
                            messageId,
                            "ROLLING_BACK",
                            rootHandler,
                        )
                    }
                    .await()
                    .indefinitely()

            assertEquals(1, exceptions.size, "We should have a single exception")
            val exception = exceptions.single()
            assertNotNull(exception, "Exception should be stored and retrieved")
            assertEquals(
                "io.github.gabrielshanahan.scoop.reactive.messaging.PostgresMessageQueueExceptionTest${'$'}CustomTestException",
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

    fun fetchCancellationExceptions(
        client: SqlClient,
        messageId: UUID,
        type: String,
        coroutineName: String,
    ): Uni<List<CooperationException>> =
        client
            .preparedQuery(
                """
                    SELECT COALESCE(JSON_AGG(exception), '[]'::json) as exceptions 
                    FROM message_event 
                    WHERE message_id = $1 AND type = $2 AND coroutine_name = $3
                    """
                    .trimIndent()
            )
            .execute(Tuple.of(messageId, type, coroutineName))
            .map { rowSet ->
                rowSet.single().getJsonArray("exceptions").map { exceptionJson ->
                    val cooperationFailure =
                        (exceptionJson as JsonObject).mapTo(CooperationFailure::class.java)
                    CooperationFailure.toCooperationException(cooperationFailure)
                }
            }

    class CustomTestException(message: String) : Exception(message)
}
