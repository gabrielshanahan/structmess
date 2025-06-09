package io.github.gabrielshanahan.scoop.reactive.coroutine

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.StructuredCooperationManager
import io.github.gabrielshanahan.scoop.reactive.messaging.HandlerRegistry
import io.github.gabrielshanahan.scoop.reactive.messaging.MessageQueue
import io.vertx.mutiny.sqlclient.Pool
import jakarta.inject.Inject
import org.junit.jupiter.api.BeforeEach

abstract class StructuredCooperationTest {
    @Inject protected lateinit var messageQueue: MessageQueue

    @Inject protected lateinit var structuredCooperationManager: StructuredCooperationManager

    @Inject protected lateinit var handlerRegistry: HandlerRegistry

    @Inject protected lateinit var objectMapper: ObjectMapper

    @Inject protected lateinit var pool: Pool

    protected val rootTopic = "root-topic"
    protected val childTopic = "child-topic"
    protected val grandchildTopic = "grandchild-topic"

    @BeforeEach
    fun cleanupDatabase() {
        pool.executeAndAwaitPreparedQuery("TRUNCATE TABLE message_event, message CASCADE")
    }
}
