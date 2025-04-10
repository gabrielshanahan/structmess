package io.github.gabrielshanahan.structmess.examples

// import com.fasterxml.jackson.databind.ObjectMapper
// import io.github.gabrielshanahan.structmess.domain.Message
// import io.github.gabrielshanahan.structmess.messaging.MessageQueue
// import io.quarkus.runtime.StartupEvent
// import io.smallrye.mutiny.Uni
// import io.vertx.mutiny.sqlclient.Pool
// import jakarta.enterprise.context.ApplicationScoped
// import jakarta.enterprise.event.Observes
// import java.time.Duration
// import java.util.concurrent.CompletableFuture
// import org.jboss.logging.Logger
//
/// **
// * Example demonstrating how to use the message queue system. This class shows both producing and
// * consuming messages.
// */
// @ApplicationScoped
// class MessageProcessingExample(
//    private val messageQueue: MessageQueue,
//    private val objectMapper: ObjectMapper,
//    private val client: Pool,
// ) {
//    private val logger = Logger.getLogger(javaClass)
//
//    companion object {
//        private const val STARTUP_DELAY_MS = 5000
//        private const val PRODUCER_DELAY_MS = 1000
//        private const val PROCESSING_TIME_MS = 500
//        private const val MESSAGE_COUNT = 10
//        private const val FAIL_MODULO = 4
//        private const val EXAMPLE_TOPIC = "example" // Using a constant for the topic name
//    }
//
//    /** On application startup, start a producer and consumer. */
//    fun onStart(@Observes @Suppress("UNUSED_PARAMETER") startupEvent: StartupEvent) {
//        logger.info("Starting message processing example")
//
//        // Start the example in a separate thread to not block startup
//        CompletableFuture.runAsync {
//            // Wait a bit for application to be fully initialized
//            Thread.sleep(STARTUP_DELAY_MS.toLong())
//
//            // Start a consumer
//            startConsumer()
//
//            // Start a producer
//            startProducer()
//        }
//    }
//
//    /** Starts a consumer that processes messages from the "example" topic. */
//    private fun startConsumer() {
//        logger.info("Starting example message consumer")
//
//        messageQueue.subscribe(EXAMPLE_TOPIC) { connection, message ->
//            processMessage(message).onItem().transform { success ->
//                ProcessingResult(message, success)
//            }
//        }
//    }
//
//    /** Starts a producer that sends messages to the "example" topic. */
//    private fun startProducer() {
//        logger.info("Starting example message producer")
//
//        // Produce example messages
//        for (i in 1..MESSAGE_COUNT) {
//            val payload =
//                objectMapper
//                    .createObjectNode()
//                    .put("index", i)
//                    .put("message", "This is example message $i")
//                    .put("timestamp", System.currentTimeMillis())
//
//            val message =
//                client
//                    .withTransaction { connection ->
//                        messageQueue.publish(connection, EXAMPLE_TOPIC, payload)
//                    }
//                    .await()
//                    .indefinitely()
//
//            logger.info("Sent message ${message.id}")
//
//            // Wait between messages
//            Thread.sleep(PRODUCER_DELAY_MS.toLong())
//        }
//    }
//
//    /** Example of processing a message. */
//    private fun processMessage(message: Message): Uni<Boolean> {
//        return Uni.createFrom()
//            .item {
//                try {
//                    // Process the payload
//                    logger.info("Processing message: ${message.payload}")
//
//                    // Simulate processing time
//                    Thread.sleep(PROCESSING_TIME_MS.toLong())
//
//                    // Simulate occasional failures
//                    val index = message.payload.get("index")?.asInt() ?: 0
//                    val shouldFail = index % FAIL_MODULO == 0
//
//                    if (shouldFail) {
//                        logger.warn("Simulating failure for message $index")
//                        false
//                    } else {
//                        true
//                    }
//                } catch (e: InterruptedException) {
//                    logger.error("Processing was interrupted", e)
//                    false
//                } catch (e: IllegalArgumentException) {
//                    logger.error("Invalid message arguments", e)
//                    false
//                }
//            }
//            .onItem()
//            .delayIt()
//            .by(Duration.ofMillis(PROCESSING_TIME_MS.toLong()))
//    }
//
//    private data class ProcessingResult(
//        val message: Message,
//        val success: Boolean,
//        val error: Throwable? = null,
//    )
// }
