package io.github.gabrielshanahan.structmess.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.messaging.MessageQueue
import io.vertx.mutiny.sqlclient.Pool
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import java.net.URI

@Path("/messages")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class MessageResource(
    private val messageQueue: MessageQueue,
    private val objectMapper: ObjectMapper,
    private val client: Pool,
) {

    /** Send a message to a topic. */
    @POST
    @Path("/{topic}")
    fun sendMessage(@PathParam("topic") topic: String, payload: Map<String, Any>): Response {
        val jsonNode = objectMapper.valueToTree<JsonNode>(payload)
        val message =
            client
                .withTransaction { connection -> messageQueue.publish(connection, topic, jsonNode) }
                .await()
                .indefinitely()

        return Response.created(URI.create("/messages/${message.id}"))
            .entity(
                mapOf(
                    "id" to message.id,
                    "topic" to message.topic,
                    "createdAt" to message.createdAt,
                )
            )
            .build()
    }
}
