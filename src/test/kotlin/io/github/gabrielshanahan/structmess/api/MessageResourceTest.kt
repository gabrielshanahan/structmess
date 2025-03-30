package io.github.gabrielshanahan.structmess.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import jakarta.inject.Inject
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.CoreMatchers.notNullValue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@QuarkusTest
class MessageResourceTest {

    @Inject lateinit var objectMapper: ObjectMapper

    private val testTopic = "test-topic"
    private lateinit var testPayload: Map<String, Any>
    private lateinit var testJsonNode: JsonNode

    @BeforeEach
    fun setup() {
        testPayload = mapOf("message" to "Hello, world!", "priority" to "HIGH")
        testJsonNode = objectMapper.valueToTree(testPayload)
    }

    @Test
    fun `sendMessage should publish a message and return response`() {
        given()
            .contentType("application/json")
            .body(testPayload)
            .`when`()
            .post("/messages/$testTopic")
            .then()
            .statusCode(201)
            .body("id", notNullValue())
            .body("topic", equalTo(testTopic))
            .body("createdAt", notNullValue())
    }
}
