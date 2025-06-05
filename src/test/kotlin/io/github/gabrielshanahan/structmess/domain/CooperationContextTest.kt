package io.github.gabrielshanahan.structmess.domain

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.coroutine.CooperationContext
import io.quarkus.test.junit.QuarkusTest
import io.vertx.core.json.JsonObject
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

object TestKey : CooperationContext.MappedKey<TestElement>()

data class TestElement(val value: String) : CooperationContext.MappedElement(TestKey)

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CooperationContextTest {

    @Inject lateinit var objectMapper: ObjectMapper

    @Test
    fun `test emtpy`() {
        val jsonString = "{}"
        val context = JsonObject(jsonString).mapTo(CooperationContext::class.java)
        assertEquals(jsonString, objectMapper.writeValueAsString(context))
    }

    @Test
    fun `test everything works as expected`() {
        val jsonString = """{"TestKey":{"value":"test-value"},"unknown-key":"test-value"}"""
        val context = JsonObject(jsonString).mapTo(CooperationContext::class.java)
        assertEquals(jsonString, objectMapper.writeValueAsString(context))

        assertEquals(TestElement("test-value"), context[TestKey])
        val unmappedKey = CooperationContext.UnmappedKey("unknown-key")
        assertEquals(
            CooperationContext.OpaqueElement(unmappedKey, "\"test-value\""),
            context[unmappedKey],
        )

        val actual = objectMapper.writeValueAsString(context + TestElement("different-test-value"))

        assertEquals(
            """{"TestKey":{"value":"different-test-value"},"unknown-key":"test-value"}""",
            actual,
        )
        assertEquals(
            """{"TestKey":{"value":"different-test-value"}}""",
            objectMapper.writeValueAsString(
                TestElement("test-value") + TestElement("different-test-value")
            ),
        )
        assertEquals(
            """{"TestKey":{"value":"test-value"}}""",
            objectMapper.writeValueAsString(context[TestKey]!!),
        )
        assertEquals(
            """{"TestKey":{"value":"test-value"}}""",
            objectMapper.writeValueAsString(context - unmappedKey),
        )
        assertEquals(
            """{"unknown-key":"test-value"}""",
            objectMapper.writeValueAsString(context[unmappedKey]!!),
        )
        assertEquals(
            """{"unknown-key":"test-value"}""",
            objectMapper.writeValueAsString(context - TestKey),
        )
    }
}
