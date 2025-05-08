package io.github.gabrielshanahan.structmess.domain

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.coroutine.CooperationContext
import io.github.gabrielshanahan.structmess.coroutine.readCooperationContext
import io.github.gabrielshanahan.structmess.coroutine.writeCooperationContext
import io.quarkus.test.junit.QuarkusTest
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
        val context = objectMapper.readCooperationContext(jsonString)
        assertEquals(jsonString, objectMapper.writeCooperationContext(context))
    }

    @Test
    fun `test everything works as expected`() {
        val jsonString = """{"TestKey":{"value":"test-value"},"unknown-key":"test-value"}"""
        val context = objectMapper.readCooperationContext(jsonString)
        assertEquals(jsonString, objectMapper.writeCooperationContext(context))

        assertEquals(TestElement("test-value"), context[TestKey])
        val unmappedKey = CooperationContext.UnmappedKey("unknown-key")
        assertEquals(
            CooperationContext.OpaqueElement(unmappedKey, "\"test-value\""),
            context[unmappedKey],
        )

        assertEquals(
            """{"TestKey":{"value":"different-test-value"},"unknown-key":"test-value"}""",
            objectMapper.writeCooperationContext(context + TestElement("different-test-value")),
        )
        assertEquals(
            """{"TestKey":{"value":"test-value"}}""",
            objectMapper.writeCooperationContext(context[TestKey]!!),
        )
        assertEquals(
            """{"TestKey":{"value":"test-value"}}""",
            objectMapper.writeCooperationContext(context - unmappedKey),
        )
        assertEquals(
            """{"unknown-key":"test-value"}""",
            objectMapper.writeCooperationContext(context[unmappedKey]!!),
        )
        assertEquals(
            """{"unknown-key":"test-value"}""",
            objectMapper.writeCooperationContext(context - TestKey),
        )
    }
}
