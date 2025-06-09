package io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.StackTraceFrame
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CooperationFailureTest {

    @Inject lateinit var objectMapper: ObjectMapper

    @Test
    fun `test deserialization of unknown failure type`() {
        val cooperationFailure =
            CooperationFailure(
                message = "Non-zero exit status returned from process",
                type = "ExitStatus",
                source = "external-system",
                stackTrace =
                    listOf(
                        StackTraceFrame(
                            fileName = "script.sh",
                            lineNumber = 42,
                            className = null,
                            functionName = null,
                        )
                    ),
            )

        val throwable = CooperationFailure.toCooperationException(cooperationFailure)

        assertEquals(
            "[external-system] ExitStatus: Non-zero exit status returned from process",
            throwable.message,
        )
        assertEquals("ExitStatus", throwable.type)
        assertEquals("external-system", throwable.source)
        assertStackTracesEquivalent(cooperationFailure.stackTrace, throwable.stackTrace)
    }

    @Test
    fun `test idempotence of mapping Throwable - CooperationFailure - JSON - CooperationFailure - Throwable`() {
        val cause = IllegalArgumentException("This is the cause")
        val originalException = RuntimeException("Test exception", cause)

        // Truncate stacktraces so they don't depend on the ways tests are run
        cause.stackTrace =
            cause.stackTrace.takeWhile { !it.className.startsWith("org.junit") }.toTypedArray()
        originalException.stackTrace =
            originalException.stackTrace
                .takeWhile { !it.className.startsWith("org.junit") }
                .toTypedArray()

        val originalCooperationFailure =
            CooperationFailure.fromThrowable(originalException, "test-system")

        val originalCooperationFailureJson =
            objectMapper
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(originalCooperationFailure)
        val deserializedCooperationFailure =
            objectMapper.readValue(originalCooperationFailureJson, CooperationFailure::class.java)

        val cooperationException =
            CooperationFailure.toCooperationException(deserializedCooperationFailure)
        val mappedCooperationFailure =
            CooperationFailure.fromThrowable(cooperationException, cooperationException.source)

        assertThrowablesEquivalent(originalException, cooperationException)
        assertEquals(originalCooperationFailure, mappedCooperationFailure)

        assertEquals(
            "[test-system] java.lang.RuntimeException: Test exception",
            cooperationException.message,
        )
        assertEquals(EXCEPTION_JSON.trimIndent(), originalCooperationFailureJson)
    }
}

private fun assertThrowablesEquivalent(expected: Throwable, actual: CooperationException) {
    assertEquals(
        expected.javaClass.name,
        actual.type,
        "Classes should be equal - expected ${expected.javaClass.name}, got ${actual.type}",
    )
    assertTrue(actual.message.endsWith(expected.message ?: ""))
    assertStackTracesEquivalent(expected.stackTrace, actual.stackTrace)

    val expectedCause = expected.cause
    val actualCause = actual.cause
    if (expectedCause != null && actualCause != null) {
        assertThrowablesEquivalent(expectedCause, actualCause)
    } else {
        assertEquals(
            expectedCause,
            actualCause,
            "Causes should both be null or both not be null - expected $expectedCause, got $actualCause",
        )
    }

    assertEquals(
        expected.suppressed.size,
        actual.suppressed.size,
        "Number of suppressed exceptions should be equal - expected ${expected.suppressed.size}, got ${actual.suppressed.size}",
    )
    (listOfNotNull(expected.cause) + expected.suppressed).zip(actual.causes).forEach {
        (expectedSuppressed, actualSuppressed) ->
        assertThrowablesEquivalent(expectedSuppressed, actualSuppressed)
    }
}

private fun assertStackTracesEquivalent(
    expected: Array<StackTraceElement>,
    actual: Array<StackTraceElement>,
) {
    assertEquals(
        expected.size,
        actual.size,
        "Stack traces have different size - expected ${expected.size}, got ${actual.size}",
    )
    expected.zip(actual).forEach { (expectedElement, actualElement) ->
        assertEquals(
            expectedElement.fileName,
            actualElement.fileName,
            "Stack trace elements have different file names - expected ${expectedElement.fileName}, got ${actualElement.fileName}",
        )
        assertEquals(
            expectedElement.lineNumber,
            actualElement.lineNumber,
            "Stack trace elements have different line numbers - expected ${expectedElement.lineNumber}, got ${actualElement.lineNumber}",
        )
        assertEquals(
            expectedElement.className,
            actualElement.className,
            "Stack trace elements have different classes - expected ${expectedElement.className}, got ${actualElement.className}",
        )
        assertEquals(
            expectedElement.methodName,
            actualElement.methodName,
            "Stack trace elements have different methods - expected ${expectedElement.methodName}, got ${actualElement.methodName}",
        )
    }
}

private fun assertStackTracesEquivalent(
    expected: List<StackTraceFrame>,
    actual: Array<StackTraceElement>,
) {
    assertEquals(
        expected.size,
        actual.size,
        "Stack traces have different size - expected ${expected.size}, got ${actual.size}",
    )
    expected.zip(actual).forEach { (expectedElement, actualElement) ->
        assertEquals(
            expectedElement.fileName,
            actualElement.fileName,
            "Stack traces have different file names - expected ${expectedElement.fileName}, got ${actualElement.fileName}",
        )
        assertEquals(
            expectedElement.lineNumber,
            actualElement.lineNumber,
            "Stack traces have different line numbers - expected ${expectedElement.lineNumber}, got ${actualElement.lineNumber}",
        )
        assertEquals(
            expectedElement.className ?: "<unknown class>",
            actualElement.className,
            "Stack traces have different classes - expected ${expectedElement.className ?: "<unknown class>"}, got ${actualElement.className}",
        )
        assertEquals(
            expectedElement.functionName ?: "<unknown function>",
            actualElement.methodName,
            "Stack traces have different methods - expected ${expectedElement.functionName ?: "<unknown function>"}, got ${actualElement.methodName}",
        )
    }
}

private const val EXCEPTION_JSON =
    """
    {
      "message" : "Test exception",
      "type" : "java.lang.RuntimeException",
      "source" : "test-system",
      "stackTrace" : [ {
        "fileName" : "CooperationFailureTest.kt",
        "lineNumber" : 52,
        "className" : "io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.CooperationFailureTest",
        "functionName" : "test idempotence of mapping Throwable - CooperationFailure - JSON - CooperationFailure - Throwable"
      }, {
        "fileName" : "DirectMethodHandleAccessor.java",
        "lineNumber" : 103,
        "className" : "jdk.internal.reflect.DirectMethodHandleAccessor",
        "functionName" : "invoke"
      }, {
        "fileName" : "Method.java",
        "lineNumber" : 580,
        "className" : "java.lang.reflect.Method",
        "functionName" : "invoke"
      }, {
        "fileName" : "QuarkusTestExtension.java",
        "lineNumber" : 960,
        "className" : "io.quarkus.test.junit.QuarkusTestExtension",
        "functionName" : "runExtensionMethod"
      }, {
        "fileName" : "QuarkusTestExtension.java",
        "lineNumber" : 810,
        "className" : "io.quarkus.test.junit.QuarkusTestExtension",
        "functionName" : "interceptTestMethod"
      } ],
      "causes" : [ {
        "message" : "This is the cause",
        "type" : "java.lang.IllegalArgumentException",
        "source" : "test-system",
        "stackTrace" : [ {
          "fileName" : "CooperationFailureTest.kt",
          "lineNumber" : 51,
          "className" : "io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.CooperationFailureTest",
          "functionName" : "test idempotence of mapping Throwable - CooperationFailure - JSON - CooperationFailure - Throwable"
        }, {
          "fileName" : "DirectMethodHandleAccessor.java",
          "lineNumber" : 103,
          "className" : "jdk.internal.reflect.DirectMethodHandleAccessor",
          "functionName" : "invoke"
        }, {
          "fileName" : "Method.java",
          "lineNumber" : 580,
          "className" : "java.lang.reflect.Method",
          "functionName" : "invoke"
        }, {
          "fileName" : "QuarkusTestExtension.java",
          "lineNumber" : 960,
          "className" : "io.quarkus.test.junit.QuarkusTestExtension",
          "functionName" : "runExtensionMethod"
        }, {
          "fileName" : "QuarkusTestExtension.java",
          "lineNumber" : 810,
          "className" : "io.quarkus.test.junit.QuarkusTestExtension",
          "functionName" : "interceptTestMethod"
        } ],
        "causes" : [ ]
      } ]
    }
"""
