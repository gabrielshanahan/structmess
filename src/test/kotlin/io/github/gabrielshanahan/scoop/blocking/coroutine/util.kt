package io.github.gabrielshanahan.scoop.blocking.coroutine

import io.github.gabrielshanahan.scoop.blocking.JsonbHelper
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import org.codejargon.fluentjdbc.api.FluentJdbc
import org.junit.jupiter.api.Assertions.assertEquals

// TODO: Doc all this! And probably move some to share
fun List<Triple<String, String?, String?>>.printEventSequenceCode():
    List<Triple<String, String?, String?>> = also {
    println(
        joinToString(prefix = "listOf(\n\t", postfix = "\n)", separator = ",\n\t") {
            "Triple(${it.first.let { "\"$it\""}}, ${it.second?.let { "\"$it\""}}, ${it.third?.let { "\"$it\""}})"
        }
    )
}

fun FluentJdbc.getEventSequence(): List<Triple<String, String?, String?>> =
    query()
        .select("SELECT type, step, coroutine_name FROM message_event ORDER BY created_at")
        .listResult {
            Triple(it.getString("type"), it.getString("step"), it.getString("coroutine_name"))
        }

fun List<Triple<String, String?, String?>>.keepOnlyHandlers(
    vararg handlers: String
): List<Triple<String, String?, String?>> = filter { it.third == null || it.third in handlers }

fun List<String>.keepOnlyPrefixedBy(vararg elements: String): List<String> = filter {
    elements.any { prefix -> it.startsWith(prefix) }
}

fun FluentJdbc.fetchExceptions(
    jsonbHelper: JsonbHelper,
    type: String,
    coroutineName: String?,
): List<CooperationException> =
    query()
        .select(
            """
                SELECT exception 
                FROM message_event 
                WHERE type = :type::message_event_type AND coroutine_name ${if (coroutineName == null) "IS NULL" else "= :coroutineName"}
            """
                .trimIndent()
        )
        .namedParam("type", type)
        .namedParam("coroutineName", coroutineName)
        .listResult {
            CooperationFailure.toCooperationException(
                jsonbHelper.fromPGobject<CooperationFailure>(it.getObject("exception"))
            )
        }

data class CooperationExceptionData(
    val message: String,
    val type: String,
    val source: String,
    val causes: List<CooperationExceptionData> = emptyList(),
)

fun List<CooperationException>.printExceptionDataCode() = apply {
    println(asExceptionData().constructorString())
}

fun assertEquivalent(expected: List<CooperationExceptionData>, actual: List<CooperationException>) {
    assertEquals(
        expected.size,
        actual.size,
        "Sizes don't match - expected $expected, but got $actual",
    )
    expected.zip(actual).forEach { (expected, actual) ->
        assertEquals(
            expected.message,
            actual.message,
            "Messages don't match - expected ${expected.message}, but got ${actual.message}",
        )
        assertEquals(
            expected.type,
            actual.type,
            "Types don't match - expected ${expected.type}, but got ${actual.type}",
        )
        assertEquals(
            expected.source,
            actual.source,
            "Sources don't match - expected ${expected.source}, but got ${actual.source}",
        )
        assertEquivalent(expected.causes, actual.causes)
    }
}

fun DistributedCoroutineIdentifier.asSource() = "$name[$instance]"

fun List<CooperationException>.asExceptionData(): List<CooperationExceptionData> = map {
    CooperationExceptionData(it.message, it.type, it.source, it.causes.asExceptionData())
}

fun List<CooperationExceptionData>.constructorString(tabs: String = ""): String =
    """
        |${tabs}listOf(
        |${joinToString(",\n") { it.constructorString("$tabs\t") }}
        |${tabs}),
        |
    """
        .trimMargin()
        .takeIf { isNotEmpty() } ?: ""

fun CooperationExceptionData.constructorString(tabs: String = ""): String =
    """
        |${tabs}CooperationExceptionData(
        |$tabs    "[${source.sourceAsCode()}]${message.substringAfterLast(']')}",
        |$tabs    "$type",
        |$tabs    "${source.sourceAsCode()}", 
        |${causes.constructorString("$tabs\t")}${tabs})
    """
        .trimMargin()

fun String.sourceAsCode() =
    variableName.takeIf { it.isNotBlank() }?.let { "${'$'}{$it.identifier.asSource()}" } ?: this

val String.variableName: String
    get() =
        when {
            contains("root-handler") -> "rootHandlerCoroutine"
            contains("child-handler-1") -> "childHandler1Coroutine"
            contains("child-handler-2") -> "childHandler2Coroutine"
            contains("child-handler") -> "childHandlerCoroutine"
            contains("grandchild") -> "grandChildCoroutine"
            else -> ""
        }
