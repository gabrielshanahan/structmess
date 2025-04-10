package io.github.gabrielshanahan.structmess.domain

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.structmess.domain.CooperationContext.Key
import java.io.StringReader
import java.lang.reflect.ParameterizedType

interface CooperationContext {

    operator fun <E : Element> get(key: Key<E>): E?

    operator fun plus(context: CooperationContext): CooperationContext =
        when (context) {
            is Element ->
                CooperationContextMap(mutableMapOf(), mutableMapOf(context.key to context)) + this
            else -> context.fold(this, CooperationContext::plus)
        }

    operator fun minus(key: Key<*>): CooperationContext =
        if (get(key) == null) {
            this
        } else {
            CooperationContextMap(mutableMapOf(), mutableMapOf()) + this - key
        }

    fun <R> fold(initial: R, operation: (R, Element) -> R): R

    // TODO: Doc that done this way to force keys to be singletons
    // TODO: Doc that the SIMPLE name is used
    sealed interface Key<E : Element>

    data class UnmappedKey(val key: String) : Key<OpaqueElement>

    abstract class MappedKey<E : MappedElement> : Key<E> {
        init {
            if (mappedElementClass == null) {
                throw IllegalStateException(
                    "${javaClass.name} must be parameterized with the element class."
                )
            }
        }

        val mappedElementClass: Class<*>?
            @JsonIgnore
            get() =
                (javaClass.genericSuperclass as? ParameterizedType)
                    ?.actualTypeArguments
                    ?.firstOrNull()
                    ?.let {
                        when (it) {
                            is Class<*> -> it
                            is ParameterizedType -> it.rawType as? Class<*>
                            else -> null
                        }
                    }
    }

    /**
     * An element of the CooperationContext. An element of the cooperation context is itself a
     * singleton context.
     */
    sealed interface Element : CooperationContext {
        @get:JsonIgnore val key: Key<*>

        override operator fun <E : Element> get(key: Key<E>): E? =
            @Suppress("UNCHECKED_CAST") if (this.key == key) this as E else null

        override fun <R> fold(initial: R, operation: (R, Element) -> R): R =
            operation(initial, this)
    }

    data class OpaqueElement(override val key: UnmappedKey, internal val json: String) : Element

    abstract class MappedElement(override val key: MappedKey<*>) : Element
}

val Key<*>.serializedValue: String
    get() =
        when (this) {
            is CooperationContext.UnmappedKey -> key
            is CooperationContext.MappedKey<*> -> this::class.simpleName!!
        }

fun emptyContext(): CooperationContext = CooperationContextMap(mutableMapOf(), mutableMapOf())

data class CooperationContextMap(
    private val serializedMap: MutableMap<String, String>,
    private val deserializedMap: MutableMap<Key<*>, CooperationContext.Element>,
    private val objectMapper: ObjectMapper? = null,
) : CooperationContext {

    // TODO: Doc that only deserialize when needed, so small cost unless you actually use it
    @Suppress("UNCHECKED_CAST")
    override fun <E : CooperationContext.Element> get(key: Key<E>): E? {
        if (deserializedMap[key] != null || objectMapper == null) {
            return (deserializedMap[key] as E?)
        }

        val serializedElement = serializedMap[key.serializedValue] ?: return null
        val deserializedElement =
            when (key) {
                is CooperationContext.UnmappedKey ->
                    CooperationContext.OpaqueElement(key, serializedElement)
                is CooperationContext.MappedKey<*> ->
                    objectMapper.readValue(serializedElement, key.mappedElementClass)
            }
        return (deserializedElement as E?)?.also { deserializedMap[key] = it }
    }

    override fun plus(context: CooperationContext): CooperationContext =
        when (context) {
            is CooperationContext.Element ->
                CooperationContextMap(
                    serializedMap,
                    buildMap {
                            putAll(deserializedMap)
                            put(context.key, context)
                        }
                        .toMutableMap(),
                    objectMapper,
                )
            is CooperationContextMap ->
                CooperationContextMap(
                    serializedMap.toMutableMap().apply { putAll(context.serializedMap) },
                    deserializedMap.toMutableMap().apply { putAll(context.deserializedMap) },
                    context.objectMapper ?: objectMapper,
                )
            else -> context.fold(this, CooperationContext::plus)
        }

    override fun minus(key: Key<*>): CooperationContext =
        if (get(key) == null) {
            this
        } else {
            CooperationContextMap(
                serializedMap.toMutableMap().also { it.remove(key.serializedValue) },
                deserializedMap.toMutableMap().also { it.remove(key) },
                objectMapper,
            )
        }

    override fun <R> fold(initial: R, operation: (R, CooperationContext.Element) -> R): R =
        buildList {
                serializedMap.forEach { (key, value) ->
                    add(
                        CooperationContext.OpaqueElement(CooperationContext.UnmappedKey(key), value)
                    )
                }
                addAll(deserializedMap.values)
            }
            .fold(initial, operation)
}

fun ObjectMapper.readCooperationContext(json: String): CooperationContext =
    CooperationContextMap(readMapOfJsonStrings(json), mutableMapOf(), this)

fun ObjectMapper.writeCooperationContext(context: CooperationContext): String =
    writeMapofJsonStrings(
        context.fold(mutableMapOf()) { acc, element ->
            acc[element.key.serializedValue] =
                if (element is CooperationContext.OpaqueElement) element.json
                else writeValueAsString(element)
            acc
        }
    )

fun ObjectMapper.writeMapofJsonStrings(rawMap: Map<String, String>): String {
    val writer = java.io.StringWriter()
    val generator = factory.createGenerator(writer)

    generator.writeStartObject()
    for ((key, rawJson) in rawMap) {
        generator.writeFieldName(key)
        generator.writeRawValue(rawJson)
    }
    generator.writeEndObject()
    generator.close()

    return writer.toString()
}

fun ObjectMapper.readMapOfJsonStrings(json: String): MutableMap<String, String> {
    val parser = factory.createParser(StringReader(json))
    val result = mutableMapOf<String, String>()

    if (parser.nextToken() != JsonToken.START_OBJECT) {
        throw IllegalArgumentException("Top-level JSON must be an object")
    }

    while (parser.nextToken() != JsonToken.END_OBJECT) {
        val fieldName = parser.currentName()
        parser.nextToken()

        val startOffset = parser.currentTokenLocation().charOffset.toInt()

        when (parser.currentToken()) {
            JsonToken.START_OBJECT,
            JsonToken.START_ARRAY -> {
                parser.skipChildren()
                val endOffset = parser.currentLocation().charOffset.toInt()
                result[fieldName] = json.substring(startOffset, endOffset).trim()
            }
            JsonToken.VALUE_STRING -> {
                result[fieldName] = "\"${parser.text}\""
            }
            JsonToken.VALUE_NUMBER_INT,
            JsonToken.VALUE_NUMBER_FLOAT -> {
                result[fieldName] = parser.numberValue.toString()
            }
            JsonToken.VALUE_TRUE,
            JsonToken.VALUE_FALSE -> {
                result[fieldName] = parser.booleanValue.toString()
            }
            JsonToken.VALUE_NULL -> {}
            else -> throw IllegalStateException("Unsupported token: ${parser.currentToken()}")
        }
    }

    parser.close()
    return result
}
