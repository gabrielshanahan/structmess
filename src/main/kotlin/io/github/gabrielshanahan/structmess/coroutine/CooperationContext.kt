package io.github.gabrielshanahan.structmess.coroutine

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.BeanDescription
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationConfig
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import io.github.gabrielshanahan.structmess.coroutine.CooperationContext.Key
import io.quarkus.jackson.ObjectMapperCustomizer
import jakarta.inject.Singleton
import java.lang.reflect.ParameterizedType

interface CooperationContext {

    operator fun <E : Element> get(key: Key<E>): E?

    operator fun plus(context: CooperationContext): CooperationContext

    operator fun minus(key: Key<*>): CooperationContext

    fun <R> fold(initial: R, operation: (R, Element) -> R): R

    // TODO: Doc that the SIMPLE name is used
    sealed interface Key<E : Element>

    data class UnmappedKey(val key: String) : Key<OpaqueElement>

    abstract class MappedKey<E : MappedElement> : Key<E> {
        init {
            checkNotNull(mappedElementClass) {
                "${javaClass.name} must be parameterized with the element class."
            }
        }

        val mappedElementClass: Class<*>?
            @JsonIgnore
            get() {
                var currentClass: Class<*> = javaClass
                while (currentClass != Any::class.java) {
                    (currentClass.genericSuperclass as? ParameterizedType)?.let { paramType ->
                        return paramType.actualTypeArguments.firstOrNull()?.let {
                            when (it) {
                                is Class<*> -> it
                                is ParameterizedType -> it.rawType as? Class<*>
                                else -> null
                            }
                        }
                    }
                    currentClass = currentClass.superclass
                }
                return null
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

        override fun plus(context: CooperationContext): CooperationContext =
            if (context.has(key)) {
                context
            } else {
                CooperationContextMap(mutableMapOf(), mutableMapOf(key to this)) + context
            }

        override fun minus(key: Key<*>): CooperationContext =
            if (this.key == key) {
                emptyContext()
            } else {
                this
            }

        override fun <R> fold(initial: R, operation: (R, Element) -> R): R =
            operation(initial, this)
    }

    data class OpaqueElement(override val key: UnmappedKey, internal val json: String) : Element

    abstract class MappedElement(override val key: MappedKey<*>) : Element
}

fun CooperationContext.has(key: Key<*>) = get(key) != null

val CooperationContext.size
    get() = fold(0) { acc, _ -> acc + 1 }

fun CooperationContext.isNotEmpty() = size > 0

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

    // TODO: Doc that this implementation will not run custom plus logic if the key hasn't been
    // touched in either of the maps!
    //  in practice, that shouldn't matter, because if we create the map ourselves, all the keys are
    // touched, and the only way
    //  we can get an untouched key is by deserializing the one in a message, so in practice we
    // could only add that map to itself,
    //  and that has no effect
    override fun plus(context: CooperationContext): CooperationContext =
        when (context) {
            is CooperationContext.Element ->
                if (has(context.key)) {
                    (this - context.key) + (get(context.key)!! + context)
                } else {
                    CooperationContextMap(
                        serializedMap,
                        buildMap {
                                putAll(deserializedMap)
                                put(context.key, context)
                            }
                            .toMutableMap(),
                        objectMapper,
                    )
                }
            is CooperationContextMap -> {
                val deserializedKeys = deserializedMap.keys + context.deserializedMap.keys

                // Make sure all keys deserialized in one map are also deserialized in the other
                // map,
                // so we can run instance-specific logic when calling
                // CooperationContext.Element.plus
                deserializedKeys.forEach { key ->
                    get(key)
                    context[key]
                }

                CooperationContextMap(
                    serializedMap.toMutableMap().apply { putAll(context.serializedMap) },
                    deserializedKeys.associateWithTo(mutableMapOf()) { key ->
                        when {
                            key in deserializedMap && key in context.deserializedMap ->
                                (deserializedMap.getValue(key) +
                                    context.deserializedMap.getValue(key))[key]!!
                            key in deserializedMap -> deserializedMap.getValue(key)
                            else -> context.deserializedMap.getValue(key)
                        }
                    },
                    context.objectMapper ?: objectMapper,
                )
            }
            else -> context.fold(this, CooperationContext::plus)
        }

    override fun minus(key: Key<*>): CooperationContext =
        if (!has(key)) {
            this
        } else {
            CooperationContextMap(
                serializedMap.toMutableMap().also { it.remove(key.serializedValue) },
                deserializedMap.toMutableMap().also { it.remove(key) },
                objectMapper,
            )
        }

    override fun <R> fold(initial: R, operation: (R, CooperationContext.Element) -> R): R =
        buildSet {
                serializedMap.forEach { (key, value) ->
                    add(
                        CooperationContext.OpaqueElement(CooperationContext.UnmappedKey(key), value)
                    )
                }
                addAll(deserializedMap.values)
            }
            .toSortedSet(compareBy { it.key.serializedValue })
            .fold(initial, operation)
}

@Singleton
class CooperationContextJacksonCustomizer : ObjectMapperCustomizer {
    override fun customize(objectMapper: ObjectMapper) {
        val module =
            SimpleModule("CooperationContextModule").apply {
                setSerializerModifier(
                    object : BeanSerializerModifier() {
                        override fun modifySerializer(
                            config: SerializationConfig,
                            beanDesc: BeanDescription,
                            serializer: JsonSerializer<*>,
                        ): JsonSerializer<*> {
                            if (
                                CooperationContext::class.java.isAssignableFrom(beanDesc.beanClass)
                            ) {
                                oldSerializers.put(beanDesc.beanClass, serializer)
                                return CooperationContextSerializer()
                            }
                            return serializer
                        }
                    }
                )

                addDeserializer(
                    CooperationContext::class.java,
                    CooperationContextDeserializer(objectMapper),
                )
            }

        objectMapper.registerModule(module)
    }

    companion object {
        val oldSerializers = mutableMapOf<Class<*>, JsonSerializer<*>>()
    }

    private class CooperationContextSerializer() :
        StdSerializer<CooperationContext>(CooperationContext::class.java) {
        override fun serialize(
            value: CooperationContext,
            gen: JsonGenerator,
            provider: SerializerProvider,
        ) {
            gen.writeStartObject()
            serializeCooperationContext(value, gen, provider)
            gen.writeEndObject()
        }

        private fun serializeCooperationContext(
            value: CooperationContext,
            gen: JsonGenerator,
            provider: SerializerProvider,
        ): Unit =
            when (value) {
                is CooperationContext.OpaqueElement -> {
                    gen.writeFieldName(value.key.serializedValue)
                    gen.writeRawValue(value.json)
                }

                is CooperationContext.Element -> {
                    gen.writeFieldName(value.key.serializedValue)
                    @Suppress("UNCHECKED_CAST")
                    val oldSerializer =
                        (oldSerializers[value::class.java]
                            ?: BeanSerializerFactory.instance.createSerializer(
                                provider,
                                provider.config.constructType(value.javaClass),
                            ))
                            as JsonSerializer<CooperationContext.Element>
                    oldSerializer.serialize(value, gen, provider)
                }

                else ->
                    value.fold(Unit) { acc, element ->
                        serializeCooperationContext(element, gen, provider)
                    }
            }
    }

    private class CooperationContextDeserializer(val objectMapper: ObjectMapper) :
        StdDeserializer<CooperationContext>(CooperationContext::class.java) {

        override fun deserialize(
            parser: JsonParser,
            ctxt: DeserializationContext,
        ): CooperationContext {
            val result = mutableMapOf<String, String>()

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                val fieldName = parser.currentName()
                parser.nextToken()
                result[fieldName] = readAsString(parser).toString()
            }

            return CooperationContextMap(result, mutableMapOf(), objectMapper)
        }

        private fun readAsString(
            parser: JsonParser,
            sb: StringBuilder = StringBuilder(),
        ): StringBuilder {
            when (parser.currentToken()) {
                JsonToken.START_OBJECT -> {
                    sb.append('{')
                    var alreadyWroteElem = false
                    while (parser.nextToken() != JsonToken.END_OBJECT) {
                        if (alreadyWroteElem) {
                            sb.append(",")
                        }
                        alreadyWroteElem = true
                        readAsString(parser, sb)
                        parser.nextToken()
                        readAsString(parser, sb)
                    }
                    sb.dropLast(1) // drop trailing comma
                    sb.append('}')
                }
                JsonToken.START_ARRAY -> {
                    sb.append('[')
                    var alreadyWroteElem = false
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        if (alreadyWroteElem) {
                            sb.append(",")
                        }
                        alreadyWroteElem = true
                        readAsString(parser, sb)
                    }
                    sb.append(']')
                }
                JsonToken.VALUE_STRING -> sb.append("\"${parser.text}\"")
                JsonToken.VALUE_NUMBER_INT,
                JsonToken.VALUE_NUMBER_FLOAT -> sb.append(parser.numberValue.toString())
                JsonToken.VALUE_TRUE,
                JsonToken.VALUE_FALSE -> sb.append(parser.booleanValue.toString())
                JsonToken.VALUE_NULL -> {}
                JsonToken.FIELD_NAME -> sb.append("\"${parser.text}\":")
                else -> throw IllegalStateException("Unsupported token: ${parser.currentToken()}")
            }
            return sb
        }
    }
}
