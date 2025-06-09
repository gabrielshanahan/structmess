package io.github.gabrielshanahan.scoop.shared.coroutine.context

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
import io.quarkus.jackson.ObjectMapperCustomizer
import jakarta.inject.Singleton

// TODO: Say sorry
@Singleton
class CooperationContextJacksonCustomizer : ObjectMapperCustomizer {
    override fun customize(objectMapper: ObjectMapper) {
        val module =
            SimpleModule("CooperationContextModule").apply {
                setSerializerModifier(
                    object : com.fasterxml.jackson.databind.ser.BeanSerializerModifier() {
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

    private class CooperationContextSerializer :
        com.fasterxml.jackson.databind.ser.std.StdSerializer<CooperationContext>(
            CooperationContext::class.java
        ) {
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
        com.fasterxml.jackson.databind.deser.std.StdDeserializer<CooperationContext>(
            CooperationContext::class.java
        ) {

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
                else -> error("Unsupported token: ${parser.currentToken()}")
            }
            return sb
        }
    }
}
