package io.github.gabrielshanahan.scoop.blocking

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.enterprise.context.ApplicationScoped
import org.postgresql.util.PGobject

@ApplicationScoped
class JsonbHelper(private val objectMapper: ObjectMapper) {

    fun toPGobject(value: Any): PGobject =
        PGobject().apply {
            type = "jsonb"
            this.value = objectMapper.writeValueAsString(value)
        }

    fun <T> fromPGobject(pgObject: Any, clazz: Class<T>): T =
        objectMapper.readValue((pgObject as PGobject).value, clazz)

    fun <T> fromPGobject(pgObject: Any, typeReference: TypeReference<T>): T =
        objectMapper.readValue((pgObject as PGobject).value, typeReference)

    final inline fun <reified T> fromPGobject(pgObject: Any): T =
        fromPGobject(pgObject, T::class.java)

    final inline fun <reified T> fromPGobjectToList(pgObject: Any): List<T> =
        fromPGobject(pgObject, object : TypeReference<List<T>>() {})

    final inline fun <reified T> fromPGobjectToSet(pgObject: Any): Set<T> =
        fromPGobject(pgObject, object : TypeReference<Set<T>>() {})

    final inline fun <reified K, reified V> fromPGobjectToMap(pgObject: Any): Map<K, V> =
        fromPGobject(pgObject, object : TypeReference<Map<K, V>>() {})
}
