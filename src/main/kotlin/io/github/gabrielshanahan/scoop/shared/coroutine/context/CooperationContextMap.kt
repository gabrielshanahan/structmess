package io.github.gabrielshanahan.scoop.shared.coroutine.context

import com.fasterxml.jackson.databind.ObjectMapper

data class CooperationContextMap(
    private val serializedMap: MutableMap<String, String>,
    private val deserializedMap: MutableMap<CooperationContext.Key<*>, CooperationContext.Element>,
    private val objectMapper: ObjectMapper? = null,
) : CooperationContext {

    // TODO: Doc that only deserialize when needed, so small cost unless you actually use it
    @Suppress("UNCHECKED_CAST")
    override fun <E : CooperationContext.Element> get(key: CooperationContext.Key<E>): E? {
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

    override fun minus(key: CooperationContext.Key<*>): CooperationContext =
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
