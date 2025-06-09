package io.github.gabrielshanahan.scoop.shared.coroutine.context

import com.fasterxml.jackson.annotation.JsonIgnore
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

fun CooperationContext.has(key: CooperationContext.Key<*>) = get(key) != null

val CooperationContext.size
    get() = fold(0) { acc, _ -> acc + 1 }

fun CooperationContext.isNotEmpty() = size > 0

val CooperationContext.Key<*>.serializedValue: String
    get() =
        when (this) {
            is CooperationContext.UnmappedKey -> key
            is CooperationContext.MappedKey<*> -> this::class.simpleName!!
        }

fun emptyContext(): CooperationContext = CooperationContextMap(mutableMapOf(), mutableMapOf())
