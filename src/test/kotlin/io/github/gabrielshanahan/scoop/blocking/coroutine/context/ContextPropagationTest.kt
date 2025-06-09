package io.github.gabrielshanahan.scoop.blocking.coroutine.context

import io.github.gabrielshanahan.scoop.blocking.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.blocking.messaging.eventLoopStrategy
import io.github.gabrielshanahan.scoop.blocking.transactional
import io.quarkus.test.junit.QuarkusTest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ContextPropagationTest : StructuredCooperationTest() {

    @Test
    fun `context is propagated correctly`() {
        val contextValues = mutableListOf<String>()

        val latch = CountDownLatch(1)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        { scope, message ->
                            contextValues.add(objectMapper.writeValueAsString(scope.context))
                            scope.context +=
                                ParentContext(scope.context[ParentContextKey]!!.value + 1)
                            contextValues.add(objectMapper.writeValueAsString(scope.context))
                            val childPayload =
                                jsonbHelper.toPGobject(mapOf("from" to "root-handler"))
                            scope.launch(childTopic, childPayload, ChildContext(0))
                        },
                        rollback = { scope, message, throwable ->
                            scope.context +=
                                ParentContext(scope.context[ParentContextKey]!!.value + 1)
                            contextValues.add(objectMapper.writeValueAsString(scope.context))
                            latch.countDown()
                        },
                    )

                    step { scope, message ->
                        scope.context += ParentContext(scope.context[ParentContextKey]!!.value + 1)
                        contextValues.add(objectMapper.writeValueAsString(scope.context))
                        throw RuntimeException("Failure")
                    }
                },
            )

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        { scope, message ->
                            scope.context +=
                                ChildContext(scope.context[ChildContextKey]!!.value + 1)
                            contextValues.add(objectMapper.writeValueAsString(scope.context))
                        },
                        rollback = { scope, message, throwable ->
                            scope.context +=
                                ChildContext(scope.context[ChildContextKey]!!.value + 1)
                            contextValues.add(objectMapper.writeValueAsString(scope.context))
                        },
                    )
                    step(
                        { scope, message ->
                            scope.context +=
                                ChildContext(scope.context[ChildContextKey]!!.value + 1)
                            contextValues.add(objectMapper.writeValueAsString(scope.context))
                        },
                        rollback = { scope, message, throwable ->
                            scope.context +=
                                ChildContext(scope.context[ChildContextKey]!!.value + 1)
                            contextValues.add(objectMapper.writeValueAsString(scope.context))
                        },
                    )
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload, ParentContext(0))
            }

            Assertions.assertTrue(
                latch.await(1, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )

            Thread.sleep(200)

            Assertions.assertEquals(
                listOf(
                    """{"ParentContextKey":{"value":0}}""",
                    """{"ParentContextKey":{"value":1}}""",
                    """{"ChildContextKey":{"value":1},"ParentContextKey":{"value":1}}""",
                    """{"ChildContextKey":{"value":2},"ParentContextKey":{"value":1}}""",
                    """{"ParentContextKey":{"value":2}}""",
                    """{"ChildContextKey":{"value":3},"ParentContextKey":{"value":2}}""",
                    """{"ChildContextKey":{"value":4},"ParentContextKey":{"value":2}}""",
                    """{"ParentContextKey":{"value":3}}""",
                ),
                contextValues,
                "Context values should match",
            )
        } finally {
            rootSubscription.close()
            childSubscription.close()
        }
    }
}

data object ParentContextKey :
    io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext.MappedKey<
        ParentContext
    >()

data class ParentContext(val value: Int) :
    io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext.MappedElement(
        ParentContextKey
    )

data object ChildContextKey :
    io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext.MappedKey<
        ChildContext
    >()

data class ChildContext(val value: Int) :
    io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext.MappedElement(
        ChildContextKey
    )
