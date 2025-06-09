package io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.blocking.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX
import io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.ROLLING_BACK_PREFIX
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.blocking.transactional
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.continuation.ContinuationIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.StandardEventLoopStrategy
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import io.quarkus.test.junit.QuarkusTest
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.collections.plus
import kotlin.concurrent.thread
import org.codejargon.fluentjdbc.api.mapper.Mappers
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.postgresql.util.PGobject

typealias Row = Map<String, Any?>

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PendingCoroutineRunSqlTest : StructuredCooperationTest() {
    private val rootHandler = "root-handler"

    private val childTopic1 = "child-topic-1"
    private val childTopic2 = "child-topic-1"
    private val childHandler1 = "child-handler-1"
    private val childHandler2 = "child-handler-2"
    private val childHandler3 = "child-handler-3"
    private val childHandler4 = "child-handler-4"

    fun DistributedCoroutineIdentifier.step(stepName: String) =
        ContinuationIdentifier(stepName, this)

    fun <T> List<T>.isEqualToInAnyOrder(other: List<T>, message: String) =
        Assertions.assertTrue(containsAll(other) && other.containsAll(this), message)

    fun Row.getString(column: String) = get(column) as String

    fun Row.getUUID(column: String) = get(column) as UUID

    fun Row.getLocalDateTime(column: String) = (get(column) as Timestamp).toLocalDateTime()

    fun Row.getArrayOfUUIDs(column: String) = (get(column) as java.sql.Array).array as Array<UUID>

    @Nested
    inner class CandidateSeenTest {

        fun List<Row>.assertNoSeen() =
            Assertions.assertTrue(isEmpty(), "no SEEN should be picked up, but got $this")

        fun List<Row>.assertSeen(vararg seenIds: UUID) =
            map { it.getUUID("id") }
                .also {
                    it.isEqualToInAnyOrder(
                        seenIds.asList(),
                        "SEEN id should be ${seenIds.asList()}, but was $it",
                    )
                }

        fun List<Row>.assertRollbackEmittedPresent() =
            Assertions.assertTrue(
                single().getLocalDateTime("rollback_emitted_at") != null,
                "rollback_emitted should be present",
            )

        @Test
        fun `picks up SEEN`() {
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                val seenId = seen()

                verify(candidateSeens) { result -> result.assertSeen(seenId) }
            }
        }

        @Test
        fun `picks up SUSPENDED`() {
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                val seenId = seen()
                suspended(0)

                verify(candidateSeens) { result -> result.assertSeen(seenId) }
            }
        }

        @Test
        fun `picks up ROLLING_BACK`() {
            val throwable = RuntimeException("A thing happened")
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                val seenId = seen()
                rollingBack(throwable, 0)

                verify(candidateSeens) { result -> result.assertSeen(seenId) }
            }
        }

        @Test
        fun `picks up combinations`() {
            lateinit var seenId1: UUID
            lateinit var seenId2: UUID
            lateinit var seenId3: UUID
            val throwable = RuntimeException("A thing happened")

            emitRootMessage(rootTopic).coroutineProgress(rootHandler) { seenId1 = seen() }

            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seenId2 = seen()
                suspended(0)
            }

            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seenId3 = seen()
                rollingBack(throwable, 0)
                verify(candidateSeens) { result -> result.assertSeen(seenId1, seenId2, seenId3) }
            }
        }

        @Test
        fun `picks up COMMITTED with parent ROLLBACK_EMITTED`() {
            lateinit var childSeenId: UUID
            val throwable = RuntimeException("A thing happened")

            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                val seenId = seen()
                val childMessage = emitted(childTopic1, 0)
                suspended(0)
                childMessage.childCoroutineProgress(childHandler1) {
                    childSeenId = seen()
                    suspended(0)
                    committed(0)
                }
                rollingBack(throwable, 1)
                rollbackEmitted(childMessage, childScopeRollback(0), throwable)
                suspended(childScopeRollback(0))

                childMessage.childCoroutineProgress(childHandler1) {
                    rollingBack(throwable)
                    verify(candidateSeens) { result ->
                        result.assertSeen(childSeenId)
                        result.assertRollbackEmittedPresent()
                    }
                }

                verify(candidateSeens) { result -> result.assertSeen(seenId) }
            }
        }

        @Test
        fun `does not pick up COMMITED without parent ROLLING_BACK`() {
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                suspended(0)
                committed(0)

                verify(candidateSeens) { result -> result.assertNoSeen() }
            }
        }

        @Test
        fun `does not pick up rolled back`() {
            val throwable = RuntimeException("A thing happened")
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                suspended(0)
                rollingBack(throwable, 0)
                suspended(childScopeRollback(0))
                suspended(rollback(0))
                rolledBack(rollback(0))

                verify(candidateSeens) { result -> result.assertNoSeen() }
            }
        }

        @Test
        fun `does not pick up rollback failed`() {
            val throwable = RuntimeException("A thing happened")
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                suspended(0)
                rollingBack(throwable, 0)
                suspended(childScopeRollback(0))
                rollbackFailed(childScopeRollback(0), throwable)

                verify(candidateSeens) { result -> result.assertNoSeen() }
            }
        }
    }

    @Nested
    inner class LatestSuspendedTest {

        fun List<Row>.assertSuspendedAbsent() =
            Assertions.assertTrue(isEmpty(), "SUSPENDED row shouldn't be present")

        fun List<Row>.assertSuspendedStepIs(step: String) =
            single().getString("step").also {
                Assertions.assertTrue(it == step, "SUSPENDED step should be $step, but is $it")
            }

        fun List<Row>.assertSuspendedStepIs(step: Int) = assertSuspendedStepIs(step.toString())

        @Test
        fun `picks up latest suspended`() {
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                suspended(0)

                verify(latestSuspended) { result -> result.assertSuspendedStepIs(0) }
            }
        }

        @Test
        fun `picks up latest suspended after rollback`() {
            val throwable = RuntimeException("A thing happened")
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                suspended(0)
                rollingBack(throwable, 0)

                verify(latestSuspended) { result -> result.assertSuspendedStepIs(0) }
            }
        }

        @Test
        fun `works when no suspended is present`() {
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                verify(latestSuspended) { result -> result.assertSuspendedAbsent() }
            }
        }
    }

    @Nested
    inner class ChildEmissionsInLatestStep {

        fun List<Row>.assertNothingWasEmitted() =
            Assertions.assertTrue(isEmpty(), "no EMITTED rows should be present")

        fun List<Row>.assertWasEmitted(vararg messageIds: UUID) =
            map { it.getUUID("message_id") }
                .also {
                    it.isEqualToInAnyOrder(
                        messageIds.asList(),
                        "expected ${messageIds.asList()} to be emitted, but was $it",
                    )
                }

        @Test
        fun `picks up child emissions in last step`() {
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                val childMessage1 = emitted(childTopic1, 0)
                val childMessage2 = emitted(childTopic1, 0)
                suspended(0)

                verify(childEmissionsInLatestStep) { result ->
                    result.assertWasEmitted(childMessage1.id, childMessage2.id)
                }
            }
        }

        @Test
        fun `does nothing when no emissions`() {
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                suspended(0)

                verify(childEmissionsInLatestStep) { result -> result.assertNothingWasEmitted() }
            }
        }
    }

    @Nested
    inner class ChildSeens {

        fun List<Row>.assertSeen(vararg seenIds: UUID) =
            map { it.getUUID("id") }
                .also {
                    it.isEqualToInAnyOrder(
                        seenIds.asList(),
                        "expected ${seenIds.asList()} to be seen, but was $it",
                    )
                }

        fun List<Row>.assertParentLineagesIs(lineage: List<UUID>) =
            map { it.getArrayOfUUIDs("parent_cooperation_lineage") }
                .forEach { parentCooperationLineage ->
                    Assertions.assertTrue(
                        parentCooperationLineage.asList() == lineage,
                        "expected $lineage as parent lineages, but was ${parentCooperationLineage.asList()}",
                    )
                }

        @Test
        fun `picks up child seens and their terminations`() {
            lateinit var childSeenId1: UUID
            lateinit var childSeenId2: UUID
            lateinit var childSeenId3: UUID
            lateinit var childSeenId4: UUID
            val throwable = RuntimeException("A thing happened")
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                val childMessage1 = emitted(childTopic1, 0)
                val childMessage2 = emitted(childTopic2, 0)
                suspended(0)

                childMessage1.childCoroutineProgress(childHandler1) {
                    childSeenId1 = seen()
                    suspended(0)
                    committed(0)
                }

                childMessage1.childCoroutineProgress(childHandler2) {
                    childSeenId2 = seen()
                    suspended(0)
                }

                childMessage2.childCoroutineProgress(childHandler3) {
                    childSeenId3 = seen()
                    rollingBack(throwable, 0)
                    rolledBack(0)
                }

                childMessage2.childCoroutineProgress(childHandler4) {
                    childSeenId4 = seen()
                    rollingBack(throwable, 0)
                    rollbackFailed(0, throwable)
                }

                verify(childSeens) { result ->
                    result.assertSeen(childSeenId1, childSeenId2, childSeenId3, childSeenId4)
                    result.assertParentLineagesIs(scope)
                }

                verify(terminatedChildSeens) { result ->
                    result.assertSeen(childSeenId1, childSeenId3, childSeenId4)
                }
            }
        }
    }

    @Nested
    inner class ChildRollbackEmissionsInLatestStep {
        fun List<Row>.assertNothingWasEmitted() =
            Assertions.assertTrue(isEmpty(), "no ROLLBACK_EMMITED rows should be present")

        fun List<Row>.assertWasEmitted(vararg messageIds: UUID) =
            map { it.getUUID("message_id") }
                .also {
                    it.isEqualToInAnyOrder(
                        messageIds.asList(),
                        "expected ${messageIds.asList()} to be emitted, but was $it",
                    )
                }

        @Test
        fun `picks up rollback emissions in last step`() {
            val throwable = RuntimeException("A thing happened")
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                val childMessage = emitted(childTopic1, 0)
                suspended(0)
                childMessage.childCoroutineProgress(childHandler1) {
                    seen()
                    suspended(0)
                    committed(0)
                }
                rollingBack(throwable, 1)
                rollbackEmitted(childMessage, childScopeRollback(0), throwable)
                suspended(childScopeRollback(0))

                verify(childRollbackEmissionsInLatestStep) { result ->
                    result.assertWasEmitted(childMessage.id)
                }
            }
        }

        @Test
        fun `does nothing when no emissions`() {
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                suspended(0)

                verify(childRollbackEmissionsInLatestStep) { result ->
                    result.assertNothingWasEmitted()
                }
            }
        }
    }

    @Nested
    inner class ChildRollingBacks {

        fun List<Row>.assertRollingBacksAre(vararg rollingBackIds: UUID) =
            map { it.getUUID("id") }
                .also {
                    it.isEqualToInAnyOrder(
                        rollingBackIds.asList(),
                        "expected ${rollingBackIds.asList()} to be rolling back, but was $it",
                    )
                }

        fun List<Row>.assertParentLineagesIs(lineage: List<UUID>) =
            map { it.getArrayOfUUIDs("parent_cooperation_lineage") }
                .forEach { parentCooperationLineage ->
                    Assertions.assertTrue(
                        parentCooperationLineage.asList() == lineage,
                        "expected $lineage as parent lineages, but was ${parentCooperationLineage.asList()}",
                    )
                }

        @Test
        fun `picks up child rolling backs and their terminations`() {
            lateinit var childRollingBackId1: UUID
            lateinit var childRollingBackId2: UUID
            lateinit var childRollingBackId3: UUID
            lateinit var childRollingBackId4: UUID
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                val throwable = RuntimeException("A thing happened")
                seen()
                val childMessage1 = emitted(childTopic1, 0)
                val childMessage2 = emitted(childTopic2, 0)
                suspended(0)

                childMessage1.childCoroutineProgress(childHandler1) {
                    seen()
                    suspended(0)
                    committed(0)
                }

                childMessage1.childCoroutineProgress(childHandler2) {
                    seen()
                    suspended(0)
                    committed(0)
                }

                childMessage2.childCoroutineProgress(childHandler3) {
                    seen()
                    childRollingBackId3 = rollingBack(throwable, 0)
                    rolledBack(0)
                }

                childMessage2.childCoroutineProgress(childHandler4) {
                    seen()
                    committed(0)
                }
                rollingBack(throwable, 1)
                rollbackEmitted(childMessage1, childScopeRollback(0), throwable)
                rollbackEmitted(childMessage2, childScopeRollback(0), throwable)
                suspended(childScopeRollback(0))

                childMessage1.childCoroutineProgress(childHandler1) {
                    childRollingBackId1 = rollingBack(throwable)
                    suspended(childScopeRollback(0))
                    suspended(rollback(0))
                    rolledBack(rollback(0))
                }

                childMessage1.childCoroutineProgress(childHandler2) {
                    childRollingBackId2 = rollingBack(throwable)
                    suspended(childScopeRollback(0))
                }

                childMessage2.childCoroutineProgress(childHandler4) {
                    childRollingBackId4 = rollingBack(throwable)
                    rollbackFailed(childScopeRollback(0), throwable)
                }

                verify(childRollingBacks) { result ->
                    result.assertRollingBacksAre(
                        childRollingBackId1,
                        childRollingBackId2,
                        childRollingBackId3,
                        childRollingBackId4,
                    )
                    result.assertParentLineagesIs(scope)
                }

                verify(terminatedChildRollingBacks) { result ->
                    result.assertRollingBacksAre(
                        childRollingBackId1,
                        childRollingBackId3,
                        childRollingBackId4,
                    )
                }
            }
        }
    }

    @Nested
    inner class CandidateSeensWaitingToBeProcessed {

        fun List<Row>.assertNoSeen() =
            Assertions.assertTrue(isEmpty(), "no SEEN should be picked up, but got $this")

        fun List<Row>.assertSeen(vararg seenIds: UUID) =
            map { it.getUUID("id") }
                .also {
                    it.isEqualToInAnyOrder(
                        seenIds.asList(),
                        "SEEN id should be ${seenIds.asList()}, but was $it",
                    )
                }

        @Nested
        inner class NoStrategy {

            val noStrategy =
                object : EventLoopStrategy {
                    override fun start(emitted: String) = "TRUE"

                    override fun resumeHappyPath(
                        candidateSeen: String,
                        emittedInLatestStep: String,
                        childSeens: String,
                    ) = "TRUE"

                    override fun giveUpOnHappyPath(seen: String): String = "SELECT NULL WHERE FALSE"

                    override fun resumeRollbackPath(
                        candidateSeen: String,
                        rollbacksEmittedInLatestStep: String,
                        childRollingBacks: String,
                    ) = "TRUE"

                    override fun giveUpOnRollbackPath(seen: String): String =
                        "SELECT NULL WHERE FALSE"
                }

            val candidateSeensWaitingToBeProcessed = candidateSeensWaitingToBeProcessed(noStrategy)

            @Test
            fun `picks up SEEN`() {
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    val seenId = seen()

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        result.assertSeen(seenId)
                    }
                }
            }

            @Test
            fun `picks up SUSPENDED`() {
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    val seenId = seen()
                    suspended(0)

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        result.assertSeen(seenId)
                    }
                }
            }

            @Test
            fun `picks up ROLLING_BACK`() {
                val throwable = RuntimeException("A thing happened")
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    val seenId = seen()
                    rollingBack(throwable, 0)

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        result.assertSeen(seenId)
                    }
                }
            }

            @Test
            fun `picks up combinations`() {
                lateinit var seenId1: UUID
                lateinit var seenId2: UUID
                lateinit var seenId3: UUID
                val throwable = RuntimeException("A thing happened")

                emitRootMessage(rootTopic).coroutineProgress(rootHandler) { seenId1 = seen() }

                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seenId2 = seen()
                    suspended(0)
                }

                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seenId3 = seen()
                    rollingBack(throwable, 0)
                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        result.assertSeen(seenId1, seenId2, seenId3)
                    }
                }
            }

            @Test
            fun `picks up when children committed`() {
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    val seenId = seen()
                    val childMessage = emitted(childTopic1, 0)
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) {
                        seen()
                        suspended(0)
                        committed(0)
                        verify(candidateSeensWaitingToBeProcessed) { result ->
                            result.assertNoSeen()
                        }
                    }

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        result.assertSeen(seenId)
                    }
                }
            }

            @Test
            fun `picks up when children rolled back`() {
                val throwable = RuntimeException("A thing happened")
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    val seenId = seen()
                    val childMessage = emitted(childTopic1, 0)
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) {
                        seen()
                        rollingBack(throwable, 0)
                        rolledBack(rollback(0))
                        verify(candidateSeensWaitingToBeProcessed) { result ->
                            result.assertNoSeen()
                        }
                    }

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        result.assertSeen(seenId)
                    }
                }
            }

            @Test
            fun `picks up COMMITTED with parent ROLLBACK_EMITTED and unfinished child ROLLING_BACK`() {
                lateinit var childSeenId: UUID
                val throwable = RuntimeException("A thing happened")

                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seen()
                    val childMessage = emitted(childTopic1, 0)
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) {
                        childSeenId = seen()
                        suspended(0)
                        committed(0)
                    }
                    rollingBack(throwable, 1)
                    rollbackEmitted(childMessage, childScopeRollback(0), throwable)
                    suspended(childScopeRollback(0))

                    childMessage.childCoroutineProgress(childHandler1) {
                        // This rollingBack is important, since noStrategy says "no handler is
                        // missing."
                        // If this wasn't included, the toplevel seen would get picked up as well,
                        // even though
                        // we know its child hasn't finished rolling back yet
                        rollingBack(throwable)
                        verify(candidateSeensWaitingToBeProcessed) { result ->
                            result.assertSeen(childSeenId)
                        }
                    }

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        // Child has a ROLLBACK_EMITTED, so it hasn't finished yet
                        result.assertNoSeen()
                    }
                }
            }

            @Test
            fun `doesn't pick up when there are unfinished children emissions`() {
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seen()
                    val childMessage = emitted(childTopic1, 0)
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) {
                        val childSeenId = seen()
                        suspended(0)
                        verify(candidateSeensWaitingToBeProcessed) { result ->
                            result.assertSeen(childSeenId)
                        }
                    }

                    verify(candidateSeensWaitingToBeProcessed) { result -> result.assertNoSeen() }
                }
            }

            @Test
            fun `doesn't pick up when there are unfinished children emissions - 2 deep`() {
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seen()
                    val childMessage = emitted(childTopic1, 0)
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) {
                        seen()
                        val grandchildMessage = emitted(childTopic2, 0)
                        suspended(0)
                        grandchildMessage.childCoroutineProgress(childHandler2) {
                            val grandChildSeenId = seen()
                            suspended(0)
                            verify(candidateSeensWaitingToBeProcessed) { result ->
                                result.assertSeen(grandChildSeenId)
                            }
                        }
                        verify(candidateSeensWaitingToBeProcessed) { result ->
                            result.assertNoSeen()
                        }
                    }

                    verify(candidateSeensWaitingToBeProcessed) { result -> result.assertNoSeen() }
                }
            }
        }

        @Nested
        inner class RegistryStrategy {

            val registryStrategy =
                StandardEventLoopStrategy(OffsetDateTime.now()) {
                    mapOf(
                        rootTopic to listOf(rootHandler),
                        childTopic1 to listOf(childHandler1, childHandler2),
                    )
                }

            val candidateSeensWaitingToBeProcessed =
                candidateSeensWaitingToBeProcessed(registryStrategy)

            @Test
            fun `picks up SEEN`() {
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    val seenId = seen()

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        result.assertSeen(seenId)
                    }
                }
            }

            @Test
            fun `picks up SUSPENDED`() {
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    val seenId = seen()
                    suspended(0)

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        result.assertSeen(seenId)
                    }
                }
            }

            @Test
            fun `picks up ROLLING_BACK`() {
                val throwable = RuntimeException("A thing happened")
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    val seenId = seen()
                    rollingBack(throwable, 0)

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        result.assertSeen(seenId)
                    }
                }
            }

            @Test
            fun `picks up combinations`() {
                lateinit var seenId1: UUID
                lateinit var seenId2: UUID
                lateinit var seenId3: UUID
                val throwable = RuntimeException("A thing happened")

                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seenId1 = seen()
                    val childMessage = emitted(childTopic1, 0)
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) {
                        seen()
                        suspended(0)
                        committed(0)
                        verify(candidateSeensWaitingToBeProcessed) { result ->
                            result.assertNoSeen()
                        }
                    }

                    childMessage.childCoroutineProgress(childHandler2) {
                        seen()
                        rollingBack(throwable, 0)
                        rolledBack(rollback(0))
                        verify(candidateSeensWaitingToBeProcessed) { result ->
                            result.assertNoSeen()
                        }
                    }
                }

                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seenId2 = seen()
                    suspended(0)
                }

                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seenId3 = seen()
                    rollingBack(throwable, 0)
                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        result.assertSeen(seenId1, seenId2, seenId3)
                    }
                }
            }

            @Test
            fun `picks up when all children finished`() {
                val throwable = RuntimeException("A thing happened")
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    val seenId = seen()
                    val childMessage = emitted(childTopic1, 0)
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) {
                        seen()
                        suspended(0)
                        committed(0)
                        verify(candidateSeensWaitingToBeProcessed) { result ->
                            result.assertNoSeen()
                        }
                    }

                    childMessage.childCoroutineProgress(childHandler2) {
                        seen()
                        rollingBack(throwable, 0)
                        rolledBack(rollback(0))
                        verify(candidateSeensWaitingToBeProcessed) { result ->
                            result.assertNoSeen()
                        }
                    }

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        result.assertSeen(seenId)
                    }
                }
            }

            @Test
            fun `picks up COMMITTED with parent ROLLBACK_EMITTED and unfinished child ROLLING_BACK`() {
                lateinit var childSeenId: UUID
                val throwable = RuntimeException("A thing happened")

                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seen()
                    val childMessage = emitted(childTopic1, 0)
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) {
                        childSeenId = seen()
                        suspended(0)
                        committed(0)
                    }
                    rollingBack(throwable, 1)
                    rollbackEmitted(childMessage, childScopeRollback(0), throwable)
                    suspended(childScopeRollback(0))

                    childMessage.childCoroutineProgress(childHandler1) {
                        // This rollingBack is important, since noStrategy says "no handler is
                        // missing."
                        // If this wasn't included, the toplevel seen would get picked up as well,
                        // even though
                        // we know its child hasn't finished rolling back yet
                        rollingBack(throwable, childScopeRollback(0))
                        verify(candidateSeensWaitingToBeProcessed) { result ->
                            result.assertSeen(childSeenId)
                        }
                    }

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        // Child has a ROLLBACK_EMITTED, so it hasn't finished yet
                        result.assertNoSeen()
                    }
                }
            }

            @Test
            fun `doesn't pick up when no child SEENs are present`() {
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seen()
                    emitted(childTopic1, 0)
                    suspended(0)

                    verify(candidateSeensWaitingToBeProcessed) { result -> result.assertNoSeen() }
                }
            }

            @Test
            fun `doesn't pick up when a child SEEN is missing, even if the rest are finished`() {
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seen()
                    val childMessage = emitted(childTopic1, 0)
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) {
                        seen()
                        suspended(0)
                        committed(0)
                        verify(candidateSeensWaitingToBeProcessed) { result ->
                            result.assertNoSeen()
                        }
                    }

                    verify(candidateSeensWaitingToBeProcessed) { result -> result.assertNoSeen() }
                }
            }

            @Test
            fun `doesn't pick up when no child ROLLING_BACKs are present`() {
                val throwable = RuntimeException("A thing happened")
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seen()
                    val childMessage = emitted(childTopic1, 0)
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) {
                        seen()
                        suspended(0)
                        committed(0)
                    }
                    childMessage.childCoroutineProgress(childHandler2) {
                        seen()
                        suspended(0)
                        committed(0)
                    }
                    rollingBack(throwable, 1)
                    rollbackEmitted(childMessage, childScopeRollback(0), throwable)
                    suspended(childScopeRollback(0))

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        // Child has a ROLLBACK_EMITTED, so it hasn't finished yet
                        result.assertNoSeen()
                    }
                }
            }

            @Test
            fun `doesn't pick up when a child ROLLING_BACK is missing, even if the rest are finished`() {
                val throwable = RuntimeException("A thing happened")
                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seen()
                    val childMessage = emitted(childTopic1, 0)
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) {
                        seen()
                        suspended(0)
                        committed(0)
                    }
                    childMessage.childCoroutineProgress(childHandler2) {
                        seen()
                        suspended(0)
                        committed(0)
                    }
                    rollingBack(throwable, 1)
                    rollbackEmitted(childMessage, childScopeRollback(0), throwable)
                    suspended(childScopeRollback(0))

                    childMessage.childCoroutineProgress(childHandler1) {
                        rollingBack(throwable)
                        suspended(childScopeRollback(0))
                        suspended(rollback(0))
                        rolledBack(rollback(0))
                    }

                    verify(candidateSeensWaitingToBeProcessed) { result ->
                        // Child has a ROLLBACK_EMITTED, so it hasn't finished yet
                        result.assertNoSeen()
                    }
                }
            }
        }
    }

    @Nested
    inner class SeenForProcessing {

        val registryStrategy =
            StandardEventLoopStrategy(OffsetDateTime.now()) {
                mapOf(
                    rootTopic to listOf(rootHandler),
                    childTopic1 to listOf(childHandler1, childHandler2),
                )
            }

        val seenForProcessing = seenForProcessing(registryStrategy)

        fun List<Row>.assertSeenIsFor(messageId: UUID) =
            single().getUUID("message_id").also {
                Assertions.assertTrue(
                    it == messageId,
                    "SEEN message_id should be $messageId, but was $it",
                )
            }

        fun List<Row>.assertNoSeen() =
            Assertions.assertTrue(isEmpty(), "no SEEN should be picked up, but got $this")

        @Test
        fun `when present, the time of rollback emission time determines precedence`() {
            lateinit var messageId: UUID
            val throwable = RuntimeException("A thing happened")
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                val childMessage = emitted(childTopic1, 0)
                suspended(0)
                childMessage.childCoroutineProgress(childHandler1) {
                    seen()
                    suspended(0)
                    committed(0)
                }
                childMessage.childCoroutineProgress(childHandler2) {
                    seen()
                    suspended(0)
                    committed(0)
                }

                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seen()
                    val childMessage = emitted(childTopic1, 0)
                    messageId = childMessage.id
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) { seen() }
                }

                rollingBack(throwable, 1)
                rollbackEmitted(childMessage, childScopeRollback(0), throwable)
                suspended(childScopeRollback(0))

                childMessage.childCoroutineProgress(childHandler1) {
                    rollingBack(throwable)
                    verify(seenForProcessing) { result -> result.assertSeenIsFor(messageId) }
                }
            }
        }

        @Test
        fun `otherwise, first emitted first processed`() {
            lateinit var messageId: UUID
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                val childMessage = emitted(childTopic1, 0)
                messageId = childMessage.id
                suspended(0)
                childMessage.childCoroutineProgress(childHandler1) {
                    seen()
                    suspended(0)
                }
                childMessage.childCoroutineProgress(childHandler2) {
                    seen()
                    suspended(0)
                    committed(0)
                }

                emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                    seen()
                    val childMessage = emitted(childTopic1, 0)
                    suspended(0)
                    childMessage.childCoroutineProgress(childHandler1) {
                        seen()
                        verify(seenForProcessing) { result -> result.assertSeenIsFor(messageId) }
                    }
                }
            }
        }

        @Test
        fun `only single transaction can pick up a SEEN`() {
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                verify(seenForProcessing) { result -> result.assertSeenIsFor(message.id) }

                val result =
                    fluentJdbc.transactional { connection ->
                        fluentJdbc
                            .queryOn(connection)
                            .select(seenForProcessing.build())
                            .namedParam("coroutine_name", distributedCoroutineIdentifier.name)
                            .listResult(Mappers.map())
                            .also {
                                it.assertSeenIsFor(message.id)
                                thread {
                                        verify(seenForProcessing) { result ->
                                            result.assertNoSeen()
                                        }
                                    }
                                    .join()
                            }
                    }

                result.assertSeenIsFor(message.id)
                verify(seenForProcessing) { result -> result.assertSeenIsFor(message.id) }
            }
        }
    }

    data class FinalSelectResult(
        val childRolledBackExceptions: List<CooperationFailure>,
        val childRollbackFailedExceptions: List<CooperationFailure>,
        val rollingBackException: CooperationFailure?,
    ) {
        val anyChildRolledBack: Boolean
            get() = !childRolledBackExceptions.isEmpty()

        val anyChildRollbackFailed: Boolean
            get() = !childRollbackFailedExceptions.isEmpty()

        val rollingBack: Boolean
            get() = rollingBackException != null
    }

    @Nested
    inner class FinalSelect {

        fun List<Row>.asResult() =
            single().let {
                FinalSelectResult(
                    childRolledBackExceptions =
                        jsonbHelper.fromPGobjectToList(
                            it["child_rolled_back_exceptions"] as PGobject
                        ),
                    childRollbackFailedExceptions =
                        jsonbHelper.fromPGobjectToList(
                            it["child_rollback_failed_exceptions"] as PGobject
                        ),
                    rollingBackException =
                        (it["rolling_back_exception"] as PGobject?)?.let(jsonbHelper::fromPGobject),
                )
            }

        val registryStrategy =
            StandardEventLoopStrategy(OffsetDateTime.now()) {
                mapOf(
                    rootTopic to listOf(rootHandler),
                    childTopic1 to listOf(childHandler1, childHandler2),
                )
            }

        val finalSelect = finalSelect(registryStrategy)

        @Test
        fun `happy path`() {
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                val childMessage = emitted(childTopic1, 0)
                suspended(0)

                childMessage.childCoroutineProgress(childHandler1) {
                    seen()
                    suspended(0)
                    committed(0)
                }

                childMessage.childCoroutineProgress(childHandler2) {
                    seen()
                    suspended(0)
                    committed(0)
                }

                verify(finalSelect) { result ->
                    result.asResult().apply {
                        Assertions.assertFalse(anyChildRolledBack)
                        Assertions.assertFalse(anyChildRollbackFailed)
                        Assertions.assertFalse(rollingBack)
                    }
                }
            }
        }

        @Test
        fun `picks up children rolling back`() {
            val throwable = RuntimeException("A thing happened")
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                val childMessage = emitted(childTopic1, 0)
                suspended(0)

                childMessage.childCoroutineProgress(childHandler1) {
                    seen()
                    suspended(0)
                    committed(0)
                }

                childMessage.childCoroutineProgress(childHandler2) {
                    seen()
                    rollingBack(throwable, 0)
                    rolledBack(rollback(0))
                }

                verify(finalSelect) { result ->
                    result.asResult().apply {
                        Assertions.assertTrue(anyChildRolledBack)
                        Assertions.assertFalse(anyChildRollbackFailed)
                        Assertions.assertFalse(rollingBack)
                    }
                }
            }
        }

        @Test
        fun `picks up children rollback failures`() {
            val originalThrowable = RuntimeException("A thing happened")
            val rollbackThrowable = RuntimeException("Another thing happened")
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                val childMessage = emitted(childTopic1, 0)
                suspended(0)

                childMessage.childCoroutineProgress(childHandler1) {
                    seen()
                    suspended(0)
                    committed(0)
                }

                childMessage.childCoroutineProgress(childHandler2) {
                    seen()
                    rollingBack(originalThrowable, 0)
                    rollbackFailed(rollback(0), rollbackThrowable)
                }

                verify(finalSelect) { result ->
                    result.asResult().apply {
                        Assertions.assertTrue(anyChildRolledBack)
                        Assertions.assertTrue(anyChildRollbackFailed)
                        Assertions.assertFalse(rollingBack)
                    }
                }
            }
        }

        @Test
        fun `picks up rolling backs just starting`() {
            val throwable = RuntimeException("A thing happened")
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                val childMessage = emitted(childTopic1, 0)
                suspended(0)
                childMessage.childCoroutineProgress(childHandler1) {
                    seen()
                    suspended(0)
                    committed(0)
                }
                childMessage.childCoroutineProgress(childHandler2) {
                    seen()
                    suspended(0)
                    committed(0)
                }

                rollingBack(throwable, 1)

                verify(finalSelect) { result ->
                    result.asResult().apply {
                        Assertions.assertFalse(anyChildRolledBack)
                        Assertions.assertFalse(anyChildRollbackFailed)
                        Assertions.assertTrue(rollingBack)
                    }
                }
            }
        }

        @Test
        fun `picks up rolling backs later on`() {
            val throwable = RuntimeException("A thing happened")
            emitRootMessage(rootTopic).coroutineProgress(rootHandler) {
                seen()
                val childMessage = emitted(childTopic1, 0)
                suspended(0)
                childMessage.childCoroutineProgress(childHandler1) {
                    seen()
                    suspended(0)
                    committed(0)
                }
                childMessage.childCoroutineProgress(childHandler2) {
                    seen()
                    suspended(0)
                    committed(0)
                }

                rollingBack(throwable, 1)
                rollbackEmitted(childMessage, childScopeRollback(0), throwable)
                suspended(childScopeRollback(0))

                childMessage.childCoroutineProgress(childHandler1) {
                    rollingBack(throwable)
                    suspended(childScopeRollback(0))
                    suspended(rollback(0))
                    rolledBack(rollback(0))
                }

                childMessage.childCoroutineProgress(childHandler2) {
                    rollingBack(throwable)
                    suspended(childScopeRollback(0))
                    suspended(rollback(0))
                    rolledBack(rollback(0))
                }

                verify(finalSelect) { result ->
                    result.asResult().apply {
                        Assertions.assertTrue(anyChildRolledBack)
                        Assertions.assertFalse(anyChildRollbackFailed)
                        Assertions.assertTrue(rollingBack)
                    }
                }
            }
        }
    }

    fun emitRootMessage(topic: String, key: String = "key", value: String = "value"): Message {
        val sqlTestUtils = SqlTestUtils(fluentJdbc, jsonbHelper)
        val message = sqlTestUtils.createSimpleMessage(topic, key, value)
        sqlTestUtils.emitted(message.id)
        return message
    }

    inner class CoroutineProgressBuilder(
        val message: Message,
        val distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
        parentScope: List<UUID> = emptyList(),
    ) {

        // Generate the same scope for the same (message, coroutine) pair
        val scope =
            parentScope +
                UUID.nameUUIDFromBytes(
                    (message.id.toString() + distributedCoroutineIdentifier.name).toByteArray(
                        StandardCharsets.UTF_8
                    )
                )

        fun childScopeRollback(stepName: String) =
            ROLLING_BACK_PREFIX + stepName + ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX

        fun childScopeRollback(stepName: Int) = childScopeRollback(stepName.toString())

        fun rollback(stepName: String) = ROLLING_BACK_PREFIX + stepName

        fun rollback(stepName: Int) = rollback(stepName.toString())

        fun emitted(
            topic: String,
            stepName: String,
            key: String = "key",
            value: String = "value",
        ): Message =
            with(SqlTestUtils(fluentJdbc, jsonbHelper)) {
                val message = createSimpleMessage(topic, key, value)
                emitted(message.id, distributedCoroutineIdentifier.step(stepName), scope)
                message
            }

        fun emitted(
            topic: String,
            stepName: Int,
            key: String = "key",
            value: String = "value",
        ): Message = emitted(topic, stepName.toString(), key, value)

        fun Message.childCoroutineProgress(
            name: String,
            block: CoroutineProgressBuilder.() -> Unit,
        ): Message = also {
            CoroutineProgressBuilder(
                    this,
                    DistributedCoroutineIdentifier(name, UUID.randomUUID().toString()),
                    scope,
                )
                .block()
        }

        fun seen() =
            SqlTestUtils(fluentJdbc, jsonbHelper)
                .seen(message.id, distributedCoroutineIdentifier, scope)

        fun suspended(stepName: String) =
            SqlTestUtils(fluentJdbc, jsonbHelper)
                .suspended(message.id, distributedCoroutineIdentifier.step(stepName), scope)

        fun suspended(stepName: Int) = suspended(stepName.toString())

        fun committed(stepName: String) =
            SqlTestUtils(fluentJdbc, jsonbHelper)
                .committed(message.id, distributedCoroutineIdentifier.step(stepName), scope)

        fun committed(stepName: Int) = committed(stepName.toString())

        fun rollingBack(throwable: Throwable, stepName: String? = null) =
            when (stepName) {
                null ->
                    SqlTestUtils(fluentJdbc, jsonbHelper)
                        .rollingBack(message.id, distributedCoroutineIdentifier, scope, throwable)
                else ->
                    SqlTestUtils(fluentJdbc, jsonbHelper)
                        .rollingBack(
                            message.id,
                            distributedCoroutineIdentifier.step(stepName),
                            scope,
                            throwable,
                        )
            }

        fun rollingBack(throwable: Throwable, stepName: Int) =
            rollingBack(throwable, stepName.toString())

        fun rollbackEmitted(childMessage: Message, stepName: String, throwable: Throwable) =
            SqlTestUtils(fluentJdbc, jsonbHelper)
                .rollbackEmitted(
                    childMessage.id,
                    distributedCoroutineIdentifier.step(stepName),
                    scope,
                    throwable,
                )

        fun rollbackEmitted(childMessage: Message, stepName: Int, throwable: Throwable) =
            rollbackEmitted(childMessage, stepName.toString(), throwable)

        fun rolledBack(stepName: String) =
            SqlTestUtils(fluentJdbc, jsonbHelper)
                .rolledBack(message.id, distributedCoroutineIdentifier.step(stepName), scope)

        fun rolledBack(stepName: Int) = rolledBack(stepName.toString())

        fun rollbackFailed(stepName: String, throwable: Throwable) =
            SqlTestUtils(fluentJdbc, jsonbHelper)
                .rollbackFailed(
                    message.id,
                    distributedCoroutineIdentifier.step(stepName),
                    scope,
                    throwable,
                )

        fun rollbackFailed(stepName: Int, throwable: Throwable) =
            rollbackFailed(stepName.toString(), throwable)

        fun verify(sql: SQL, block: (List<Row>) -> Unit) {
            val result =
                fluentJdbc
                    .query()
                    .select(sql.build())
                    .namedParam("coroutine_name", distributedCoroutineIdentifier.name)
                    .listResult(Mappers.map())

            block(result)
        }
    }

    fun Message.coroutineProgress(
        name: String,
        block: CoroutineProgressBuilder.() -> Unit,
    ): Message = also {
        CoroutineProgressBuilder(
                this,
                DistributedCoroutineIdentifier(name, UUID.randomUUID().toString()),
            )
            .block()
    }
}
