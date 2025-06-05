package io.github.gabrielshanahan.structmess.messaging

import com.github.f4b6a3.uuid.UuidCreator
import io.github.gabrielshanahan.structmess.coroutine.CancellationRequestedException
import io.github.gabrielshanahan.structmess.coroutine.CooperationContext
import io.github.gabrielshanahan.structmess.coroutine.CooperationScope
import io.github.gabrielshanahan.structmess.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.structmess.coroutine.GaveUpException
import io.github.gabrielshanahan.structmess.coroutine.ParentSaidSoException
import io.github.gabrielshanahan.structmess.coroutine.RollbackRequestedException
import io.github.gabrielshanahan.structmess.coroutine.RootCooperationScopeIdentifier
import io.github.gabrielshanahan.structmess.coroutine.emptyContext
import io.github.gabrielshanahan.structmess.coroutine.renderAsString
import io.github.gabrielshanahan.structmess.domain.CooperationFailure
import io.github.gabrielshanahan.structmess.domain.Message
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.infrastructure.Infrastructure
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.SqlConnection
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class StructuredCooperationManager(
    private val messageRepository: MessageRepository,
    private val messageEventRepository: MessageEventRepository,
) {
    fun launchOnGlobalScope(
        connection: SqlConnection,
        topic: String,
        payload: JsonObject,
        context: CooperationContext?,
    ): Uni<CooperationRoot> =
        messageRepository.insertMessage(connection, topic, payload).flatMap { message ->
            val cooperationId = UuidCreator.getTimeOrderedEpoch()
            val cooperationLineage = listOf(cooperationId)
            messageEventRepository
                .insertGlobalEmittedEvent(connection, message.id, cooperationLineage, context)
                .replaceWith(
                    CooperationRoot(RootCooperationScopeIdentifier(cooperationId), message)
                )
        }

    fun launch(
        scope: CooperationScope,
        topic: String,
        payload: JsonObject,
        additionalContext: CooperationContext?,
    ): Uni<Message> =
        messageRepository.insertMessage(scope.connection, topic, payload).flatMap { message ->
            messageEventRepository
                .insertScopedEmittedEvent(
                    scope.connection,
                    message.id,
                    scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.name,
                    scope.continuation.continuationIdentifier.distributedCoroutineIdentifier
                        .instance,
                    scope.continuation.continuationIdentifier.stepName,
                    scope.continuationCooperationScopeIdentifier.cooperationLineage,
                    scope.context + (additionalContext ?: emptyContext()),
                )
                .invoke { _ -> scope.emitted(message) }
                .replaceWith(message)
        }

    fun cancel(
        connection: SqlConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    ): Uni<Unit> {
        val exception = CancellationRequestedException(reason)
        val cooperationFailure = CooperationFailure.fromThrowable(exception, source)

        return messageEventRepository.insertCancellationRequestedEvent(
            connection,
            null,
            null,
            null,
            cooperationScopeIdentifier.cooperationLineage,
            cooperationFailure,
        )
    }

    fun cancel(scope: CooperationScope, reason: String): Uni<Nothing> {
        val exception = CancellationRequestedException(reason)
        val cooperationFailure =
            CooperationFailure.fromThrowable(
                exception,
                scope.continuation.continuationIdentifier.distributedCoroutineIdentifier
                    .renderAsString(),
            )

        return messageEventRepository
            .insertCancellationRequestedEvent(
                scope.connection,
                scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.name,
                scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.instance,
                scope.continuation.continuationIdentifier.stepName,
                scope.continuationCooperationScopeIdentifier.cooperationLineage,
                cooperationFailure,
            )
            .map { _ -> throw exception }
    }

    fun rollback(
        connection: SqlConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    ): Uni<Unit> {
        val exception = RollbackRequestedException(reason)
        val cooperationFailure = CooperationFailure.fromThrowable(exception, source)

        return messageEventRepository.insertRollbackEmittedEvent(
            connection,
            cooperationScopeIdentifier.cooperationLineage,
            cooperationFailure,
        )
    }

    fun emitRollbacksForEmissions(
        scope: CooperationScope,
        stepName: String,
        throwable: Throwable,
    ): Uni<Unit> {
        val cooperationFailure =
            CooperationFailure.fromThrowable(
                ParentSaidSoException(throwable),
                scope.continuation.continuationIdentifier.distributedCoroutineIdentifier
                    .renderAsString(),
            )

        return messageEventRepository.insertRollbackEmittedEventsForStep(
            scope.connection,
            stepName,
            scope.continuationCooperationScopeIdentifier.cooperationLineage,
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.name,
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.instance,
            scope.continuation.continuationIdentifier.stepName,
            cooperationFailure,
            scope.context,
        )
    }

    fun giveUpIfNecessary(
        scope: CooperationScope,
        giveUpSqlProvider: (String) -> String,
    ): Uni<Unit> =
        messageEventRepository
            .fetchGiveUpExceptions(
                scope.connection,
                giveUpSqlProvider,
                scope.continuationCooperationScopeIdentifier.cooperationLineage,
            )
            .emitOn(Infrastructure.getDefaultWorkerPool())
            .map { exceptions ->
                if (exceptions.any()) {
                    throw GaveUpException(exceptions)
                }
            }
}

data class CooperationRoot(
    val cooperationScopeIdentifier: RootCooperationScopeIdentifier,
    val message: Message,
)
