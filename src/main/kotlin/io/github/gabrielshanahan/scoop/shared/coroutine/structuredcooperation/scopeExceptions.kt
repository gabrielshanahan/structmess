package io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.shared.coroutine.ScopeException

class ParentSaidSoException(cause: Throwable) : ScopeException(null, cause, false)

class CancellationRequestedException(
    reason: String,
    causes: List<CooperationException> = emptyList(),
) : ScopeException(reason, causes.firstOrNull(), true) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
        // Point to the place where cancel() was actually called
        stackTrace = stackTrace.drop(1).toTypedArray()
    }
}

class RollbackRequestedException(reason: String, causes: List<CooperationException> = emptyList()) :
    ScopeException(reason, causes.firstOrNull(), true) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
        // Point to the place where rollback() was actually called
        stackTrace = stackTrace.drop(1).toTypedArray()
    }
}

class GaveUpException(causes: List<Throwable>) : ScopeException(null, causes.first(), false) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
    }
}
