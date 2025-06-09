package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop

import io.github.gabrielshanahan.scoop.shared.coroutine.ScopeException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException

class ChildRolledBackException(causes: List<CooperationException>) :
    ScopeException(null, causes.first(), false) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
    }
}

class ChildRollbackFailedException(causes: List<Throwable>) :
    ScopeException(null, causes.first(), false) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
    }
}
