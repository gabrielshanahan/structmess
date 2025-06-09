package io.github.gabrielshanahan.scoop.shared.coroutine

abstract class ScopeException(message: String?, cause: Throwable?, stackTrace: Boolean) :
    Exception(message, cause, true, stackTrace)
