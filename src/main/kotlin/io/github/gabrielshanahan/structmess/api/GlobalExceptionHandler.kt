package io.github.gabrielshanahan.structmess.api

import jakarta.ws.rs.core.Response
import jakarta.ws.rs.ext.ExceptionMapper
import jakarta.ws.rs.ext.Provider

/** Global exception handler for uncaught exceptions. */
@Provider
class GlobalExceptionHandler : ExceptionMapper<Exception> {

    override fun toResponse(exception: Exception): Response {
        val status =
            when (exception) {
                is IllegalArgumentException -> Response.Status.BAD_REQUEST
                else -> Response.Status.INTERNAL_SERVER_ERROR
            }

        val error =
            mapOf(
                "error" to (exception.message ?: "An error occurred"),
                "type" to exception.javaClass.simpleName,
            )

        return Response.status(status).entity(error).build()
    }
}
