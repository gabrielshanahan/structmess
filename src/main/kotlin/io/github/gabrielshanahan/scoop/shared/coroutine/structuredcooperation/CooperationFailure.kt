package io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation

import java.util.Objects

data class StackTraceFrame(
    val fileName: String,
    val lineNumber: Int,
    // Not all languages have classes
    val className: String? = null,
    // Could be the toplevel of a script
    val functionName: String? = null,
)

private const val NO_MESSAGE = "<no message>"
private const val UNKNOWN_FILENAME = "<unknown filename>"
private const val UNKNOWN_CLASSNAME = "<unknown class>"
private const val UNKNOWN_FUNCTION = "<unknown function>"

data class CooperationFailure(
    val message: String,
    val type: String,
    // Identifier of the system which threw the exception
    val source: String,
    val stackTrace: List<StackTraceFrame>,
    val causes: List<CooperationFailure> = emptyList(),
) {
    companion object {

        fun fromThrowable(throwable: Throwable, source: String): CooperationFailure {
            val stackTrace =
                throwable.stackTrace.map { element ->
                    StackTraceFrame(
                        fileName = element.fileName ?: UNKNOWN_FILENAME,
                        lineNumber = element.lineNumber,
                        className = element.className.takeIf { it != UNKNOWN_CLASSNAME },
                        functionName = element.methodName.takeIf { it != UNKNOWN_FUNCTION },
                    )
                }

            val causes = buildList {
                throwable.cause?.takeIf { it != throwable }?.also { add(fromThrowable(it, source)) }

                throwable.suppressed.forEach { suppressed ->
                    add(fromThrowable(suppressed, source))
                }
            }

            return if (throwable is CooperationException) {
                CooperationFailure(
                    message =
                        throwable.message.substringAfter(
                            "[${throwable.source}] ${throwable.type}: "
                        ),
                    type = throwable.type,
                    source = throwable.source,
                    stackTrace = stackTrace,
                    causes = causes,
                )
            } else {
                CooperationFailure(
                    message = throwable.message ?: NO_MESSAGE,
                    type = throwable.javaClass.name,
                    source = source,
                    stackTrace = stackTrace,
                    causes = causes,
                )
            }
        }

        fun toCooperationException(cooperationFailure: CooperationFailure): CooperationException =
            CooperationException(
                message = cooperationFailure.message,
                type = cooperationFailure.type,
                source = cooperationFailure.source,
                stackTraceElements =
                    cooperationFailure.stackTrace
                        .map { element ->
                            StackTraceElement(
                                element.className ?: UNKNOWN_CLASSNAME,
                                element.functionName ?: UNKNOWN_FUNCTION,
                                element.fileName.takeIf { it != UNKNOWN_FILENAME },
                                element.lineNumber,
                            )
                        }
                        .toTypedArray(),
                causes = cooperationFailure.causes.map(::toCooperationException),
            )
    }
}

class CooperationException(
    message: String,
    val type: String,
    val source: String,
    stackTraceElements: Array<StackTraceElement>,
    val causes: List<CooperationException>,
) :
    Exception(
        "[$source] $type: ${message.takeIf { it.isNotBlank() } ?: NO_MESSAGE}",
        causes.firstOrNull(),
        true,
        true,
    ) {
    init {
        stackTrace = stackTraceElements
        causes.drop(1).forEach(::addSuppressed)
    }

    override val message: String
        get() = super.message!!

    override val cause: CooperationException?
        get() = causes.firstOrNull()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CooperationException

        if (type != other.type) return false
        if (source != other.source) return false
        if (causes != other.causes) return false
        if (message != other.message) return false
        if (!stackTrace.contentEquals(other.stackTrace)) return false

        return true
    }

    override fun hashCode() = Objects.hash(type, source, causes, message, stackTrace)
}
