package de.example.gron.api

import groovy.transform.CompileStatic

/**
 * Thrown when a {@link de.example.gron.spi.TaskStore} operation fails, for
 * example on a serialization error or an underlying SQL/IO failure.
 */
@CompileStatic
class TaskStoreException extends GronException {

    TaskStoreException(String message) {
        super(message)
    }

    TaskStoreException(String message, Throwable cause) {
        super(message, cause)
    }
}
