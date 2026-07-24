package de.example.gron.api

import groovy.transform.CompileStatic

/**
 * Thrown when a task's {@link Placement} cannot be honoured by the current
 * scheduler configuration, for example an {@code EVERY_NODE} task submitted
 * without an active, real replication provider.
 */
@CompileStatic
class UnsupportedPlacementException extends GronException {

    UnsupportedPlacementException(String message) {
        super(message)
    }

    UnsupportedPlacementException(String message, Throwable cause) {
        super(message, cause)
    }
}
