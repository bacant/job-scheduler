package de.example.gron.api

import groovy.transform.CompileStatic

/**
 * Thrown when a cluster-wide lock cannot be acquired within the requested
 * timeout.
 */
@CompileStatic
class ClusterLockTimeoutException extends GronException {

    ClusterLockTimeoutException(String message) {
        super(message)
    }

    ClusterLockTimeoutException(String message, Throwable cause) {
        super(message, cause)
    }
}
