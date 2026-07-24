package de.example.gron.api

import groovy.transform.CompileStatic

/**
 * Base type for all GronScheduler failures.
 *
 * <p>Design decision: the public API uses an <strong>unchecked</strong>
 * exception hierarchy (extends {@link RuntimeException}). Scheduling and
 * persistence failures are almost always non-recoverable programming or
 * infrastructure errors; forcing every caller to declare or catch a checked
 * type (as Quartz does with its {@code SchedulerException} hierarchy) adds
 * ceremony without improving reliability. Handlers may still signal failures
 * with any exception they like; {@link TaskFailedException} is the only type
 * carrying extra scheduler semantics.</p>
 */
@CompileStatic
class GronException extends RuntimeException {

    GronException(String message) {
        super(message)
    }

    GronException(String message, Throwable cause) {
        super(message, cause)
    }
}
