package de.example.gron.api

import groovy.transform.CompileStatic

/**
 * Optional exception a {@link TaskHandler} may throw to signal a failure with
 * extra scheduler semantics. Handlers are free to signal failures with any
 * exception; throwing this type additionally allows the handler to request
 * that the task be moved to {@link TaskState#FAILED} (no further runs until
 * {@code resume} or a fresh {@code submit}) by setting {@link #abortSchedule}.
 */
@CompileStatic
class TaskFailedException extends GronException {

    /**
     * When {@code true}, the scheduler moves the task to
     * {@link TaskState#FAILED} after the final attempt, suppressing all future
     * scheduled runs until the task is resumed or resubmitted.
     */
    final boolean abortSchedule

    TaskFailedException(String message, boolean abortSchedule = false) {
        super(message)
        this.abortSchedule = abortSchedule
    }

    TaskFailedException(String message, Throwable cause, boolean abortSchedule = false) {
        super(message, cause)
        this.abortSchedule = abortSchedule
    }
}
