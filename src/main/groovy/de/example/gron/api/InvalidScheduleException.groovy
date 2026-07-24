package de.example.gron.api

import groovy.transform.CompileStatic

/**
 * Thrown when a schedule cannot be constructed, for example because a cron
 * expression is syntactically invalid. The original cause (typically an
 * {@link IllegalArgumentException} from the underlying cron parser) is
 * preserved.
 */
@CompileStatic
class InvalidScheduleException extends GronException {

    InvalidScheduleException(String message) {
        super(message)
    }

    InvalidScheduleException(String message, Throwable cause) {
        super(message, cause)
    }
}
