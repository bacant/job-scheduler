package de.example.gron.schedule

import groovy.transform.CompileStatic

import java.time.Instant

/**
 * A schedule that runs exactly once, at a fixed instant. After that single run
 * {@link #nextRunAfter} returns {@code null} and the task is finished.
 */
@CompileStatic
class OneTimeSchedule implements Schedule {

    private static final long serialVersionUID = 1L

    /** Type tag used by the serializer. */
    static final String TYPE = 'once'

    /** The single instant this task runs at. */
    final Instant at

    OneTimeSchedule(Instant at) {
        if (at == null) {
            throw new IllegalArgumentException('OneTimeSchedule instant must not be null')
        }
        this.at = at
    }

    @Override
    Instant nextRunAfter(Instant reference) {
        return at.isAfter(reference) ? at : null
    }

    @Override
    String toString() {
        return "OneTimeSchedule(at=${at})"
    }

    @Override
    boolean equals(Object o) {
        if (this.is(o)) {
            return true
        }
        if (!(o instanceof OneTimeSchedule)) {
            return false
        }
        return at == ((OneTimeSchedule) o).at
    }

    @Override
    int hashCode() {
        return at.hashCode()
    }
}
