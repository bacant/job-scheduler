package de.example.gron.schedule

import java.time.Instant

/**
 * A schedule computes the next run time of a task. Implementations are
 * immutable, thread-safe and persistable through the serializer SPI (they
 * carry a type tag and their own fields).
 */
interface Schedule extends Serializable {

    /**
     * Returns the first run strictly after the given reference instant.
     *
     * @param reference the instant to search after (exclusive)
     * @return the next run instant, or {@code null} if there is no further run
     */
    Instant nextRunAfter(Instant reference)
}
