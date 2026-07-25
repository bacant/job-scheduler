package de.example.gron.spi

import java.time.Instant

/**
 * Optional capability a {@link TaskStore} may implement to back scheduler gauges
 * cheaply. {@link #taskCount} and {@link #pausedCount} are maintained as O(1)
 * counters. {@link #backlogCount} counts occurrences already due but not yet
 * claimed; it is a bounded scan of the due index (not on the hot path — read
 * only when a metrics snapshot is taken), since a time-driven backlog cannot be
 * maintained purely incrementally.
 */
interface StoreStats {

    /** @return the total number of stored tasks (O(1)). */
    int taskCount()

    /** @return the number of paused tasks (O(1)). */
    int pausedCount()

    /** @return the number of occurrences due at or before {@code until} and unclaimed. */
    int backlogCount(Instant until)
}
