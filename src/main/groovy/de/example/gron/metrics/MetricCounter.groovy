package de.example.gron.metrics

/**
 * A monotonically increasing counter. Implementations must be safe for
 * concurrent updates and allocation-free on the hot path.
 */
interface MetricCounter {

    /** Increments the counter by one. */
    void increment()

    /** Adds the given non-negative delta. */
    void add(long delta)
}
