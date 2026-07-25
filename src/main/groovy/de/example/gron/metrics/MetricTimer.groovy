package de.example.gron.metrics

/**
 * Records elapsed durations in nanoseconds. The API takes a {@code long} rather
 * than a {@code Duration} so the hot path stays allocation-free.
 */
interface MetricTimer {

    /** Records a single elapsed duration in nanoseconds. */
    void record(long elapsedNanos)
}
