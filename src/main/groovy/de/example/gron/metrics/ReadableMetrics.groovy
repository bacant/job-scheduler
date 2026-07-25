package de.example.gron.metrics

/**
 * A {@link MetricsCollector} that can also expose its current values as an
 * immutable {@link MetricsSnapshot}. The default {@code SimpleMetrics}
 * implements this; adapters that forward to an external system typically do not.
 */
interface ReadableMetrics extends MetricsCollector {

    /** @return an immutable snapshot of all counters, timers and gauges. */
    MetricsSnapshot snapshot()
}
