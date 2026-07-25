package de.example.gron.metrics

import java.util.function.Supplier

/**
 * The metrics facade. This is the bridge to external metrics libraries: the
 * core only ever talks to this small SPI, so a Micrometer (or other) adapter can
 * replace the default without touching the scheduler.
 */
interface MetricsCollector {

    /** Returns (creating if needed) the counter for the given name and tags. */
    MetricCounter counter(String name, Map<String, String> tags)

    /** Returns (creating if needed) the timer for the given name and tags. */
    MetricTimer timer(String name, Map<String, String> tags)

    /**
     * Registers a gauge backed by a supplier. The supplier must be O(1) and
     * safe to call from any thread; it is read on demand (e.g. at snapshot or
     * scrape time).
     */
    void gauge(String name, Map<String, String> tags, Supplier<Number> value)
}
