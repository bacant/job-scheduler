package de.example.gron.metrics

import groovy.transform.CompileStatic

import java.util.function.Supplier

/**
 * A metrics collector that records nothing. Used when metrics are disabled.
 */
@CompileStatic
class NoOpMetricsCollector implements MetricsCollector {

    private static final MetricCounter COUNTER = new MetricCounter() {
        @Override
        void increment() { }

        @Override
        void add(long delta) { }
    }

    private static final MetricTimer TIMER = new MetricTimer() {
        @Override
        void record(long elapsedNanos) { }
    }

    @Override
    MetricCounter counter(String name, Map<String, String> tags) {
        return COUNTER
    }

    @Override
    MetricTimer timer(String name, Map<String, String> tags) {
        return TIMER
    }

    @Override
    void gauge(String name, Map<String, String> tags, Supplier<Number> value) { }
}
