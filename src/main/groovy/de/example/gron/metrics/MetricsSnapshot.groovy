package de.example.gron.metrics

import groovy.transform.CompileStatic

/**
 * An immutable point-in-time view of all counters, timers and gauges of a
 * {@link ReadableMetrics} collector. Values are keyed by the canonical
 * {@link MetricKey} of the metric name and its tags.
 */
@CompileStatic
class MetricsSnapshot {

    private final Map<String, Long> counters
    private final Map<String, TimerSnapshot> timers
    private final Map<String, Number> gauges

    MetricsSnapshot(Map<String, Long> counters, Map<String, TimerSnapshot> timers,
                    Map<String, Number> gauges) {
        this.counters = Collections.unmodifiableMap(new LinkedHashMap<String, Long>(counters))
        this.timers = Collections.unmodifiableMap(new LinkedHashMap<String, TimerSnapshot>(timers))
        this.gauges = Collections.unmodifiableMap(new LinkedHashMap<String, Number>(gauges))
    }

    /** @return an empty snapshot (used when the collector is not readable). */
    static MetricsSnapshot empty() {
        return new MetricsSnapshot(Collections.<String, Long> emptyMap(),
                Collections.<String, TimerSnapshot> emptyMap(),
                Collections.<String, Number> emptyMap())
    }

    /** All counter values by canonical key. */
    Map<String, Long> counters() { return counters }

    /** All timer snapshots by canonical key. */
    Map<String, TimerSnapshot> timers() { return timers }

    /** All gauge values by canonical key. */
    Map<String, Number> gauges() { return gauges }

    /** @return the counter value for a name/tags pair, or 0 if absent. */
    long counter(String name, Map<String, String> tags = Collections.<String, String> emptyMap()) {
        Long v = counters.get(MetricKey.of(name, tags))
        return v == null ? 0L : v.longValue()
    }

    /** @return the timer snapshot for a name/tags pair, or {@code null} if absent. */
    TimerSnapshot timer(String name, Map<String, String> tags = Collections.<String, String> emptyMap()) {
        return timers.get(MetricKey.of(name, tags))
    }

    /** @return the gauge value for a name/tags pair, or {@code null} if absent. */
    Number gauge(String name, Map<String, String> tags = Collections.<String, String> emptyMap()) {
        return gauges.get(MetricKey.of(name, tags))
    }

    @Override
    String toString() {
        return "MetricsSnapshot(counters=${counters}, timers=${timers}, gauges=${gauges})"
    }
}
