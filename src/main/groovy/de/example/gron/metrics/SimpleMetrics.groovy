package de.example.gron.metrics

import groovy.transform.CompileStatic

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAccumulator
import java.util.concurrent.atomic.LongAdder
import java.util.function.Supplier

/**
 * Default {@link ReadableMetrics} using only JDK primitives. Counters and timer
 * accumulators use {@link LongAdder}/{@link LongAccumulator} rather than
 * {@link java.util.concurrent.atomic.AtomicLong}, because they scale much better
 * under the high update contention of a busy scheduler (many worker threads
 * incrementing the same counters).
 *
 * <p>Timers expose count, total, min, max and mean. Percentiles are
 * intentionally omitted from the default (they require bounded-memory histograms
 * or reservoirs); plug in a percentile-capable library via the
 * {@link MetricsCollector} SPI if you need them.</p>
 */
@CompileStatic
class SimpleMetrics implements ReadableMetrics {

    private final ConcurrentHashMap<String, AdderCounter> counters = new ConcurrentHashMap<>()
    private final ConcurrentHashMap<String, AccumulatingTimer> timers = new ConcurrentHashMap<>()
    private final ConcurrentHashMap<String, Supplier<Number>> gauges = new ConcurrentHashMap<>()

    @Override
    MetricCounter counter(String name, Map<String, String> tags) {
        return counters.computeIfAbsent(MetricKey.of(name, tags),
                { String k -> new AdderCounter() })
    }

    @Override
    MetricTimer timer(String name, Map<String, String> tags) {
        return timers.computeIfAbsent(MetricKey.of(name, tags),
                { String k -> new AccumulatingTimer() })
    }

    @Override
    void gauge(String name, Map<String, String> tags, Supplier<Number> value) {
        gauges.put(MetricKey.of(name, tags), value)
    }

    @Override
    MetricsSnapshot snapshot() {
        Map<String, Long> c = new LinkedHashMap<>()
        for (Map.Entry<String, AdderCounter> e : counters.entrySet()) {
            c.put(e.key, e.value.adder.sum())
        }
        Map<String, TimerSnapshot> t = new LinkedHashMap<>()
        for (Map.Entry<String, AccumulatingTimer> e : timers.entrySet()) {
            t.put(e.key, e.value.snapshot())
        }
        Map<String, Number> g = new LinkedHashMap<>()
        for (Map.Entry<String, Supplier<Number>> e : gauges.entrySet()) {
            try {
                g.put(e.key, e.value.get())
            } catch (Exception ignored) {
                g.put(e.key, 0)
            }
        }
        return new MetricsSnapshot(c, t, g)
    }

    /** Counter backed by a {@link LongAdder}. */
    @CompileStatic
    private static class AdderCounter implements MetricCounter {
        final LongAdder adder = new LongAdder()

        @Override
        void increment() { adder.increment() }

        @Override
        void add(long delta) { adder.add(delta) }
    }

    /** Timer backed by adders for count/total and accumulators for min/max. */
    @CompileStatic
    private static class AccumulatingTimer implements MetricTimer {
        final LongAdder count = new LongAdder()
        final LongAdder total = new LongAdder()
        final LongAccumulator min = new LongAccumulator({ long a, long b -> Math.min(a, b) } as java.util.function.LongBinaryOperator, Long.MAX_VALUE)
        final LongAccumulator max = new LongAccumulator({ long a, long b -> Math.max(a, b) } as java.util.function.LongBinaryOperator, Long.MIN_VALUE)

        @Override
        void record(long elapsedNanos) {
            count.increment()
            total.add(elapsedNanos)
            min.accumulate(elapsedNanos)
            max.accumulate(elapsedNanos)
        }

        TimerSnapshot snapshot() {
            long n = count.sum()
            if (n == 0L) {
                return new TimerSnapshot(0L, 0L, 0L, 0L)
            }
            return new TimerSnapshot(n, total.sum(), min.get(), max.get())
        }
    }
}
