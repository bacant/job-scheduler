package de.example.gron.metrics

import groovy.transform.CompileStatic

/**
 * Immutable statistics for a single timer at snapshot time. No percentiles are
 * provided by the default implementation (see {@code SimpleMetrics}); attach a
 * percentile-capable library through the {@link MetricsCollector} SPI if needed.
 */
@CompileStatic
class TimerSnapshot {

    /** Number of recorded samples. */
    final long count

    /** Sum of all recorded durations, in nanoseconds. */
    final long totalNanos

    /** Smallest recorded duration in nanoseconds (0 if {@code count == 0}). */
    final long minNanos

    /** Largest recorded duration in nanoseconds (0 if {@code count == 0}). */
    final long maxNanos

    TimerSnapshot(long count, long totalNanos, long minNanos, long maxNanos) {
        this.count = count
        this.totalNanos = totalNanos
        this.minNanos = minNanos
        this.maxNanos = maxNanos
    }

    /** @return the mean duration in nanoseconds (0 if no samples). */
    double meanNanos() {
        return count == 0L ? 0.0d : ((double) totalNanos) / ((double) count)
    }

    @Override
    String toString() {
        return "TimerSnapshot(count=${count}, total=${totalNanos}ns, min=${minNanos}ns, " +
                "max=${maxNanos}ns, mean=${meanNanos()}ns)"
    }
}
