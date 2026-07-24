package de.example.gron.schedule

import groovy.transform.CompileStatic

import java.time.Duration
import java.time.Instant

/**
 * A fixed-rate schedule. Runs occur at {@code start}, {@code start + period},
 * {@code start + 2·period}, and so on.
 *
 * <p><strong>No drift:</strong> the next run is derived from the fixed anchor
 * ({@code start}), not from when a run actually executed. Because the store
 * feeds each computation the previous <em>planned</em> time (never the actual
 * finish time), slow runs never push the schedule forward.</p>
 *
 * <ul>
 *   <li>If {@code start} is {@code null}, the first run occurs one
 *       {@code period} after the reference passed on the first computation
 *       (typically submission time). In that mode {@code maxRuns} must be
 *       {@code null}, since there is no fixed anchor to count occurrences
 *       from.</li>
 *   <li>If {@code maxRuns} is set, exactly {@code maxRuns} occurrences are
 *       produced (indices {@code 0..maxRuns-1}); afterwards
 *       {@link #nextRunAfter} returns {@code null}.</li>
 * </ul>
 */
@CompileStatic
class IntervalSchedule implements Schedule {

    private static final long serialVersionUID = 1L

    /** Type tag used by the serializer. */
    static final String TYPE = 'interval'

    /** The fixed period between runs. */
    final Duration period

    /** The anchor instant, or {@code null} for a lazily anchored schedule. */
    final Instant start

    /** The maximum number of runs, or {@code null} for unbounded. */
    final Integer maxRuns

    IntervalSchedule(Duration period, Instant start = null, Integer maxRuns = null) {
        if (period == null || period.isZero() || period.isNegative()) {
            throw new IllegalArgumentException('Interval period must be positive')
        }
        if (start == null && maxRuns != null) {
            throw new IllegalArgumentException(
                    'maxRuns requires a non-null start (no anchor to count from otherwise)')
        }
        if (maxRuns != null && maxRuns < 1) {
            throw new IllegalArgumentException('maxRuns must be at least 1')
        }
        this.period = period
        this.start = start
        this.maxRuns = maxRuns
    }

    @Override
    Instant nextRunAfter(Instant reference) {
        if (start == null) {
            // Lazily anchored: the next run is one period after the reference.
            return reference.plus(period)
        }
        long periodNanos = period.toNanos()
        long n
        if (reference.isBefore(start)) {
            n = 0L
        } else {
            long diffNanos = Duration.between(start, reference).toNanos()
            n = Math.floorDiv(diffNanos, periodNanos) + 1L
        }
        if (maxRuns != null && n >= maxRuns.longValue()) {
            return null
        }
        return start.plusNanos(n * periodNanos)
    }

    @Override
    String toString() {
        return "IntervalSchedule(period=${period}, start=${start}, maxRuns=${maxRuns})"
    }

    @Override
    boolean equals(Object o) {
        if (this.is(o)) {
            return true
        }
        if (!(o instanceof IntervalSchedule)) {
            return false
        }
        IntervalSchedule that = (IntervalSchedule) o
        return period == that.period && start == that.start && maxRuns == that.maxRuns
    }

    @Override
    int hashCode() {
        return Objects.hash(period, start, maxRuns)
    }
}
