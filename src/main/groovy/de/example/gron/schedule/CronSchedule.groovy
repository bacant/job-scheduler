package de.example.gron.schedule

import de.example.gron.api.InvalidScheduleException
import de.example.gron.cron.CronExpression
import groovy.transform.CompileStatic

import java.time.Instant
import java.time.ZoneId

/**
 * A {@link Schedule} backed by the provided {@link CronExpression}. The wrapper
 * keeps the expression string and the {@link ZoneId} as its own fields (for
 * serialization and {@code toString()}) and delegates the actual computation to
 * {@link CronExpression#nextInstant(Instant)}.
 *
 * <p>When the underlying computation cannot find a match within its search
 * horizon it throws {@link IllegalStateException}; this wrapper maps that to a
 * {@code null} result, so the task is considered finished ({@code DONE}).</p>
 */
@CompileStatic
class CronSchedule implements Schedule {

    private static final long serialVersionUID = 1L

    /** Type tag used by the serializer. */
    static final String TYPE = 'cron'

    /** The original cron expression. */
    final String expression

    /** The zone in which the expression is evaluated. */
    final ZoneId zone

    /** The parsed expression; {@code transient} because it is rebuilt on read. */
    private transient CronExpression cron

    CronSchedule(String expression, ZoneId zone = ZoneId.systemDefault()) {
        this.expression = expression
        this.zone = zone ?: ZoneId.systemDefault()
        this.cron = buildCron(expression, this.zone)
    }

    private static CronExpression buildCron(String expression, ZoneId zone) {
        try {
            return new CronExpression(expression, zone)
        } catch (IllegalArgumentException e) {
            throw new InvalidScheduleException(
                    "Invalid cron expression '${expression}': ${e.message}", e)
        }
    }

    private CronExpression cron() {
        // Rebuilt lazily after deserialization (the parsed state is transient).
        if (cron == null) {
            cron = buildCron(expression, zone)
        }
        return cron
    }

    @Override
    Instant nextRunAfter(Instant reference) {
        try {
            return cron().nextInstant(reference)
        } catch (IllegalStateException ignored) {
            // No further match within the search horizon -> the task is done.
            return null
        }
    }

    @Override
    String toString() {
        return "CronSchedule('${expression}', zone=${zone})"
    }

    @Override
    boolean equals(Object o) {
        if (this.is(o)) {
            return true
        }
        if (!(o instanceof CronSchedule)) {
            return false
        }
        CronSchedule that = (CronSchedule) o
        return expression == that.expression && zone == that.zone
    }

    @Override
    int hashCode() {
        return Objects.hash(expression, zone)
    }
}
