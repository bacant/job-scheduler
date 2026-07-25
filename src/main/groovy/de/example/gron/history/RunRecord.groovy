package de.example.gron.history

import de.example.gron.api.RunOutcome
import de.example.gron.api.SkipReason
import groovy.transform.CompileStatic

import java.time.Instant

/**
 * An immutable record of a single run attempt, skip or recovery replay. Written
 * by the scheduler core (never by a {@link de.example.gron.spi.TaskStore}) and
 * queried through {@link HistoryStore}.
 */
@CompileStatic
class RunRecord {

    /** Unique run id; equals the claim token of the run. */
    final String runId

    final String taskId

    /** Denormalized copy of the task tags at run time. */
    final Set<String> tags

    final Instant plannedTime

    /** When the run started; {@code null} for SKIPPED entries. */
    final Instant startedAt

    /** When the run finished; {@code null} for SKIPPED entries. */
    final Instant finishedAt

    /** Wall-clock duration in milliseconds; 0 for SKIPPED entries. */
    final long durationMillis

    final String nodeId

    /** Attempt counter, 1-based. */
    final int attempt

    /** {@code OK}, {@code FAILED} or {@code SKIPPED}. */
    final RunOutcome outcome

    /** Only set for SKIPPED entries. */
    final SkipReason skipReason

    /** True if this run was replayed after a node failure. */
    final boolean recovered

    /** Error class FQCN, or {@code null}. */
    final String errorType

    /** Error message (truncated), or {@code null}. */
    final String errorMessage

    /** Error stack trace (truncated), or {@code null}. */
    final String errorStackTrace

    RunRecord(Map<String, ?> args) {
        this.runId = (String) args.runId
        this.taskId = (String) args.taskId
        Set<String> t = (Set<String>) args.tags
        this.tags = t == null ? Collections.<String> emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<String>(t))
        this.plannedTime = (Instant) args.plannedTime
        this.startedAt = (Instant) args.startedAt
        this.finishedAt = (Instant) args.finishedAt
        this.durationMillis = args.durationMillis == null ? 0L : ((Number) args.durationMillis).longValue()
        this.nodeId = (String) args.nodeId
        this.attempt = args.attempt == null ? 1 : ((Number) args.attempt).intValue()
        this.outcome = (RunOutcome) args.outcome
        this.skipReason = (SkipReason) args.skipReason
        this.recovered = Boolean.TRUE == args.recovered
        this.errorType = (String) args.errorType
        this.errorMessage = (String) args.errorMessage
        this.errorStackTrace = (String) args.errorStackTrace
    }

    @Override
    String toString() {
        return "RunRecord(runId=${runId}, taskId=${taskId}, plannedTime=${plannedTime}, " +
                "outcome=${outcome}, attempt=${attempt}, node=${nodeId}, recovered=${recovered})"
    }
}
