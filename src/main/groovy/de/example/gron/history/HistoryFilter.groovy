package de.example.gron.history

import groovy.transform.CompileStatic

import java.time.Instant

/**
 * Shared in-memory filter/order semantics for {@link HistoryQuery}, used by the
 * memory and file history stores so they behave identically.
 */
@CompileStatic
final class HistoryFilter {

    private HistoryFilter() { }

    /** @return true if the record satisfies the query filter (limit aside). */
    static boolean matches(RunRecord r, HistoryQuery q) {
        if (q.taskId != null && q.taskId != r.taskId) {
            return false
        }
        if (q.tag != null && !r.tags.contains(q.tag)) {
            return false
        }
        if (q.outcomes != null && !q.outcomes.isEmpty() && !q.outcomes.contains(r.outcome)) {
            return false
        }
        if (q.nodeId != null && q.nodeId != r.nodeId) {
            return false
        }
        Instant t = RunRecordJson.recordTime(r)
        if (q.from != null && t.isBefore(q.from)) {
            return false
        }
        if (q.to != null && t.isAfter(q.to)) {
            return false
        }
        if (q.startedBefore != null && !t.isBefore(q.startedBefore)) {
            return false
        }
        return true
    }

    /** @return effective limit (default 100 if non-positive). */
    static int limit(HistoryQuery q) {
        return q.limit > 0 ? q.limit : 100
    }
}
