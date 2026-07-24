package de.example.gron.spi

import de.example.gron.api.SkipReason

import java.time.Instant

/**
 * Callback a store uses to report a skipped occurrence, so the scheduler can
 * forward it to its {@link de.example.gron.api.TaskListener}s. Overlap and
 * catch-up decisions are made store-side (they must be consistent cluster-wide
 * for shared stores), but listener dispatch lives in the scheduler; this sink
 * bridges the two without widening the {@link TaskStore#claimDue} return type.
 */
interface SkipSink {

    /** Reports that the given occurrence was skipped for the given reason. */
    void skipped(String taskId, Instant plannedTime, SkipReason reason)
}
