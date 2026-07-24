package de.example.gron.api

/**
 * Controls how overdue runs are handled. A run is considered overdue when
 * {@code now - plannedTime > overdueAfter}, which typically happens after a
 * node outage, a pause, or a saturated worker pool. The policy is applied
 * store-side, both while claiming and when a persistent store is opened after
 * a restart.
 */
enum CatchUpPolicy {

    /** Discard overdue occurrences; only schedule the next regular run. */
    SKIP,

    /** Run one catch-up occurrence, then continue on the regular schedule. */
    RUN_ONCE,

    /** Run every missed occurrence in order, then continue on the regular schedule. */
    RUN_ALL
}
