package de.example.gron.api

/**
 * Controls what happens when a run of a task becomes due while another run of
 * the <em>same</em> task is still active. With a shared store the policy is
 * enforced cluster-wide via the store's claim bookkeeping.
 */
enum OverlapPolicy {

    /** Runs may overlap; every due run is executed regardless of active runs. */
    PARALLEL,

    /**
     * Due runs that occur during an active run are skipped (reported via
     * {@link TaskListener#onSkip}); the next regular occurrence is scheduled
     * normally. This is the default.
     */
    SKIP,

    /**
     * At most one pending run is remembered and started right after the active
     * run finishes. Further occurrences during the wait collapse into that
     * single pending run.
     */
    WAIT
}
