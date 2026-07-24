package de.example.gron.api

/** Reason a due run was skipped rather than executed. */
enum SkipReason {

    /** Skipped because another run of the same task was still active
     *  (see {@link OverlapPolicy#SKIP}). */
    OVERLAP,

    /** Skipped because the run was overdue and the {@link CatchUpPolicy}
     *  discarded it (see {@link CatchUpPolicy#SKIP}). */
    OVERDUE
}
