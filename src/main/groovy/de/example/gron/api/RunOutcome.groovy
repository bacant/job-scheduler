package de.example.gron.api

/** Terminal outcome of a single run, as recorded in history. */
enum RunOutcome {

    /** The run completed without throwing. */
    OK,

    /** The run threw and all retries (if any) were exhausted. */
    FAILED,

    /**
     * A due occurrence was skipped rather than executed (overlap or overdue).
     * This value only ever appears in run history; {@link TaskListener#afterRun}
     * still receives only {@link #OK} or {@link #FAILED}.
     */
    SKIPPED
}
