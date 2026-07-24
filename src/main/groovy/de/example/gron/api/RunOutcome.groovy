package de.example.gron.api

/** Terminal outcome of a single task run. */
enum RunOutcome {

    /** The run completed without throwing. */
    OK,

    /** The run threw and all retries (if any) were exhausted. */
    FAILED
}
