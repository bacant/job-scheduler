package de.example.gron.history

/** Controls which runs are written to history. */
enum HistoryMode {

    /** Record every run, retry attempt, skip and recovery (default). */
    ALL,

    /** Record only failures and skips (keeps noisy successful runs out). */
    FAILURES_AND_SKIPS,

    /** Record nothing. */
    OFF
}
