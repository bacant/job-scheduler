package de.example.gron.api

/**
 * Externally visible lifecycle state of a task. Exactly these five states are
 * exposed; internal claim/lease states remain private to the
 * {@link de.example.gron.spi.TaskStore}.
 */
enum TaskState {

    /** The task is registered and waiting for its next run. */
    SCHEDULED,

    /** At least one run of the task is currently executing. */
    RUNNING,

    /** The task is paused; no runs are claimed until it is resumed. */
    PAUSED,

    /** The task was aborted via {@link TaskFailedException#abortSchedule}. */
    FAILED,

    /** The schedule has no further runs; the task is finished. */
    DONE
}
