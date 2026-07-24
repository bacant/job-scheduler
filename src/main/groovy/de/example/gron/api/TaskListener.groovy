package de.example.gron.api

import java.time.Instant

/**
 * The single listener type for observing task execution. One interface with
 * four callbacks is deliberately enough — GronScheduler does not offer the
 * multiple listener types with matchers that Quartz does.
 *
 * <p>Exceptions thrown by listener callbacks are logged at WARN and never
 * affect the scheduler or the task run.</p>
 */
interface TaskListener {

    /** Called just before a run's handler executes. */
    void beforeRun(TaskContext context)

    /** Called after a run finishes, with its terminal outcome. */
    void afterRun(TaskContext context, RunOutcome outcome)

    /** Called when a due occurrence is skipped rather than executed. */
    void onSkip(String taskId, Instant plannedTime, SkipReason reason)

    /** Called when a run throws (once per failed attempt). */
    void onError(TaskContext context, Throwable error)
}
