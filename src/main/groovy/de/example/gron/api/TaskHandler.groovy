package de.example.gron.api

/**
 * The unit of work executed for a task. Implementations receive a
 * {@link TaskContext} describing the current run.
 *
 * <p>Handlers signal failure by throwing any exception. To additionally move
 * the task to {@link TaskState#FAILED}, throw a {@link TaskFailedException}
 * with {@code abortSchedule = true}.</p>
 *
 * <p>Implementations should be idempotent where possible: for
 * {@code recoverable} tasks a run may be re-executed after a node failure
 * (at-least-once semantics).</p>
 */
interface TaskHandler {

    /**
     * Executes one run of the task.
     *
     * @param context describes the current run (task id, params, timing, node)
     */
    void run(TaskContext context)
}
