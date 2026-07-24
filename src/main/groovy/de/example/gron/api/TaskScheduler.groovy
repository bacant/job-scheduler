package de.example.gron.api

import java.time.Duration

/**
 * The public scheduler API. Implementations are thread-safe; multiple
 * scheduler instances may coexist in a single JVM (no forced static
 * singletons).
 */
interface TaskScheduler {

    /** Human-readable scheduler name. */
    String getName()

    /** Id of the local node this scheduler runs as. */
    String getNodeId()

    /** Starts the control loop and worker pool. Idempotent. */
    void start()

    /** @return true while the scheduler is running. */
    boolean isRunning()

    /**
     * Stops the scheduler gracefully: no new claims are taken, open claims are
     * released, and running tasks are awaited up to {@code awaitRunning}
     * before their workers are interrupted. {@code Duration.ZERO} stops
     * immediately.
     */
    void stop(Duration awaitRunning)

    /** Inserts a task, or replaces an existing one with the same id atomically. */
    void submit(Task task)

    /** Removes a task. @return true if a task was removed. */
    boolean cancel(String taskId)

    /** Pauses a task: no further runs are claimed until {@link #resume}. */
    void pause(String taskId)

    /** Resumes a paused task. */
    void resume(String taskId)

    /** Triggers an ad-hoc run now, respecting the task's {@link OverlapPolicy}. */
    void runNow(String taskId)

    /** @return the task with the given id, or {@code null} if absent. */
    Task find(String taskId)

    /** @return all registered tasks. */
    List<Task> findAll()

    /** @return all tasks carrying the given tag. */
    List<Task> findByTag(String tag)

    /** @return the current {@link TaskState} of the task, or {@code null} if absent. */
    TaskState stateOf(String taskId)

    /** @return contexts for all runs currently executing on this node. */
    List<TaskContext> activeRuns()

    /** Registers a listener. */
    void addListener(TaskListener listener)

    /** Removes a previously registered listener. */
    void removeListener(TaskListener listener)
}
