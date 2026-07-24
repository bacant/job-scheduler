package de.example.gron.spi

import de.example.gron.api.Task
import de.example.gron.api.TaskState
import de.example.gron.api.RunOutcome

import java.time.Instant

/**
 * Persistence and run-state SPI. Deliberately small (at most ~15 methods).
 *
 * <p>An implementation stores task definitions and tracks per-occurrence run
 * state. {@link #claimDue} atomically claims due occurrences for a node,
 * applying the catch-up and placement rules; {@link #complete} records the
 * outcome and the next run.</p>
 *
 * <p>Thread-safety: all methods may be called concurrently by the control loop
 * and workers. Lock ordering within an implementation must be documented to
 * avoid deadlocks (see {@code MemoryTaskStore}).</p>
 */
interface TaskStore {

    /** Opens the store with its runtime environment. Called once before use. */
    void open(StoreContext context)

    /** Releases resources. Called once on scheduler stop. */
    void close()

    /** @return true if tasks survive a restart. */
    boolean isPersistent()

    /** @return true if the store may be used by multiple nodes concurrently. */
    boolean isShared()

    /** Inserts a task, or replaces an existing one with the same id. */
    void save(Task task)

    /** Removes a task and its run state. @return true if a task was removed. */
    boolean delete(String taskId)

    /** @return the task with the given id, or {@code null}. */
    Task find(String taskId)

    /** @return all tasks. */
    List<Task> findAll()

    /** @return all tasks carrying the given tag. */
    List<Task> findByTag(String tag)

    /** Pauses or resumes a task. */
    void setPaused(String taskId, boolean paused)

    /**
     * Moves a task to {@link TaskState#FAILED} (no further runs until resume or
     * resubmit). This transition is separate from {@link #complete} because the
     * fixed {@code complete} signature cannot distinguish an aborted task
     * (FAILED) from a naturally exhausted schedule (DONE, {@code nextRun == null}).
     */
    void setFailed(String taskId)

    /** @return the current state of the task, or {@code null} if absent. */
    TaskState stateOf(String taskId)

    /**
     * Atomically claims due runs up to {@code until} for the given node, at
     * most {@code limit}. The implementation applies the task's catch-up
     * policy and the placement/node-selector filter.
     *
     * @return the claimed runs (possibly empty)
     */
    List<DueRun> claimDue(Instant until, int limit, NodeInfo node)

    /** Returns an unexecuted claim to the store so it can be claimed again. */
    void release(DueRun run)

    /**
     * Records the outcome of a run and the next planned run.
     *
     * @param nextRun the next run instant, or {@code null} to finish the task
     *                (state {@link TaskState#DONE})
     */
    void complete(DueRun run, RunOutcome outcome, Instant nextRun)

    /**
     * Reclaims the claims held by a dead node for fail-over. Recoverable runs
     * are re-scheduled as immediate one-off runs; the rest are released and the
     * catch-up policy applies.
     *
     * @return the recoverable runs to execute immediately (possibly empty)
     */
    List<DueRun> reclaimFromDeadNode(String nodeId)
}
