package de.example.gron.api

import groovy.transform.CompileStatic

import java.time.Instant

/**
 * Context passed to a {@link TaskHandler} for a single run. Instances are
 * created by the scheduler; the {@link #params} map is an immutable view.
 */
@CompileStatic
class TaskContext {

    /** The id of the task being run. */
    final String taskId

    /** Immutable view of the task parameters. */
    final Map<String, Object> params

    /** The instant this run was planned to start. */
    final Instant plannedTime

    /** The instant this run actually started. */
    final Instant startedAt

    /** The previous run's planned time, or {@code null} if this is the first. */
    final Instant previousRun

    /** The next planned run after this one, or {@code null} if none. */
    final Instant nextRun

    /** Attempt counter: 1 on the first try, greater than 1 on retries. */
    final int attempt

    /** The id of the node executing this run. */
    final String nodeId

    /** The scheduler owning this run (for {@code runNow}, {@code find}, ...). */
    final TaskScheduler scheduler

    TaskContext(Map<String, ?> args) {
        this.taskId = (String) args.taskId
        Map<String, Object> p = (Map<String, Object>) (args.params ?: [:])
        this.params = Collections.unmodifiableMap(new LinkedHashMap<String, Object>(p))
        this.plannedTime = (Instant) args.plannedTime
        this.startedAt = (Instant) args.startedAt
        this.previousRun = (Instant) args.previousRun
        this.nextRun = (Instant) args.nextRun
        this.attempt = args.attempt == null ? 1 : ((Number) args.attempt).intValue()
        this.nodeId = (String) args.nodeId
        this.scheduler = (TaskScheduler) args.scheduler
    }

    @Override
    String toString() {
        return "TaskContext(taskId=${taskId}, plannedTime=${plannedTime}, " +
                "attempt=${attempt}, nodeId=${nodeId})"
    }
}
