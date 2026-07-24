package de.example.gron.api

import groovy.transform.CompileStatic

import java.time.Instant

/**
 * Convenience base class with empty default implementations of all
 * {@link TaskListener} callbacks. Subclass and override only what you need.
 */
@CompileStatic
class TaskListenerAdapter implements TaskListener {

    @Override
    void beforeRun(TaskContext context) { }

    @Override
    void afterRun(TaskContext context, RunOutcome outcome) { }

    @Override
    void onSkip(String taskId, Instant plannedTime, SkipReason reason) { }

    @Override
    void onError(TaskContext context, Throwable error) { }
}
