package de.example.gron.spi

import groovy.transform.CompileStatic

/**
 * A replicated change to task state. Carries a type, a serializer-encoded
 * payload, a monotonic sequence number and the originating node id. Conflict
 * resolution is last-writer-wins based on the sequence number; applying an
 * incoming event must be idempotent.
 */
@CompileStatic
class TaskChangeEvent implements Serializable {

    private static final long serialVersionUID = 1L

    /** The kind of change. */
    enum Type {
        TASK_SAVED,
        TASK_DELETED,
        TASK_PAUSED,
        TASK_RESUMED,
        RUN_COMPLETED
    }

    /** The change type. */
    final Type type

    /** Serializer-encoded payload (task JSON, task id, or run summary). */
    final String payload

    /** Monotonic sequence number for last-writer-wins ordering. */
    final long sequence

    /** Id of the node that produced this event. */
    final String originNodeId

    TaskChangeEvent(Type type, String payload, long sequence, String originNodeId) {
        this.type = type
        this.payload = payload
        this.sequence = sequence
        this.originNodeId = originNodeId
    }

    @Override
    String toString() {
        return "TaskChangeEvent(type=${type}, seq=${sequence}, origin=${originNodeId})"
    }
}
