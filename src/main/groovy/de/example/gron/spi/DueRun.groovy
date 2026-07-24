package de.example.gron.spi

import de.example.gron.api.Task
import groovy.transform.CompileStatic

import java.time.Instant

/**
 * A claimed, ready-to-execute occurrence of a task. Returned by
 * {@link TaskStore#claimDue}, handed back on {@link TaskStore#release} and
 * finalized via {@link TaskStore#complete}.
 */
@CompileStatic
class DueRun implements Serializable {

    private static final long serialVersionUID = 1L

    /** The task this run belongs to. */
    final Task task

    /** The instant this run was planned for. */
    final Instant plannedTime

    /** The node that claimed this run. */
    final String claimedBy

    /** Opaque token identifying this specific claim (for optimistic release/complete). */
    final String claimToken

    DueRun(Task task, Instant plannedTime, String claimedBy, String claimToken) {
        this.task = task
        this.plannedTime = plannedTime
        this.claimedBy = claimedBy
        this.claimToken = claimToken
    }

    @Override
    String toString() {
        return "DueRun(taskId=${task?.id}, plannedTime=${plannedTime}, " +
                "claimedBy=${claimedBy}, claimToken=${claimToken})"
    }
}
