package de.example.gron.spi

/**
 * Optional capability a {@link TaskStore} may implement to support ad-hoc
 * ({@code runNow}) executions. Kept separate from the core {@link TaskStore}
 * SPI so it stays small; the scheduler reports {@code runNow} as unsupported
 * for stores that do not implement it.
 */
interface AdHocRunSupport {

    /**
     * Requests an immediate ad-hoc run of the task, subject to its
     * {@link de.example.gron.api.OverlapPolicy}. No-op if the task is absent,
     * paused or failed.
     */
    void triggerNow(String taskId)
}
