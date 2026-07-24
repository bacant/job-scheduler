package de.example.gron.spi

import java.time.Instant

/**
 * Optional capability a {@link TaskStore} may implement so the control loop can
 * sleep exactly until the next due occurrence instead of polling. Kept separate
 * from {@link TaskStore} so the core SPI stays small; the loop falls back to a
 * bounded poll interval for stores that do not implement it.
 */
interface DueTimeAware {

    /**
     * @return the earliest instant at which a run will become due, or
     *         {@code null} if no run is currently scheduled
     */
    Instant nextDueTime()
}
