package de.example.gron.spi

/**
 * Callback a store (or coordinator) uses to wake the scheduler control loop
 * early, for example after a remote change made a run due sooner than the
 * loop's current sleep target.
 */
interface Signaler {

    /** Wakes the control loop so it re-evaluates due runs immediately. */
    void signal()
}
