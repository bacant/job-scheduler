package de.example.gron.spi

import java.time.Duration

/**
 * A single, lean cluster interface combining node registry, heartbeats and
 * cluster-wide locks. Splitting these into separate lock/registry/coordinator
 * interfaces (as heavier frameworks do) is deliberately avoided.
 */
interface ClusterCoordinator {

    /** @return the local node's id and tags. */
    NodeInfo localNode()

    /** Starts the coordinator and its internal heartbeat. */
    void start()

    /** Stops the coordinator and its heartbeat. */
    void stop()

    /**
     * Acquires a cluster-wide lock.
     *
     * @param name    lock name
     * @param timeout maximum time to wait
     * @return an {@link AutoCloseable} that releases the lock on close
     * @throws de.example.gron.api.ClusterLockTimeoutException if not acquired in time
     */
    AutoCloseable lock(String name, Duration timeout)

    /** @return the currently active nodes (including the local node). */
    List<NodeInfo> activeNodes()

    /** @return nodes detected as dead since the previous call. */
    List<NodeInfo> newlyDeadNodes()
}
