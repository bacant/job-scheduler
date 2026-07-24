package de.example.gron.cluster

import de.example.gron.spi.ClusterCoordinator
import de.example.gron.spi.NodeInfo
import groovy.transform.CompileStatic

import java.time.Clock
import java.time.Duration

/**
 * Default coordinator for single-node operation. Locks are no-ops (there is no
 * contention), there is exactly one active node, and no node is ever detected
 * as dead.
 */
@CompileStatic
class SingleNodeCoordinator implements ClusterCoordinator {

    private final String nodeId
    private final Set<String> tags
    private final Clock clock

    /** A no-op lock handle. */
    private static final AutoCloseable NOOP_LOCK = new AutoCloseable() {
        @Override
        void close() { }
    }

    SingleNodeCoordinator(String nodeId, Set<String> tags = Collections.<String> emptySet(),
                          Clock clock = Clock.systemUTC()) {
        this.nodeId = nodeId
        this.tags = tags == null ? Collections.<String> emptySet()
                : new LinkedHashSet<String>(tags)
        this.clock = clock
    }

    @Override
    NodeInfo localNode() {
        return new NodeInfo(nodeId, tags, clock.instant())
    }

    @Override
    void start() { }

    @Override
    void stop() { }

    @Override
    AutoCloseable lock(String name, Duration timeout) {
        return NOOP_LOCK
    }

    @Override
    List<NodeInfo> activeNodes() {
        return Collections.singletonList(localNode())
    }

    @Override
    List<NodeInfo> newlyDeadNodes() {
        return Collections.<NodeInfo> emptyList()
    }
}
