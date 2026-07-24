package de.example.gron.replication

import de.example.gron.spi.ReplicationContext
import de.example.gron.spi.ReplicationProvider
import de.example.gron.spi.TaskChangeEvent
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.Consumer

/**
 * A real {@link ReplicationProvider} that couples several schedulers/stores
 * <strong>within a single JVM</strong> through a shared {@link Bus}. It is the
 * reference implementation and the basis for the {@code EVERY_NODE}
 * demonstration; a network transport is intentionally out of scope for the
 * library.
 *
 * <p>Delivery is synchronous and skips the originating node. Applying an event
 * is idempotent on the receiving store, so re-delivery is harmless. Conflict
 * resolution is last-writer-wins by the event sequence number (the receiving
 * store's {@code save}/{@code delete} are already last-writer-wins).</p>
 */
@CompileStatic
class InMemoryReplicationProvider implements ReplicationProvider {

    private static final Logger log = LoggerFactory.getLogger(InMemoryReplicationProvider)

    /** The shared in-JVM channel connecting a set of providers. */
    static class Bus {
        final List<InMemoryReplicationProvider> members = new CopyOnWriteArrayList<>()
    }

    private final Bus bus
    private Consumer<TaskChangeEvent> consumer
    private volatile boolean started

    InMemoryReplicationProvider(Bus bus) {
        this.bus = bus
    }

    @Override
    void start(ReplicationContext context) {
        this.started = true
        bus.members.add(this)
    }

    @Override
    void stop() {
        this.started = false
        bus.members.remove(this)
    }

    @Override
    void publish(TaskChangeEvent event) {
        if (!started) {
            return
        }
        for (InMemoryReplicationProvider member : bus.members) {
            if (member != this) {
                member.deliver(event)
            }
        }
    }

    @Override
    void onRemoteChange(Consumer<TaskChangeEvent> consumer) {
        this.consumer = consumer
    }

    @Override
    boolean isReal() {
        return true
    }

    private void deliver(TaskChangeEvent event) {
        Consumer<TaskChangeEvent> c = this.consumer
        if (c != null) {
            try {
                c.accept(event)
            } catch (Exception e) {
                log.warn('Failed to apply replicated event {}: {}', event, e.message)
            }
        }
    }
}
