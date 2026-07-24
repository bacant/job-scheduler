package de.example.gron.replication

import de.example.gron.spi.ReplicationContext
import de.example.gron.spi.ReplicationProvider
import de.example.gron.spi.TaskChangeEvent
import groovy.transform.CompileStatic

import java.util.function.Consumer

/**
 * Default replication provider that does nothing. It never publishes and never
 * delivers remote changes. {@code EVERY_NODE} placement is rejected when this
 * provider is active (see {@link #isReal}).
 */
@CompileStatic
class NoOpReplicationProvider implements ReplicationProvider {

    @Override
    void start(ReplicationContext context) { }

    @Override
    void stop() { }

    @Override
    void publish(TaskChangeEvent event) { }

    @Override
    void onRemoteChange(Consumer<TaskChangeEvent> consumer) { }

    @Override
    boolean isReal() {
        return false
    }
}
