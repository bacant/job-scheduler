package de.example.gron.spi

import java.util.function.Consumer

/**
 * SPI for distributing task changes between nodes. A network transport is
 * deliberately out of scope for the library; users provide one through this
 * interface. The bundled {@code InMemoryReplicationProvider} couples several
 * schedulers within one JVM for testing and for the {@code EVERY_NODE} demo.
 */
interface ReplicationProvider {

    /** Starts the provider with its runtime environment. */
    void start(ReplicationContext context)

    /** Stops the provider. */
    void stop()

    /** Publishes a local change to the other nodes. */
    void publish(TaskChangeEvent event)

    /**
     * Registers the consumer that applies incoming remote changes. Applying an
     * event must be idempotent.
     */
    void onRemoteChange(Consumer<TaskChangeEvent> consumer)

    /**
     * @return true if this is a real replication provider (not the no-op).
     *         {@code EVERY_NODE} placement requires this to be true.
     */
    boolean isReal()
}
