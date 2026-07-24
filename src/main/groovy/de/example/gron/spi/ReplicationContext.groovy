package de.example.gron.spi

import groovy.transform.CompileStatic

import java.time.Clock

/**
 * Environment handed to a {@link ReplicationProvider} on
 * {@link ReplicationProvider#start}.
 */
@CompileStatic
class ReplicationContext {

    /** Serializer used to encode/decode event payloads. */
    final Serializer serializer

    /** Injected time source. */
    final Clock clock

    /** Local node id used as the origin of published events. */
    final String nodeId

    ReplicationContext(Serializer serializer, Clock clock, String nodeId) {
        this.serializer = serializer
        this.clock = clock
        this.nodeId = nodeId
    }
}
