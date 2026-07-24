package de.example.gron.replication

import de.example.gron.api.Task
import de.example.gron.api.TaskScheduler
import de.example.gron.api.UnsupportedPlacementException
import de.example.gron.core.GronScheduler
import de.example.gron.store.memory.MemoryTaskStore
import de.example.gron.support.RecordingHandler
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.time.Clock
import java.time.Duration
import java.time.Instant

/**
 * Replication and {@code EVERY_NODE}: two schedulers coupled by an
 * {@link InMemoryReplicationProvider} stay in sync, and an {@code EVERY_NODE}
 * task runs locally on every node. Without a real replication provider,
 * {@code EVERY_NODE} is rejected.
 */
class ReplicationSpec extends Specification {

    PollingConditions poll = new PollingConditions(timeout: 8, delay: 0.05)
    List<TaskScheduler> started = []

    def setup() {
        RecordingHandler.reset()
    }

    def cleanup() {
        started.each { it.stop(Duration.ofSeconds(2)) }
    }

    private TaskScheduler node(String id, InMemoryReplicationProvider.Bus bus) {
        TaskScheduler s = GronScheduler.builder()
                .name(id).nodeId(id)
                .store(new MemoryTaskStore())
                .replication(new InMemoryReplicationProvider(bus))
                .clock(Clock.systemUTC())
                .maxSleep(Duration.ofMillis(30))
                .build()
        started.add(s)
        return s
    }

    def "a submitted task is replicated to the other node"() {
        given:
        InMemoryReplicationProvider.Bus bus = new InMemoryReplicationProvider.Bus()
        TaskScheduler a = node('node-a', bus)
        TaskScheduler b = node('node-b', bus)
        a.start(); b.start()

        when:
        a.submit(Task.define('shared') {
            handler RecordingHandler
            every Duration.ofHours(1)
        })

        then: 'node-b learns about the task via replication'
        poll.eventually { b.find('shared')?.id == 'shared' }
    }

    def "an EVERY_NODE task runs on every node"() {
        given:
        InMemoryReplicationProvider.Bus bus = new InMemoryReplicationProvider.Bus()
        TaskScheduler a = node('node-a', bus)
        TaskScheduler b = node('node-b', bus)
        a.start(); b.start()

        when:
        a.submit(Task.define('broadcast') {
            handler RecordingHandler
            every Duration.ofMillis(100), Instant.now().plusMillis(300), 3
            runOn { everyNode() }
        })

        then: 'each of the 3 occurrences runs once on each of the 2 nodes'
        poll.eventually { RecordingHandler.count('broadcast') == 6 }
        RecordingHandler.nodes('broadcast') == ['node-a', 'node-b'] as Set
    }

    def "EVERY_NODE without a real replication provider is rejected"() {
        given: 'a scheduler with the default no-op replication'
        TaskScheduler solo = GronScheduler.builder()
                .name('solo').nodeId('solo')
                .store(new MemoryTaskStore())
                .build()
        started.add(solo)
        solo.start()

        when:
        solo.submit(Task.define('broadcast') {
            handler RecordingHandler
            every Duration.ofSeconds(1)
            runOn { everyNode() }
        })

        then:
        thrown(UnsupportedPlacementException)
    }
}
