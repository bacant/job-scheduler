package de.example.gron.examples

import de.example.gron.api.Task
import de.example.gron.api.TaskContext
import de.example.gron.api.TaskHandler
import de.example.gron.api.TaskScheduler
import de.example.gron.core.GronScheduler
import de.example.gron.replication.InMemoryReplicationProvider
import de.example.gron.store.memory.MemoryTaskStore

import java.time.Duration
import java.time.Instant

/**
 * Demonstrates {@code EVERY_NODE} placement: two schedulers coupled by an
 * {@link InMemoryReplicationProvider} each run the same task locally on every
 * occurrence.
 */
class EveryNodeDemo {

    static class LocalHandler implements TaskHandler {
        @Override
        void run(TaskContext context) {
            println "running on ${context.nodeId} (planned=${context.plannedTime})"
        }
    }

    static TaskScheduler node(String id, InMemoryReplicationProvider.Bus bus) {
        return GronScheduler.builder()
                .name(id).nodeId(id)
                .store(new MemoryTaskStore())
                .replication(new InMemoryReplicationProvider(bus))
                .maxSleep(Duration.ofMillis(50))
                .build()
    }

    static void main(String[] args) {
        InMemoryReplicationProvider.Bus bus = new InMemoryReplicationProvider.Bus()
        TaskScheduler a = node('node-a', bus)
        TaskScheduler b = node('node-b', bus)
        a.start(); b.start()

        // Submitted on node-a, replicated to node-b, run on both.
        a.submit(Task.define('broadcast') {
            handler LocalHandler
            every Duration.ofMillis(500), Instant.now().plusMillis(500), 4
            runOn { everyNode() }
        })

        Thread.sleep(4000)
        a.stop(Duration.ofSeconds(2))
        b.stop(Duration.ofSeconds(2))
    }
}
