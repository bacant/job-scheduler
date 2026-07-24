package de.example.gron.examples

import de.example.gron.api.Task
import de.example.gron.api.TaskContext
import de.example.gron.api.TaskHandler
import de.example.gron.api.TaskScheduler
import de.example.gron.cluster.JdbcClusterCoordinator
import de.example.gron.core.GronScheduler
import de.example.gron.store.jdbc.JdbcTaskStore
import org.h2.jdbcx.JdbcDataSource

import javax.sql.DataSource
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * Demonstrates cluster operation: two schedulers share one JDBC store, so each
 * due occurrence runs exactly once across the cluster.
 */
class ClusterDemo {

    static final AtomicInteger TOTAL = new AtomicInteger(0)

    static class CountingHandler implements TaskHandler {
        @Override
        void run(TaskContext context) {
            println "run #${TOTAL.incrementAndGet()} on ${context.nodeId} " +
                    "(planned=${context.plannedTime})"
        }
    }

    static DataSource sharedH2() {
        JdbcDataSource ds = new JdbcDataSource()
        ds.setURL('jdbc:h2:mem:cluster-demo;DB_CLOSE_DELAY=-1')
        ds.setUser('sa'); ds.setPassword('')
        return ds
    }

    static TaskScheduler node(String id, DataSource ds) {
        return GronScheduler.builder()
                .name(id).nodeId(id)
                .store(new JdbcTaskStore(ds, true))
                .coordinator(new JdbcClusterCoordinator(ds, id, [] as Set, Clock.systemUTC(),
                        Duration.ofSeconds(1), 2.0d, Duration.ofMillis(500)))
                .maxSleep(Duration.ofMillis(50))
                .build()
    }

    static void main(String[] args) {
        DataSource ds = sharedH2()
        TaskScheduler a = node('node-a', ds)
        TaskScheduler b = node('node-b', ds)
        a.start(); b.start()

        a.submit(Task.define('cluster-task') {
            handler CountingHandler
            every Duration.ofMillis(200), Instant.now().plusMillis(300), 10
        })

        Thread.sleep(4000)
        println "Total runs across the cluster: ${TOTAL.get()} (expected 10)"

        a.stop(Duration.ofSeconds(2))
        b.stop(Duration.ofSeconds(2))
    }
}
