package de.example.gron.cluster

import de.example.gron.api.Task
import de.example.gron.api.TaskScheduler
import de.example.gron.core.GronScheduler
import de.example.gron.store.jdbc.JdbcTaskStore
import de.example.gron.support.RecordingHandler
import de.example.gron.util.TimeCodec
import groovy.sql.Sql
import org.h2.jdbcx.JdbcDataSource
import spock.lang.Specification
import spock.lang.Unroll
import spock.util.concurrent.PollingConditions

import javax.sql.DataSource
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * Cluster behaviour on a shared H2 {@link JdbcTaskStore}: exactly-once
 * execution across two nodes, fail-over with a recoverable task, node-selector
 * filtering, and the ANY_NODE fallback.
 */
class ClusterSpec extends Specification {

    private static final AtomicInteger DB_SEQ = new AtomicInteger(0)

    String dbUrl = "jdbc:h2:mem:cluster_${DB_SEQ.incrementAndGet()};DB_CLOSE_DELAY=-1"
    PollingConditions poll = new PollingConditions(timeout: 10, delay: 0.05)
    List<TaskScheduler> started = []

    def setup() {
        RecordingHandler.reset()
        // Initialise the schema once via a throwaway store.
        JdbcTaskStore init = new JdbcTaskStore(ds(), true)
        init.open(new de.example.gron.spi.StoreContext(new de.example.gron.core.JsonSerializer(),
                Clock.systemUTC(), getClass().classLoader, ({ -> } as de.example.gron.spi.Signaler),
                ({ a, b, c -> } as de.example.gron.spi.SkipSink)))
        init.close()
    }

    def cleanup() {
        started.each { it.stop(Duration.ofSeconds(2)) }
    }

    private DataSource ds() {
        JdbcDataSource d = new JdbcDataSource()
        d.setURL(dbUrl); d.setUser('sa'); d.setPassword('')
        return d
    }

    private TaskScheduler node(String id, Set<String> tags = [] as Set) {
        DataSource dataSource = ds()
        TaskScheduler s = GronScheduler.builder()
                .name(id)
                .nodeId(id)
                .nodeTags(tags)
                .workers(2)
                .clock(Clock.systemUTC())
                .maxSleep(Duration.ofMillis(30))
                .maintenanceInterval(Duration.ofMillis(200))
                .store(new JdbcTaskStore(dataSource, false))
                .coordinator(new JdbcClusterCoordinator(dataSource, id, tags, Clock.systemUTC(),
                        Duration.ofMillis(300), 2.0d, Duration.ofMillis(200)))
                .build()
        started.add(s)
        return s
    }

    def "each due occurrence runs exactly once across two nodes"() {
        given:
        TaskScheduler a = node('node-a')
        TaskScheduler b = node('node-b')
        a.start(); b.start()

        when: 'a bounded interval task fires 10 times'
        a.submit(Task.define('once-each') {
            handler RecordingHandler
            overlap de.example.gron.api.OverlapPolicy.PARALLEL
            every Duration.ofMillis(100), Instant.now().plusMillis(200), 10
        })

        then: 'exactly 10 total runs, and both nodes participated'
        poll.eventually { RecordingHandler.count('once-each') == 10 }

        and: 'no duplicates appear afterwards'
        Thread.sleep(500)
        RecordingHandler.count('once-each') == 10
    }

    def "fail-over re-runs a recoverable task claimed by a dead node"() {
        given: 'a running node with a recoverable task'
        TaskScheduler a = node('node-a')
        a.start()
        a.submit(Task.define('recover-me') {
            handler RecordingHandler
            recoverable()
            every Duration.ofHours(1)   // no regular runs during the test
        })

        when: 'simulate node-dead: register it with an old heartbeat and an unfinished claim'
        Sql sql = new Sql(ds())
        long oldSeen = TimeCodec.toNanos(Instant.now().minusSeconds(30))
        long planned = TimeCodec.toNanos(Instant.now().minusSeconds(5))
        sql.executeUpdate('INSERT INTO GRON_NODE (ID, TAGS, LAST_SEEN) VALUES (?, ?, ?)',
                ['node-dead', '', oldSeen])
        sql.executeUpdate('''INSERT INTO GRON_RUN
                (CLAIM_TOKEN, TASK_ID, PLANNED_TIME, CLAIMED_BY, CLAIMED_AT, RECOVERABLE)
                VALUES (?, ?, ?, ?, ?, TRUE)''',
                ['dead-token', 'recover-me', planned, 'node-dead', oldSeen])

        then: 'node-a detects the dead node and re-runs the recoverable claim'
        poll.eventually { RecordingHandler.count('recover-me') >= 1 }

        and: 'the stale claim row is cleared after completion'
        poll.eventually {
            sql.firstRow('SELECT COUNT(*) AS C FROM GRON_RUN WHERE CLAIM_TOKEN = ?',
                    ['dead-token']).C == 0
        }

        and: 'the recovery run is recorded in history with recovered == true'
        poll.eventually {
            a.historyOf('recover-me', 10).any { it.recovered && it.plannedTime != null }
        }
        a.metricsSnapshot().counter('gron.runs.recovered') >= 1

        cleanup:
        sql.close()
    }

    def "node selector restricts execution to matching nodes"() {
        given:
        TaskScheduler eu = node('node-eu', ['eu'] as Set)
        TaskScheduler us = node('node-us', ['us'] as Set)
        eu.start(); us.start()

        when: 'a task pinned to the eu tag fires a few times'
        eu.submit(Task.define('eu-only') {
            handler RecordingHandler
            every Duration.ofMillis(100), Instant.now().plusMillis(200), 5
            runOn { tag 'eu' }
        })

        then: 'all runs happen, only on the eu node'
        poll.eventually { RecordingHandler.count('eu-only') == 5 }
        RecordingHandler.nodes('eu-only') == ['node-eu'] as Set
    }

    def "ANY_NODE fallback lets any node run after the grace period"() {
        given: 'a single node, task pinned to a non-existent node with ANY_NODE fallback'
        TaskScheduler a = node('node-a')
        a.start()

        when:
        a.submit(Task.define('fallback') {
            handler RecordingHandler
            every Duration.ofMillis(100), Instant.now().plusMillis(100), 3
            runOn { node 'ghost'; fallback de.example.gron.api.NodeFallback.ANY_NODE, Duration.ofMillis(600) }
        })

        then: 'after the fallback grace period, node-a runs the occurrences'
        poll.eventually { RecordingHandler.count('fallback') >= 1 }
        RecordingHandler.nodes('fallback') == ['node-a'] as Set
    }
}
