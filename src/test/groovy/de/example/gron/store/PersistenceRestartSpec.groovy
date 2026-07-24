package de.example.gron.store

import de.example.gron.api.CatchUpPolicy
import de.example.gron.api.OverlapPolicy
import de.example.gron.api.Task
import de.example.gron.api.TaskStoreException
import de.example.gron.core.JsonSerializer
import de.example.gron.spi.DueRun
import de.example.gron.spi.NodeInfo
import de.example.gron.spi.SkipSink
import de.example.gron.spi.Signaler
import de.example.gron.spi.StoreContext
import de.example.gron.spi.TaskStore
import de.example.gron.store.file.FileTaskStore
import de.example.gron.store.jdbc.JdbcTaskStore
import de.example.gron.support.RecordingHandler
import de.example.gron.util.MutableClock
import org.h2.jdbcx.JdbcDataSource
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import javax.sql.DataSource
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * Restart behaviour for the persistent stores: tasks survive a reopen and the
 * catch-up policy applies on the first claim after reopening. Also verifies that
 * unpersistable params and closure runners are rejected.
 */
class PersistenceRestartSpec extends Specification {

    private static final Instant T0 = Instant.parse('2026-01-01T00:00:00Z')
    private static final AtomicInteger DB_SEQ = new AtomicInteger(0)

    @TempDir
    Path tempDir

    private MutableClock clock = new MutableClock(T0)
    private List<Object[]> skips = []
    private String dbUrl = "jdbc:h2:mem:restart_${DB_SEQ.incrementAndGet()};DB_CLOSE_DELAY=-1"

    private StoreContext ctxAt(Instant now) {
        clock.setInstant(now)
        SkipSink sink = { String id, Instant t, r -> skips.add([id, t, r] as Object[]) } as SkipSink
        Signaler sig = { -> } as Signaler
        return new StoreContext(new JsonSerializer(), clock, getClass().classLoader, sig, sink)
    }

    private DataSource h2() {
        JdbcDataSource ds = new JdbcDataSource()
        ds.setURL(dbUrl); ds.setUser('sa'); ds.setPassword('')
        return ds
    }

    private TaskStore freshFile() { return new FileTaskStore(tempDir) }

    private TaskStore freshJdbc(boolean create) { return new JdbcTaskStore(h2(), create) }

    private Task recurring(String id, CatchUpPolicy cu) {
        return Task.define(id) {
            handler RecordingHandler
            every Duration.ofSeconds(10), T0
            overlap OverlapPolicy.PARALLEL
            catchUp cu
            overdueAfter Duration.ofSeconds(60)
        }
    }

    @Unroll
    def "#kind store: tasks survive a restart"() {
        given: 'first store instance saves a task'
        TaskStore first = open ? openFirst(kind) : null
        first.save(recurring('survivor', CatchUpPolicy.SKIP))
        first.close()

        when: 'a fresh store instance opens the same backing storage'
        TaskStore second = reopen(kind)
        second.open(ctxAt(T0.plusSeconds(5)))

        then:
        second.find('survivor')?.id == 'survivor'

        cleanup:
        second.close()

        where:
        kind   | open
        'file' | true
        'jdbc' | true
    }

    @Unroll
    def "#kind store: catch-up applies on the first claim after reopen"() {
        given: 'save a RUN_ONCE task, then simulate downtime'
        TaskStore first = openFirst(kind)
        first.save(recurring('catchup', CatchUpPolicy.RUN_ONCE))
        first.close()

        when: 'reopen far in the future and claim'
        TaskStore second = reopen(kind)
        second.open(ctxAt(T0.plusSeconds(300)))
        List<DueRun> due = second.claimDue(clock.instant(), 10, new NodeInfo('n', [] as Set, clock.instant()))

        then: 'exactly one catch-up run for the earliest missed occurrence'
        due.size() == 1
        due[0].plannedTime == T0.plusSeconds(10)

        cleanup:
        second.close()

        where:
        kind << ['file', 'jdbc']
    }

    private TaskStore openFirst(String kind) {
        TaskStore s = kind == 'file' ? freshFile() : freshJdbc(true)
        s.open(ctxAt(T0))
        return s
    }

    private TaskStore reopen(String kind) {
        return kind == 'file' ? freshFile() : freshJdbc(false)
    }

    def "persistent stores reject a closure runner on save"() {
        given:
        TaskStore s = freshJdbc(true)
        s.open(ctxAt(T0))
        Task closureTask = Task.define('c') {
            handler { }
            every Duration.ofSeconds(10)
        }

        when:
        s.save(closureTask)

        then:
        thrown(TaskStoreException)

        cleanup:
        s.close()
    }

    def "persistent stores reject unsupported param types on save"() {
        given:
        TaskStore s = freshJdbc(true)
        s.open(ctxAt(T0))
        Task badParams = Task.define('p') {
            handler RecordingHandler
            params custom: new Object()
            every Duration.ofSeconds(10)
        }

        when:
        s.save(badParams)

        then:
        thrown(TaskStoreException)

        cleanup:
        s.close()
    }

    def "persistent stores round-trip supported param types including Instant"() {
        given:
        TaskStore s = freshJdbc(true)
        s.open(ctxAt(T0))
        Instant when = Instant.parse('2026-05-01T08:00:00Z')
        s.save(Task.define('rt') {
            handler RecordingHandler
            params region: 'EU', maxRows: 500, active: true, at: when, list: [1, 2, 3]
            every Duration.ofSeconds(10)
        })

        expect:
        Task restored = s.find('rt')
        restored.params.region == 'EU'
        restored.params.maxRows == 500
        restored.params.active == true
        restored.params.at == when
        restored.params.list == [1, 2, 3]

        cleanup:
        s.close()
    }
}
