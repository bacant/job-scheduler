package de.example.gron.store

import de.example.gron.api.CatchUpPolicy
import de.example.gron.api.OverlapPolicy
import de.example.gron.api.RunOutcome
import de.example.gron.api.SkipReason
import de.example.gron.api.Task
import de.example.gron.api.TaskState
import de.example.gron.core.JsonSerializer
import de.example.gron.spi.DueRun
import de.example.gron.spi.NodeInfo
import de.example.gron.spi.SkipSink
import de.example.gron.spi.Signaler
import de.example.gron.spi.StoreContext
import de.example.gron.spi.TaskStore
import de.example.gron.support.RecordingHandler
import de.example.gron.util.MutableClock
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

/**
 * Abstract contract every {@link TaskStore} implementation must satisfy
 * unchanged. Concrete subclasses only provide {@link #createStore}.
 */
abstract class TaskStoreContract extends Specification {

    protected static final Instant T0 = Instant.parse('2026-01-01T00:00:00Z')

    protected MutableClock clock = new MutableClock(T0)
    protected List<Object[]> skips = []
    protected TaskStore store
    protected NodeInfo node = new NodeInfo('node-a', [] as Set, T0)

    /** @return a fresh, unopened store instance. */
    protected abstract TaskStore createStore()

    /** Optional teardown hook for subclasses. */
    protected void destroyStore(TaskStore s) { }

    def setup() {
        store = createStore()
        SkipSink sink = { String id, Instant t, SkipReason r -> skips.add([id, t, r] as Object[]) } as SkipSink
        Signaler sig = { -> } as Signaler
        store.open(new StoreContext(new JsonSerializer(), clock, getClass().classLoader, sig, sink))
    }

    def cleanup() {
        try { store?.close() } finally { destroyStore(store) }
    }

    protected Task every(String id, long seconds, OverlapPolicy ov = OverlapPolicy.SKIP,
                         CatchUpPolicy cu = CatchUpPolicy.SKIP, Set<String> labels = [] as Set) {
        String[] labelArray = labels.toArray(new String[0])
        return Task.define(id) {
            handler RecordingHandler
            tags labelArray
            every Duration.ofSeconds(seconds), T0
            overlap ov
            catchUp cu
            overdueAfter Duration.ofSeconds(60)
        }
    }

    protected List<DueRun> claim(int limit = 10) {
        return store.claimDue(clock.instant(), limit, node)
    }

    def "save, find, findAll and findByTag"() {
        when:
        store.save(every('a', 10, OverlapPolicy.SKIP, CatchUpPolicy.SKIP, ['x'] as Set))
        store.save(every('b', 10, OverlapPolicy.SKIP, CatchUpPolicy.SKIP, ['y'] as Set))

        then:
        store.find('a').id == 'a'
        store.find('missing') == null
        store.findAll().collect { it.id }.toSet() == ['a', 'b'].toSet()
        store.findByTag('x').collect { it.id } == ['a']
        store.findByTag('none').isEmpty()
    }

    def "replace by id via save"() {
        given:
        store.save(every('a', 10))

        when:
        store.save(every('a', 20))

        then:
        store.findAll().size() == 1
    }

    def "delete removes a task"() {
        given:
        store.save(every('a', 10))

        expect:
        store.delete('a')
        store.find('a') == null
        !store.delete('a')
    }

    def "claim, complete and reschedule"() {
        given:
        store.save(every('a', 10))

        expect:
        claim().isEmpty()
        store.stateOf('a') == TaskState.SCHEDULED

        when:
        clock.setInstant(T0.plusSeconds(10))
        List<DueRun> due = claim()

        then:
        due.size() == 1
        due[0].plannedTime == T0.plusSeconds(10)
        store.stateOf('a') == TaskState.RUNNING

        when:
        store.complete(due[0], RunOutcome.OK, null)
        clock.setInstant(T0.plusSeconds(20))

        then:
        store.stateOf('a') == TaskState.SCHEDULED
        claim()[0].plannedTime == T0.plusSeconds(20)
    }

    def "release makes an unexecuted claim due again"() {
        given:
        store.save(every('a', 10))
        clock.setInstant(T0.plusSeconds(10))
        DueRun due = claim()[0]

        when:
        store.release(due)

        then:
        claim()[0].plannedTime == T0.plusSeconds(10)
    }

    def "overlap SKIP skips while active and reports it"() {
        given:
        store.save(every('a', 10, OverlapPolicy.SKIP))
        clock.setInstant(T0.plusSeconds(10))
        claim()

        when:
        clock.setInstant(T0.plusSeconds(20))
        List<DueRun> due = claim()

        then:
        due.isEmpty()
        skips.any { it[2] == SkipReason.OVERLAP }
    }

    def "overlap WAIT defers and merges to one run"() {
        given:
        store.save(every('a', 10, OverlapPolicy.WAIT))
        clock.setInstant(T0.plusSeconds(10))
        DueRun active = claim()[0]
        clock.setInstant(T0.plusSeconds(20)); claim()
        clock.setInstant(T0.plusSeconds(30)); claim()

        when:
        store.complete(active, RunOutcome.OK, null)
        List<DueRun> due = claim()

        then:
        due.size() == 1
        due[0].plannedTime == T0.plusSeconds(30)
    }

    def "catch-up SKIP discards overdue"() {
        given:
        store.save(every('a', 10, OverlapPolicy.SKIP, CatchUpPolicy.SKIP))

        when:
        clock.setInstant(T0.plusSeconds(300))
        List<DueRun> due = claim()

        then:
        due.isEmpty()
        skips.any { it[2] == SkipReason.OVERDUE }
    }

    def "catch-up RUN_ONCE runs a single catch-up"() {
        given:
        store.save(every('a', 10, OverlapPolicy.PARALLEL, CatchUpPolicy.RUN_ONCE))

        when:
        clock.setInstant(T0.plusSeconds(300))
        List<DueRun> due = claim()

        then:
        due.size() == 1
        due[0].plannedTime == T0.plusSeconds(10)
    }

    def "catch-up RUN_ALL catches up every missed occurrence"() {
        given:
        store.save(every('a', 10, OverlapPolicy.PARALLEL, CatchUpPolicy.RUN_ALL))

        when:
        clock.setInstant(T0.plusSeconds(55))
        List<DueRun> due = claim()

        then:
        due.collect { it.plannedTime } == [
                T0.plusSeconds(10), T0.plusSeconds(20), T0.plusSeconds(30),
                T0.plusSeconds(40), T0.plusSeconds(50)
        ]
    }

    def "pause and resume gate claiming"() {
        given:
        store.save(every('a', 10))
        store.setPaused('a', true)
        clock.setInstant(T0.plusSeconds(10))

        expect:
        store.stateOf('a') == TaskState.PAUSED
        claim().isEmpty()

        when:
        store.setPaused('a', false)

        then:
        claim()[0].plannedTime == T0.plusSeconds(10)
    }

    def "setFailed then resume clears the failed state"() {
        given:
        store.save(every('a', 10))
        store.setFailed('a')
        clock.setInstant(T0.plusSeconds(10))

        expect:
        store.stateOf('a') == TaskState.FAILED
        claim().isEmpty()

        when:
        store.setPaused('a', false)

        then:
        store.stateOf('a') == TaskState.SCHEDULED
        claim()[0].plannedTime == T0.plusSeconds(10)
    }

    def "one-time schedule ends in DONE"() {
        given:
        store.save(Task.define('once') {
            handler RecordingHandler
            once T0.plusSeconds(5)
        })
        clock.setInstant(T0.plusSeconds(5))
        DueRun due = claim()[0]
        store.complete(due, RunOutcome.OK, null)

        expect:
        store.stateOf('once') == TaskState.DONE
        claim().isEmpty()
    }

    def "capabilities reported correctly"() {
        expect:
        store.isPersistent() == expectedPersistent()
        store.isShared() == expectedShared()
    }

    protected abstract boolean expectedPersistent()

    protected abstract boolean expectedShared()
}
