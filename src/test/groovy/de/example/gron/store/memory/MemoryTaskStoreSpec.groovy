package de.example.gron.store.memory

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
import de.example.gron.util.MutableClock
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

/**
 * Deterministic verification of the reference {@link MemoryTaskStore}: claim /
 * complete, all overlap and catch-up policies, states, and WAIT merging.
 */
class MemoryTaskStoreSpec extends Specification {

    private static final Instant T0 = Instant.parse('2026-01-01T00:00:00Z')

    MutableClock clock = new MutableClock(T0)
    MemoryTaskStore store = new MemoryTaskStore()
    List<Object[]> skips = []
    NodeInfo node = new NodeInfo('node-a', [] as Set, T0)

    def setup() {
        SkipSink sink = { String id, Instant t, SkipReason r -> skips.add([id, t, r] as Object[]) } as SkipSink
        Signaler sig = { -> } as Signaler
        store.open(new StoreContext(new JsonSerializer(), clock, getClass().classLoader, sig, sink))
    }

    private Task every(String id, long seconds, OverlapPolicy ov = OverlapPolicy.SKIP,
                       CatchUpPolicy cu = CatchUpPolicy.SKIP) {
        return Task.define(id) {
            handler { }
            every Duration.ofSeconds(seconds), T0
            overlap ov
            catchUp cu
            overdueAfter Duration.ofSeconds(60)
        }
    }

    private List<DueRun> claim(int limit = 10) {
        return store.claimDue(clock.instant(), limit, node)
    }

    def "claims a due occurrence and reschedules on complete"() {
        given:
        store.save(every('t', 10))

        expect: 'nothing due yet at T0 (first run is T0+10)'
        claim().isEmpty()
        store.stateOf('t') == TaskState.SCHEDULED

        when:
        clock.setInstant(T0.plusSeconds(10))
        List<DueRun> due = claim()

        then:
        due.size() == 1
        due[0].plannedTime == T0.plusSeconds(10)
        store.stateOf('t') == TaskState.RUNNING

        when:
        store.complete(due[0], RunOutcome.OK, null)

        then:
        store.stateOf('t') == TaskState.SCHEDULED

        when: 'next tick'
        clock.setInstant(T0.plusSeconds(20))

        then:
        claim()[0].plannedTime == T0.plusSeconds(20)
    }

    def "OverlapPolicy SKIP skips occurrences while a run is active"() {
        given:
        store.save(every('t', 10, OverlapPolicy.SKIP))
        clock.setInstant(T0.plusSeconds(10))
        DueRun active = claim()[0]

        when: 'another occurrence becomes due while the first is still running'
        clock.setInstant(T0.plusSeconds(20))
        List<DueRun> due = claim()

        then: 'it is skipped and reported'
        due.isEmpty()
        skips.any { it[2] == SkipReason.OVERLAP && it[1] == T0.plusSeconds(20) }
    }

    def "OverlapPolicy PARALLEL allows concurrent runs"() {
        given:
        store.save(every('t', 10, OverlapPolicy.PARALLEL))
        clock.setInstant(T0.plusSeconds(10))
        claim()   // first run active, not completed

        when:
        clock.setInstant(T0.plusSeconds(20))
        List<DueRun> due = claim()

        then:
        due.size() == 1
        due[0].plannedTime == T0.plusSeconds(20)
    }

    def "OverlapPolicy WAIT defers and merges to a single pending run"() {
        given:
        store.save(every('t', 10, OverlapPolicy.WAIT))
        clock.setInstant(T0.plusSeconds(10))
        DueRun active = claim()[0]

        when: 'two occurrences pass while the run is active -> merge into one'
        clock.setInstant(T0.plusSeconds(20))
        claim()
        clock.setInstant(T0.plusSeconds(30))
        claim()

        then: 'nothing runs yet while active'
        store.stateOf('t') == TaskState.RUNNING

        when: 'the active run finishes'
        store.complete(active, RunOutcome.OK, null)
        List<DueRun> due = claim()

        then: 'exactly one deferred run starts, carrying the most recent planned time'
        due.size() == 1
        due[0].plannedTime == T0.plusSeconds(30)
    }

    def "CatchUpPolicy SKIP discards overdue occurrences"() {
        given:
        store.save(every('t', 10, OverlapPolicy.SKIP, CatchUpPolicy.SKIP))

        when: 'long outage: now is far past the first planned run'
        clock.setInstant(T0.plusSeconds(300))
        List<DueRun> due = claim()

        then: 'nothing runs; an overdue skip is reported; next run is in the future'
        due.isEmpty()
        skips.any { it[2] == SkipReason.OVERDUE }
    }

    def "CatchUpPolicy RUN_ONCE runs a single catch-up then continues"() {
        given:
        store.save(every('t', 10, OverlapPolicy.PARALLEL, CatchUpPolicy.RUN_ONCE))

        when:
        clock.setInstant(T0.plusSeconds(300))
        List<DueRun> due = claim()

        then: 'exactly one catch-up run for the earliest missed occurrence'
        due.size() == 1
        due[0].plannedTime == T0.plusSeconds(10)

        when: 'after completing it, the next run is a future regular one'
        store.complete(due[0], RunOutcome.OK, null)
        List<DueRun> next = claim()

        then:
        next.isEmpty()   // next regular run is after "now"
    }

    def "CatchUpPolicy RUN_ALL catches up every missed occurrence"() {
        given:
        store.save(every('t', 10, OverlapPolicy.PARALLEL, CatchUpPolicy.RUN_ALL))

        when: 'five occurrences were missed (T0+10..T0+50), now is T0+55'
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
        store.save(every('t', 10))
        store.setPaused('t', true)

        when:
        clock.setInstant(T0.plusSeconds(10))

        then:
        store.stateOf('t') == TaskState.PAUSED
        claim().isEmpty()

        when:
        store.setPaused('t', false)

        then:
        claim()[0].plannedTime == T0.plusSeconds(10)
    }

    def "setFailed moves the task to FAILED and stops claiming"() {
        given:
        store.save(every('t', 10))
        store.setFailed('t')
        clock.setInstant(T0.plusSeconds(10))

        expect:
        store.stateOf('t') == TaskState.FAILED
        claim().isEmpty()

        when: 'resume clears the failed state'
        store.setPaused('t', false)

        then:
        store.stateOf('t') == TaskState.SCHEDULED
        claim()[0].plannedTime == T0.plusSeconds(10)
    }

    def "a one-time schedule ends in DONE"() {
        given:
        Task t = Task.define('once') {
            handler { }
            once T0.plusSeconds(5)
        }
        store.save(t)
        clock.setInstant(T0.plusSeconds(5))
        DueRun due = claim()[0]
        store.complete(due, RunOutcome.OK, null)

        expect:
        store.stateOf('once') == TaskState.DONE
        claim().isEmpty()
    }

    def "release makes an unexecuted claim due again"() {
        given:
        store.save(every('t', 10))
        clock.setInstant(T0.plusSeconds(10))
        DueRun due = claim()[0]

        when:
        store.release(due)

        then:
        claim()[0].plannedTime == T0.plusSeconds(10)
    }

    def "runNow injects an immediate ad-hoc occurrence"() {
        given:
        store.save(every('t', 3600))   // next regular run far away
        clock.setInstant(T0.plusSeconds(5))

        when:
        store.triggerNow('t')
        List<DueRun> due = claim()

        then:
        due.size() == 1
        due[0].plannedTime == T0.plusSeconds(5)
    }
}
