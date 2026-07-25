package de.example.gron.history

import de.example.gron.api.RunOutcome
import de.example.gron.api.SkipReason
import de.example.gron.core.JsonSerializer
import de.example.gron.spi.Signaler
import de.example.gron.spi.SkipSink
import de.example.gron.spi.StoreContext
import de.example.gron.util.MutableClock
import spock.lang.Specification

import java.time.Instant

/**
 * Abstract contract every {@link HistoryStore} must satisfy unchanged.
 */
abstract class HistoryStoreContract extends Specification {

    protected static final Instant DAY1 = Instant.parse('2026-01-01T12:00:00Z')
    protected static final Instant DAY2 = Instant.parse('2026-01-02T12:00:00Z')

    protected HistoryStore store

    protected abstract HistoryStore createStore()

    protected void destroyStore(HistoryStore s) { }

    def setup() {
        store = createStore()
        MutableClock clock = new MutableClock(DAY1)
        Signaler sig = { -> } as Signaler
        SkipSink sink = { a, b, c -> } as SkipSink
        store.open(new StoreContext(new JsonSerializer(), clock, getClass().classLoader, sig, sink))
    }

    def cleanup() {
        try { store?.close() } finally { destroyStore(store) }
    }

    protected RunRecord ok(String runId, String taskId, Instant t,
                           Set<String> tags = [] as Set, String node = 'n1') {
        return new RunRecord([runId: runId, taskId: taskId, tags: tags, plannedTime: t,
                              startedAt: t, finishedAt: t.plusMillis(5), durationMillis: 5,
                              nodeId: node, attempt: 1, outcome: RunOutcome.OK])
    }

    protected RunRecord failed(String runId, String taskId, Instant t, int attempt = 1) {
        return new RunRecord([runId: runId, taskId: taskId, tags: ([] as Set), plannedTime: t,
                              startedAt: t, finishedAt: t.plusMillis(5), durationMillis: 5,
                              nodeId: 'n1', attempt: attempt, outcome: RunOutcome.FAILED,
                              errorType: 'java.lang.RuntimeException', errorMessage: 'boom'])
    }

    protected RunRecord skipped(String runId, String taskId, Instant t) {
        return new RunRecord([runId: runId, taskId: taskId, tags: ([] as Set), plannedTime: t,
                              nodeId: 'n1', attempt: 1, outcome: RunOutcome.SKIPPED,
                              skipReason: SkipReason.OVERLAP])
    }

    def "records and returns newest first"() {
        given:
        store.record(ok('r1', 'a', DAY1))
        store.record(ok('r2', 'a', DAY1.plusSeconds(60)))
        store.record(ok('r3', 'a', DAY1.plusSeconds(120)))

        when:
        List<RunRecord> all = store.query(new HistoryQuery(taskId: 'a', limit: 10))

        then:
        all.collect { it.runId } == ['r3', 'r2', 'r1']
    }

    def "filters by task, tag, outcome and node"() {
        given:
        store.record(ok('r1', 'a', DAY1, ['x'] as Set, 'n1'))
        store.record(ok('r2', 'b', DAY1.plusSeconds(10), ['y'] as Set, 'n2'))
        store.record(failed('r3', 'a', DAY1.plusSeconds(20)))
        store.record(skipped('r4', 'a', DAY1.plusSeconds(30)))

        expect:
        store.query(new HistoryQuery(taskId: 'a', limit: 10)).collect { it.runId }.toSet() ==
                ['r1', 'r3', 'r4'] as Set
        store.query(new HistoryQuery(tag: 'y', limit: 10)).collect { it.runId } == ['r2']
        store.query(new HistoryQuery(outcomes: [RunOutcome.FAILED] as Set, limit: 10))
                .collect { it.runId } == ['r3']
        store.query(new HistoryQuery(outcomes: [RunOutcome.SKIPPED] as Set, limit: 10))
                .collect { it.runId } == ['r4']
        store.query(new HistoryQuery(nodeId: 'n2', limit: 10)).collect { it.runId } == ['r2']
    }

    def "supports keyset pagination via startedBefore"() {
        given:
        (0..4).each { int i -> store.record(ok('r' + i, 'a', DAY1.plusSeconds(i * 60))) }

        when: 'first page of 2 (newest first: r4, r3)'
        List<RunRecord> page1 = store.query(new HistoryQuery(taskId: 'a', limit: 2))

        then:
        page1.collect { it.runId } == ['r4', 'r3']

        when: 'next page using the last record time as cursor'
        Instant cursor = page1.last().startedAt
        List<RunRecord> page2 = store.query(new HistoryQuery(taskId: 'a', limit: 2, startedBefore: cursor))

        then:
        page2.collect { it.runId } == ['r2', 'r1']
    }

    def "deleteOlderThan removes only records before the cutoff"() {
        given:
        store.record(ok('old1', 'a', DAY1))
        store.record(ok('old2', 'a', DAY1.plusSeconds(60)))
        store.record(ok('new1', 'a', DAY2))

        when:
        long deleted = store.deleteOlderThan(Instant.parse('2026-01-02T00:00:00Z'))

        then:
        deleted == 2
        store.query(new HistoryQuery(taskId: 'a', limit: 10)).collect { it.runId } == ['new1']
    }

    def "preserves all record fields including error and skip details"() {
        given:
        store.record(failed('f1', 'a', DAY1, 2))
        store.record(skipped('s1', 'a', DAY1.plusSeconds(10)))

        when:
        List<RunRecord> recs = store.query(new HistoryQuery(taskId: 'a', limit: 10))

        then:
        RunRecord failed = recs.find { it.runId == 'f1' }
        failed.outcome == RunOutcome.FAILED
        failed.attempt == 2
        failed.errorType == 'java.lang.RuntimeException'
        failed.errorMessage == 'boom'

        and:
        RunRecord skip = recs.find { it.runId == 's1' }
        skip.outcome == RunOutcome.SKIPPED
        skip.skipReason == SkipReason.OVERLAP
        skip.startedAt == null
    }
}
