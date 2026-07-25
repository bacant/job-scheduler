package de.example.gron.history

import de.example.gron.api.RunOutcome
import de.example.gron.metrics.SimpleMetrics
import de.example.gron.spi.StoreContext
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Verifies the asynchronous history pipeline: the run path never blocks on a
 * slow store, overflow policies behave as specified, and {@code stop} drains.
 */
class HistoryPipelineSpec extends Specification {

    private static final Instant T0 = Instant.parse('2026-01-01T00:00:00Z')

    PollingConditions poll = new PollingConditions(timeout: 5, delay: 0.02)

    /** A history store whose record() blocks on a latch (simulates a slow sink). */
    static class BlockingHistoryStore implements HistoryStore {
        final CountDownLatch gate
        final AtomicInteger written = new AtomicInteger(0)

        BlockingHistoryStore(CountDownLatch gate) { this.gate = gate }

        void open(StoreContext c) { }
        void close() { }
        void record(RunRecord r) {
            gate.await(10, TimeUnit.SECONDS)
            written.incrementAndGet()
        }
        List<RunRecord> query(HistoryQuery q) { return [] }
        long deleteOlderThan(Instant cutoff) { return 0 }
    }

    private RunRecord rec(int i) {
        return new RunRecord([runId: 'r' + i, taskId: 't', tags: ([] as Set), plannedTime: T0,
                              startedAt: T0, finishedAt: T0, durationMillis: 0, nodeId: 'n',
                              attempt: 1, outcome: RunOutcome.OK])
    }

    private HistoryPolicy policy(HistoryOverflow overflow, int buffer) {
        return new HistoryPolicy(overflow: overflow, buffer: buffer, batchSize: 1,
                flushInterval: Duration.ofMillis(5))
    }

    def "DROP_OLDEST never blocks the caller and counts drops"() {
        given:
        CountDownLatch gate = new CountDownLatch(1)
        BlockingHistoryStore store = new BlockingHistoryStore(gate)
        SimpleMetrics metrics = new SimpleMetrics()
        AsyncHistoryWriter writer = new AsyncHistoryWriter(store, policy(HistoryOverflow.DROP_OLDEST, 2), metrics)
        writer.start()

        when: 'the store is stuck, but we submit many records'
        long start = System.nanoTime()
        (0..199).each { int i -> writer.submit(rec(i)) }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L

        then: 'submit did not block, and drops were counted'
        elapsedMs < 1000
        poll.eventually { metrics.snapshot().counter('gron.history.dropped') > 0 }

        cleanup:
        gate.countDown()
        writer.stop(1000)
    }

    def "DROP_NEW never blocks the caller and counts drops"() {
        given:
        CountDownLatch gate = new CountDownLatch(1)
        BlockingHistoryStore store = new BlockingHistoryStore(gate)
        SimpleMetrics metrics = new SimpleMetrics()
        AsyncHistoryWriter writer = new AsyncHistoryWriter(store, policy(HistoryOverflow.DROP_NEW, 2), metrics)
        writer.start()

        when:
        long start = System.nanoTime()
        (0..199).each { int i -> writer.submit(rec(i)) }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L

        then:
        elapsedMs < 1000
        poll.eventually { metrics.snapshot().counter('gron.history.dropped') > 0 }

        cleanup:
        gate.countDown()
        writer.stop(1000)
    }

    def "BLOCK policy blocks the caller until room is available"() {
        given:
        CountDownLatch gate = new CountDownLatch(1)
        BlockingHistoryStore store = new BlockingHistoryStore(gate)
        SimpleMetrics metrics = new SimpleMetrics()
        AsyncHistoryWriter writer = new AsyncHistoryWriter(store, policy(HistoryOverflow.BLOCK, 2), metrics)
        writer.start()
        AtomicInteger submitted = new AtomicInteger(0)

        when: 'a producer submits more than fits while the store is stuck'
        Thread producer = new Thread({
            (0..49).each { int i -> writer.submit(rec(i)); submitted.incrementAndGet() }
        })
        producer.start()
        Thread.sleep(200)

        then: 'the producer is blocked (has not submitted all 50)'
        submitted.get() < 50
        producer.isAlive()

        when: 'the store unblocks'
        gate.countDown()
        producer.join(5000)

        then: 'the producer completes and no records were dropped'
        submitted.get() == 50
        metrics.snapshot().counter('gron.history.dropped') == 0

        cleanup:
        writer.stop(2000)
    }

    def "stop drains the queue into the store"() {
        given:
        MemoryHistoryStore store = new MemoryHistoryStore(1000)
        SimpleMetrics metrics = new SimpleMetrics()
        AsyncHistoryWriter writer = new AsyncHistoryWriter(store,
                new HistoryPolicy(buffer: 1000, batchSize: 50, flushInterval: Duration.ofMillis(20)),
                metrics)
        writer.start()

        when:
        (0..49).each { int i -> writer.submit(rec(i)) }
        writer.stop(3000)

        then:
        store.query(new HistoryQuery(limit: 1000)).size() == 50
    }
}
