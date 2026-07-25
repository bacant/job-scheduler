package de.example.gron.examples.bench

import de.example.gron.api.CatchUpPolicy
import de.example.gron.api.OverlapPolicy
import de.example.gron.api.RunOutcome
import de.example.gron.api.Task
import de.example.gron.api.TaskContext
import de.example.gron.api.TaskHandler
import de.example.gron.api.TaskScheduler
import de.example.gron.core.GronScheduler
import de.example.gron.core.JsonSerializer
import de.example.gron.spi.DueRun
import de.example.gron.spi.NodeInfo
import de.example.gron.spi.Signaler
import de.example.gron.spi.SkipSink
import de.example.gron.spi.StoreContext
import de.example.gron.store.memory.MemoryTaskStore
import de.example.gron.util.MutableClock

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

/**
 * Hand-rolled throughput benchmark (no JMH — an external dependency). A hand
 * benchmark cannot control JIT warmup, GC or dead-code elimination as rigorously
 * as JMH; treat the numbers as relative indicators, not absolutes. Each scenario
 * warms up before measuring.
 *
 * <p>Scenarios:</p>
 * <ol>
 *   <li><b>submit</b> — task submission throughput.</li>
 *   <li><b>logic</b> — deterministic claim/complete throughput on the
 *       {@link MemoryTaskStore} using a {@link MutableClock} (no real-time
 *       waiting); this is what the relative scaling acceptance criterion uses.</li>
 *   <li><b>wall</b> — a real scheduler with no-op handlers over wall-clock time.</li>
 * </ol>
 *
 * <p>Run: {@code groovy -cp <cp> ThroughputBench [wall]}.</p>
 */
class ThroughputBench {

    private static final Instant T0 = Instant.parse('2026-01-01T00:00:00Z')

    static class NoopHandler implements TaskHandler {
        @Override
        void run(TaskContext context) { }
    }

    private static StoreContext benchContext(Clock clock) {
        return new StoreContext(new JsonSerializer(), clock, ThroughputBench.classLoader,
                ({ -> } as Signaler), ({ a, b, c -> } as SkipSink))
    }

    /** Populates a memory store with n interval tasks (all due each period). */
    private static MemoryTaskStore populate(int n, MutableClock clock) {
        MemoryTaskStore store = new MemoryTaskStore()
        store.open(benchContext(clock))
        for (int i = 0; i < n; i++) {
            store.save(Task.define('t-' + i) {
                handler { }
                every Duration.ofSeconds(1), T0
                overlap OverlapPolicy.PARALLEL
                catchUp CatchUpPolicy.SKIP
            })
        }
        return store
    }

    /**
     * Deterministic claim/complete throughput: advances a mutable clock and
     * claims/completes {@code targetRuns} occurrences over {@code n} stored
     * tasks. Returns runs per second.
     */
    static double measureStoreOpsPerSecond(int n, long targetRuns, int batch) {
        MutableClock clock = new MutableClock(T0)
        MemoryTaskStore store = populate(n, clock)
        NodeInfo node = new NodeInfo('bench', [] as Set, T0)
        Instant now = T0

        // Warmup (not measured).
        now = drive(store, node, clock, now, Math.min(targetRuns, n * 5L))

        long start = System.nanoTime()
        long processed = 0L
        while (processed < targetRuns) {
            now = now.plusSeconds(1)
            clock.setInstant(now)
            while (true) {
                List<DueRun> due = store.claimDue(now, batch, node)
                if (due.isEmpty()) {
                    break
                }
                for (DueRun r : due) {
                    store.complete(r, RunOutcome.OK, null)
                    processed++
                }
            }
        }
        double seconds = (System.nanoTime() - start) / 1_000_000_000.0d
        store.close()
        return processed / seconds
    }

    private static Instant drive(MemoryTaskStore store, NodeInfo node, MutableClock clock,
                                 Instant now, long runs) {
        long done = 0L
        while (done < runs) {
            now = now.plusSeconds(1)
            clock.setInstant(now)
            while (true) {
                List<DueRun> due = store.claimDue(now, 1000, node)
                if (due.isEmpty()) {
                    break
                }
                for (DueRun r : due) {
                    store.complete(r, RunOutcome.OK, null)
                    done++
                }
            }
        }
        return now
    }

    /**
     * Full run-path throughput with no-op handlers through a real scheduler
     * (the setup the relative scaling acceptance criterion targets). Submits
     * {@code n} always-due tasks and measures {@code gron.runs.started} over a
     * wall-clock window. Returns runs per second.
     */
    static double measureSchedulerRunsPerSecond(int n, long windowMillis) {
        TaskScheduler scheduler = GronScheduler.builder()
                .name('bench').workers(4).claimBatch(200)
                .maxSleep(Duration.ofMillis(20))
                .historyPolicy(new de.example.gron.history.HistoryPolicy(
                        mode: de.example.gron.history.HistoryMode.OFF))
                .build()
        scheduler.start()
        try {
            for (int i = 0; i < n; i++) {
                scheduler.submit(Task.define('r-' + i) {
                    handler { }
                    every Duration.ofMillis(1)
                    overlap OverlapPolicy.PARALLEL
                })
            }
            Thread.sleep(500)   // warmup
            long before = scheduler.metricsSnapshot().counter('gron.runs.started',
                    [node: scheduler.nodeId])
            Thread.sleep(windowMillis)
            long after = scheduler.metricsSnapshot().counter('gron.runs.started',
                    [node: scheduler.nodeId])
            return (after - before) / (windowMillis / 1000.0d)
        } finally {
            scheduler.stop(Duration.ofSeconds(3))
        }
    }

    /** Submission throughput: tasks saved per second. */
    static double measureSubmitPerSecond(int n) {
        MutableClock clock = new MutableClock(T0)
        MemoryTaskStore store = new MemoryTaskStore()
        store.open(benchContext(clock))
        long start = System.nanoTime()
        for (int i = 0; i < n; i++) {
            store.save(Task.define('s-' + i) {
                handler { }
                every Duration.ofSeconds(10), T0
            })
        }
        double seconds = (System.nanoTime() - start) / 1_000_000_000.0d
        store.close()
        return n / seconds
    }

    static void main(String[] args) {
        println '== GronScheduler throughput benchmark =='
        println '(hand-rolled; relative indicators only)\n'

        println 'Scenario (a) submit throughput:'
        printf('  %,8d tasks: %,12.0f submits/s%n', 10_000, measureSubmitPerSecond(10_000))
        printf('  %,8d tasks: %,12.0f submits/s%n', 100_000, measureSubmitPerSecond(100_000))

        println '\nScenario (b) deterministic claim/complete throughput:'
        double ops1k = measureStoreOpsPerSecond(1_000, 500_000L, 200)
        double ops100k = measureStoreOpsPerSecond(100_000, 500_000L, 200)
        printf('  %,8d tasks: %,12.0f runs/s%n', 1_000, ops1k)
        printf('  %,8d tasks: %,12.0f runs/s%n', 100_000, ops100k)
        printf('  ratio 100k/1k: %.1f%% (acceptance: >= 70%%)%n', 100.0d * ops100k / ops1k)

        if (args.length > 0 && args[0] == 'wall') {
            println '\nScenario (c) wall-clock, 10k tasks @ every 10s, no-op handlers, 30s:'
            wallClock(10_000, Duration.ofSeconds(30))
        }
    }

    private static void wallClock(int n, Duration duration) {
        AtomicLong runs = new AtomicLong(0)
        TaskScheduler scheduler = GronScheduler.builder()
                .name('bench').workers(8).claimBatch(500)
                .maxSleep(Duration.ofMillis(50))
                .build()
        scheduler.start()
        Instant startAt = Instant.now().plusSeconds(1)
        for (int i = 0; i < n; i++) {
            scheduler.submit(Task.define('w-' + i) {
                handler NoopHandler
                every Duration.ofSeconds(10), startAt
            })
        }
        Thread.sleep(duration.toMillis())
        def snap = scheduler.metricsSnapshot()
        long started = snap.counter('gron.runs.started', [node: scheduler.nodeId])
        printf('  runs started in %ds: %,d (%.0f runs/s)%n',
                duration.seconds, started, started / (double) duration.seconds)
        scheduler.stop(Duration.ofSeconds(5))
    }
}
