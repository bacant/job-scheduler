package de.example.gron.core

import de.example.gron.api.OverlapPolicy
import de.example.gron.api.Task
import de.example.gron.examples.bench.ThroughputBench
import spock.lang.Specification
import spock.lang.Tag
import spock.util.concurrent.PollingConditions

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Throughput guarantees: claim throttling never claims more than the free worker
 * capacity, and run throughput scales sub-linearly with the number of tasks.
 */
class ThroughputSpec extends Specification {

    PollingConditions poll = new PollingConditions(timeout: 5, delay: 0.02)

    def "claim throttling never exceeds the free worker capacity"() {
        given: 'a scheduler with 2 workers and a queue of 2 (max 4 in flight)'
        int workers = 2
        int queue = 2
        int maxInflight = workers + queue
        CountDownLatch gate = new CountDownLatch(1)
        GronScheduler scheduler = GronScheduler.builder()
                .name('t').nodeId('node-a').workers(workers).workerQueue(queue).claimBatch(100)
                .maxSleep(Duration.ofMillis(20))
                .build()
        scheduler.start()

        when: 'many always-due tasks whose handlers block'
        (0..49).each { int i ->
            scheduler.submit(Task.define('b-' + i) {
                handler { gate.await(5, TimeUnit.SECONDS) }
                every Duration.ofMillis(1)
                overlap OverlapPolicy.PARALLEL
            })
        }

        then: 'at most maxInflight runs are ever started (claims are not hoarded)'
        poll.eventually { scheduler.metricsSnapshot().counter('gron.runs.started', [node: 'node-a']) >= 1 }
        // Give the loop time to (not) over-claim.
        Thread.sleep(300)
        long started = scheduler.metricsSnapshot().counter('gron.runs.started', [node: 'node-a'])
        started <= maxInflight

        and: 'a backlog remains (not everything was claimed)'
        scheduler.metricsSnapshot().gauge('gron.runs.overdue').intValue() > 0
        scheduler.metricsSnapshot().gauge('gron.tasks.total').intValue() == 50

        cleanup:
        gate.countDown()
        scheduler.stop(Duration.ofSeconds(2))
    }

    @Tag('slow')
    def "run throughput at 100k tasks is at least 70% of at 1k tasks"() {
        when: 'no-op handlers through a real scheduler (per acceptance criterion)'
        double r1k = ThroughputBench.measureSchedulerRunsPerSecond(1_000, 1500L)
        double r100k = ThroughputBench.measureSchedulerRunsPerSecond(100_000, 1500L)
        double ratio = r100k / r1k
        println "runs/s: 1k=${(long) r1k}, 100k=${(long) r100k}, ratio=${(int) (ratio * 100)}%"

        then: 'sub-linear scaling: no O(n) scan per tick'
        ratio >= 0.70d

        and: 'comfortably above the soft absolute floor of 5000 runs/s'
        r1k >= 5000d
    }
}
