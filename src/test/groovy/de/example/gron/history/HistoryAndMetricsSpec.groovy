package de.example.gron.history

import de.example.gron.api.RunOutcome
import de.example.gron.api.SkipReason
import de.example.gron.api.Task
import de.example.gron.api.TaskContext
import de.example.gron.core.GronScheduler
import de.example.gron.metrics.MetricsSnapshot
import de.example.gron.util.MutableClock
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Verifies that every run, retry attempt, skip and recovery lands in history
 * with correct fields, and that the metric counters/timers match a defined
 * scenario.
 */
class HistoryAndMetricsSpec extends Specification {

    private static final Instant T0 = Instant.parse('2026-01-01T00:00:00Z')

    MutableClock clock = new MutableClock(T0)
    GronScheduler scheduler
    PollingConditions poll = new PollingConditions(timeout: 5, delay: 0.02)

    def setup() {
        scheduler = GronScheduler.builder()
                .name('h').nodeId('node-a').workers(3)
                .clock(clock).maxSleep(Duration.ofMillis(20)).maintenanceInterval(Duration.ofMillis(50))
                .build()
        scheduler.start()
    }

    def cleanup() {
        scheduler?.stop(Duration.ofSeconds(2))
    }

    private long counter(String name, Map<String, String> tags = [:]) {
        return scheduler.metricsSnapshot().counter(name, tags)
    }

    def "a successful run is recorded OK with metrics"() {
        given:
        scheduler.submit(Task.define('ok') {
            handler { }
            every Duration.ofHours(1)
        })

        when:
        scheduler.runNow('ok')

        then:
        poll.eventually {
            List<RunRecord> h = scheduler.historyOf('ok', 10)
            h.size() == 1 && h[0].outcome == RunOutcome.OK && h[0].attempt == 1 &&
                    h[0].nodeId == 'node-a' && h[0].startedAt != null && h[0].finishedAt != null
        }

        and:
        counter('gron.runs.started', [node: 'node-a']) >= 1
        counter('gron.runs.completed', [outcome: 'ok', node: 'node-a']) >= 1
        scheduler.metricsSnapshot().timer('gron.run.duration', [outcome: 'ok']).count >= 1
    }

    def "each failed retry attempt is recorded and metrics count retries"() {
        given:
        scheduler.submit(Task.define('fail') {
            handler { throw new RuntimeException('boom') }
            every Duration.ofHours(1)
            retries 2, Duration.ofMillis(10)
        })

        when:
        scheduler.runNow('fail')

        then: 'three FAILED records (attempts 1, 2, 3)'
        poll.eventually {
            List<RunRecord> h = scheduler.historyOf('fail', 10)
            h.size() == 3 && h.every { it.outcome == RunOutcome.FAILED } &&
                    h.collect { it.attempt }.toSet() == [1, 2, 3].toSet() &&
                    h.every { it.errorType == 'java.lang.RuntimeException' && it.errorMessage == 'boom' }
        }

        and:
        counter('gron.runs.retried') == 2
        counter('gron.runs.started', [node: 'node-a']) == 3
        counter('gron.runs.completed', [outcome: 'failed', node: 'node-a']) == 3
    }

    def "an overlap skip is recorded SKIPPED/OVERLAP"() {
        given:
        CountDownLatch latch = new CountDownLatch(1)
        scheduler.submit(Task.define('ov') {
            handler { latch.await(3, TimeUnit.SECONDS) }
            every Duration.ofSeconds(10), T0
            overlap de.example.gron.api.OverlapPolicy.SKIP
        })

        when: 'first occurrence starts and blocks'
        clock.setInstant(T0.plusSeconds(10))
        poll.eventually { scheduler.activeRuns().size() == 1 }

        and: 'next occurrence becomes due while the first is active'
        clock.setInstant(T0.plusSeconds(20))

        then: 'the skipped occurrence is recorded'
        poll.eventually {
            scheduler.queryHistory(new HistoryQuery(taskId: 'ov',
                    outcomes: [RunOutcome.SKIPPED] as Set, limit: 10))
                    .any { it.skipReason == SkipReason.OVERLAP && it.startedAt == null }
        }
        counter('gron.runs.skipped', [reason: 'overlap']) >= 1

        cleanup:
        latch.countDown()
    }

    def "an overdue skip is recorded SKIPPED/OVERDUE"() {
        given:
        scheduler.submit(Task.define('od') {
            handler { }
            every Duration.ofSeconds(10), T0
            catchUp de.example.gron.api.CatchUpPolicy.SKIP
            overdueAfter Duration.ofSeconds(60)
        })

        when: 'jump far past the first due time'
        clock.setInstant(T0.plusSeconds(300))

        then:
        poll.eventually {
            scheduler.queryHistory(new HistoryQuery(taskId: 'od',
                    outcomes: [RunOutcome.SKIPPED] as Set, limit: 10))
                    .any { it.skipReason == SkipReason.OVERDUE }
        }
        counter('gron.runs.skipped', [reason: 'overdue']) >= 1
    }

    def "history OFF per task suppresses records but metrics still count"() {
        given:
        scheduler.submit(Task.define('quiet') {
            handler { }
            every Duration.ofHours(1)
            history HistoryMode.OFF
        })

        when:
        scheduler.runNow('quiet')

        then:
        poll.eventually { counter('gron.runs.completed', [outcome: 'ok', node: 'node-a']) >= 1 }
        scheduler.historyOf('quiet', 10).isEmpty()
    }

    def "metrics taskTag opt-in creates an extra tagged series"() {
        given:
        scheduler.submit(Task.define('tagged') {
            handler { }
            every Duration.ofHours(1)
            metrics taskTag: true
        })
        scheduler.submit(Task.define('untagged') {
            handler { }
            every Duration.ofHours(1)
        })

        when:
        scheduler.runNow('tagged')
        scheduler.runNow('untagged')

        then:
        poll.eventually {
            counter('gron.runs.started', [node: 'node-a', task: 'tagged']) == 1
        }

        and: 'the untagged task produces no task-tagged series'
        MetricsSnapshot snap = scheduler.metricsSnapshot()
        snap.counter('gron.runs.started', [node: 'node-a', task: 'untagged']) == 0
    }
}
