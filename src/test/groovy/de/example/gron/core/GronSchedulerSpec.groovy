package de.example.gron.core

import de.example.gron.api.RunOutcome
import de.example.gron.api.SkipReason
import de.example.gron.api.Task
import de.example.gron.api.TaskContext
import de.example.gron.api.TaskFailedException
import de.example.gron.api.TaskListener
import de.example.gron.api.TaskState
import de.example.gron.util.MutableClock
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * End-to-end verification of {@link GronScheduler}: execution, runNow, retry to
 * FAILED, abortSchedule, pause/resume, graceful stop and listener ordering.
 * Scheduling decisions use an injected {@link MutableClock}; the loop polls at a
 * short {@code maxSleep} so advancing the clock takes effect quickly.
 */
class GronSchedulerSpec extends Specification {

    private static final Instant T0 = Instant.parse('2026-01-01T00:00:00Z')

    MutableClock clock = new MutableClock(T0)
    GronScheduler scheduler
    PollingConditions poll = new PollingConditions(timeout: 5, delay: 0.02)

    def setup() {
        scheduler = GronScheduler.builder()
                .name('test')
                .nodeId('node-a')
                .workers(3)
                .clock(clock)
                .maxSleep(Duration.ofMillis(20))
                .maintenanceInterval(Duration.ofMillis(50))
                .build()
        scheduler.start()
    }

    def cleanup() {
        scheduler?.stop(Duration.ofSeconds(2))
    }

    def "runs a due task"() {
        given:
        AtomicInteger runs = new AtomicInteger(0)
        scheduler.submit(Task.define('t') {
            handler { runs.incrementAndGet() }
            every Duration.ofSeconds(1)
        })

        when:
        clock.advance(Duration.ofSeconds(1))

        then:
        poll.eventually { runs.get() >= 1 }
    }

    def "runNow triggers an immediate ad-hoc run"() {
        given:
        AtomicInteger runs = new AtomicInteger(0)
        scheduler.submit(Task.define('t') {
            handler { runs.incrementAndGet() }
            every Duration.ofHours(1)
        })

        when:
        scheduler.runNow('t')

        then:
        poll.eventually { runs.get() == 1 }
    }

    def "retries then moves to FAILED via abortSchedule after exhausting retries"() {
        given:
        AtomicInteger attempts = new AtomicInteger(0)
        scheduler.submit(Task.define('t') {
            handler { throw new TaskFailedException('boom', true) }
            every Duration.ofHours(1)
            retries 2, Duration.ofMillis(10)
        })

        when: 'attempts happen via retries (1 + 2 = 3)'
        scheduler.runNow('t')

        then:
        poll.eventually { scheduler.stateOf('t') == TaskState.FAILED }
    }

    def "retry counter reaches maxRetries+1 attempts"() {
        given:
        AtomicInteger attempts = new AtomicInteger(0)
        scheduler.submit(Task.define('t') {
            handler { attempts.incrementAndGet(); throw new RuntimeException('x') }
            every Duration.ofHours(1)
            retries 2, Duration.ofMillis(10)
        })

        when:
        scheduler.runNow('t')

        then: 'first attempt + two retries'
        poll.eventually { attempts.get() == 3 }

        and: 'without abortSchedule the schedule continues (not FAILED)'
        scheduler.stateOf('t') != TaskState.FAILED
    }

    def "pause prevents runs, resume allows them again"() {
        given:
        AtomicInteger runs = new AtomicInteger(0)
        scheduler.submit(Task.define('t') {
            handler { runs.incrementAndGet() }
            every Duration.ofSeconds(1)
        })
        scheduler.pause('t')

        when:
        clock.advance(Duration.ofSeconds(2))

        then:
        scheduler.stateOf('t') == TaskState.PAUSED

        when:
        scheduler.resume('t')

        then:
        poll.eventually { runs.get() >= 1 }
    }

    def "graceful stop waits for a running task to finish"() {
        given:
        AtomicInteger finished = new AtomicInteger(0)
        scheduler.submit(Task.define('t') {
            handler { Thread.sleep(300); finished.incrementAndGet() }
            every Duration.ofHours(1)
        })
        scheduler.runNow('t')
        poll.eventually { scheduler.activeRuns().size() == 1 }

        when:
        scheduler.stop(Duration.ofSeconds(3))

        then:
        finished.get() == 1
    }

    def "listeners fire beforeRun then afterRun in order"() {
        given:
        Queue<String> events = new ConcurrentLinkedQueue<>()
        scheduler.addListener(new TaskListener() {
            void beforeRun(TaskContext c) { events.add('before') }
            void afterRun(TaskContext c, RunOutcome o) { events.add('after:' + o) }
            void onSkip(String id, Instant t, SkipReason r) { events.add('skip') }
            void onError(TaskContext c, Throwable e) { events.add('error') }
        })
        scheduler.submit(Task.define('t') {
            handler { }
            every Duration.ofHours(1)
        })

        when:
        scheduler.runNow('t')

        then:
        poll.eventually { events.size() == 2 }
        events.toList() == ['before', 'after:OK']
    }
}
