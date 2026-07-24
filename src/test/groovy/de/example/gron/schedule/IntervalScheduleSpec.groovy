package de.example.gron.schedule

import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class IntervalScheduleSpec extends Specification {

    private static final Instant T0 = Instant.parse('2026-01-01T00:00:00Z')

    def "anchored schedule produces drift-free multiples of the period"() {
        given:
        IntervalSchedule schedule = new IntervalSchedule(Duration.ofMinutes(5), T0, null)

        expect:
        schedule.nextRunAfter(T0.minusSeconds(1)) == T0
        schedule.nextRunAfter(T0) == T0.plusSeconds(300)
        schedule.nextRunAfter(T0.plusSeconds(300)) == T0.plusSeconds(600)
    }

    def "no drift: a late reference between ticks still lands on the next tick"() {
        given:
        IntervalSchedule schedule = new IntervalSchedule(Duration.ofMinutes(5), T0, null)

        expect: 'reference at +301s (past the +300 tick) yields the +600 tick, not +601'
        schedule.nextRunAfter(T0.plusSeconds(301)) == T0.plusSeconds(600)
    }

    def "maxRuns bounds the number of occurrences"() {
        given:
        IntervalSchedule schedule = new IntervalSchedule(Duration.ofMinutes(1), T0, 3)

        expect:
        schedule.nextRunAfter(T0.minusSeconds(1)) == T0                 // index 0
        schedule.nextRunAfter(T0) == T0.plusSeconds(60)                 // index 1
        schedule.nextRunAfter(T0.plusSeconds(60)) == T0.plusSeconds(120) // index 2
        schedule.nextRunAfter(T0.plusSeconds(120)) == null              // index 3 -> exhausted
    }

    def "lazily anchored schedule runs one period after the reference"() {
        given:
        IntervalSchedule schedule = new IntervalSchedule(Duration.ofSeconds(30), null, null)

        expect:
        schedule.nextRunAfter(T0) == T0.plusSeconds(30)
    }

    def "invalid configurations are rejected"() {
        when:
        new IntervalSchedule(period, start, maxRuns)

        then:
        thrown(IllegalArgumentException)

        where:
        period                 | start | maxRuns
        Duration.ZERO          | null  | null
        Duration.ofMinutes(-1) | null  | null
        Duration.ofMinutes(1)  | null  | 5      // maxRuns without anchor
        Duration.ofMinutes(1)  | T0    | 0      // maxRuns < 1
    }
}
