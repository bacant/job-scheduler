package de.example.gron.schedule

import spock.lang.Specification

import java.time.Instant

class OneTimeScheduleSpec extends Specification {

    private static final Instant AT = Instant.parse('2026-01-01T12:00:00Z')

    def "runs exactly once, then never again"() {
        given:
        OneTimeSchedule schedule = new OneTimeSchedule(AT)

        expect:
        schedule.nextRunAfter(AT.minusSeconds(1)) == AT
        schedule.nextRunAfter(AT) == null
        schedule.nextRunAfter(AT.plusSeconds(1)) == null
    }

    def "null instant is rejected"() {
        when:
        new OneTimeSchedule(null)

        then:
        thrown(IllegalArgumentException)
    }
}
