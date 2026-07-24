package de.example.gron.schedule

import de.example.gron.api.InvalidScheduleException
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

/**
 * Table-driven verification of {@link CronSchedule}, covering 5- and 6-field
 * expressions, macros, names, steps, DOM/DOW OR-semantics, DST transitions and
 * the no-further-run case.
 */
class CronScheduleSpec extends Specification {

    private static final ZoneId UTC = ZoneId.of('UTC')
    private static final ZoneId BERLIN = ZoneId.of('Europe/Berlin')

    private static Instant utc(String isoLocal) {
        return ZonedDateTime.parse(isoLocal + 'Z').toInstant()
    }

    @Unroll
    def "cron '#expr' after #fromIso yields #expectedIso"() {
        given:
        CronSchedule schedule = new CronSchedule(expr, UTC)

        expect:
        schedule.nextRunAfter(utc(fromIso)) == utc(expectedIso)

        where:
        expr                    | fromIso                | expectedIso
        '0 * * * *'             | '2026-01-01T10:15:00'  | '2026-01-01T11:00:00'
        '*/15 * * * *'          | '2026-01-01T10:05:00'  | '2026-01-01T10:15:00'
        '0 0 * * *'             | '2026-01-01T10:00:00'  | '2026-01-02T00:00:00'
        '30 9 * * MON-FRI'      | '2026-01-03T00:00:00'  | '2026-01-05T09:30:00'  // Sat -> Mon
        '0 0 1 * *'             | '2026-01-15T00:00:00'  | '2026-02-01T00:00:00'
        '@daily'                | '2026-01-01T05:00:00'  | '2026-01-02T00:00:00'
        '@hourly'               | '2026-01-01T05:30:00'  | '2026-01-01T06:00:00'
        '@monthly'              | '2026-01-10T00:00:00'  | '2026-02-01T00:00:00'
        '@weekly'               | '2026-01-01T00:00:00'  | '2026-01-04T00:00:00'  // next Sunday
        '@yearly'               | '2026-03-01T00:00:00'  | '2027-01-01T00:00:00'
        '0 12 * * SUN'          | '2026-01-01T00:00:00'  | '2026-01-04T12:00:00'
        '0 0 * * 0'             | '2026-01-01T00:00:00'  | '2026-01-04T00:00:00'  // 0 = Sunday
        '0 0 * * 7'             | '2026-01-01T00:00:00'  | '2026-01-04T00:00:00'  // 7 = Sunday too
        '15 10 * JAN *'         | '2026-01-01T00:00:00'  | '2026-01-01T10:15:00'
        '30 2-4 * * *'          | '2026-01-01T00:00:00'  | '2026-01-01T02:30:00'
        '0 0 0 1 1 *'           | '2026-06-01T00:00:00'  | '2027-01-01T00:00:00'  // 6-field seconds
        '30 30 9 * * *'         | '2026-01-01T00:00:00'  | '2026-01-01T09:30:30'  // 6-field seconds
        '3-59/15 * * * *'       | '2026-01-01T10:00:00'  | '2026-01-01T10:03:00'
    }

    def "DOM and DOW restricted use classic OR semantics"() {
        given: 'run on the 15th OR on any Monday'
        CronSchedule schedule = new CronSchedule('0 0 15 * MON', UTC)

        expect: 'the 5th (a Monday) matches via DOW even though it is not the 15th'
        schedule.nextRunAfter(utc('2026-01-01T00:00:00')) == utc('2026-01-05T00:00:00')

        and: 'from just after that Monday, the 12th (next Monday) matches'
        schedule.nextRunAfter(utc('2026-01-05T00:00:00')) == utc('2026-01-12T00:00:00')

        and: 'the 15th matches via DOM (a Thursday)'
        schedule.nextRunAfter(utc('2026-01-13T00:00:00')) == utc('2026-01-15T00:00:00')
    }

    def "DST spring-forward in Europe/Berlin skips the missing hour"() {
        given: 'on 2026-03-29 clocks jump 02:00 -> 03:00 CET->CEST'
        CronSchedule schedule = new CronSchedule('0 30 2 * * *', BERLIN)
        Instant from = ZonedDateTime.parse('2026-03-29T00:00:00+01:00[Europe/Berlin]').toInstant()

        when: 'the 02:30 local run does not exist that day'
        Instant next = schedule.nextRunAfter(from)

        then: 'the schedule advances to the next valid 02:30 local time (next day)'
        ZonedDateTime local = next.atZone(BERLIN)
        local.hour == 2 && local.minute == 30
        local.dayOfMonth == 30
    }

    def "DST fall-back in Europe/Berlin still produces a daily run"() {
        given: 'on 2026-10-25 clocks fall 03:00 -> 02:00 CEST->CET'
        CronSchedule schedule = new CronSchedule('0 30 2 * * *', BERLIN)
        Instant from = ZonedDateTime.parse('2026-10-25T00:00:00+02:00[Europe/Berlin]').toInstant()

        when:
        Instant next = schedule.nextRunAfter(from)

        then: 'a 02:30 local run exists on that day'
        ZonedDateTime local = next.atZone(BERLIN)
        local.hour == 2 && local.minute == 30 && local.dayOfMonth == 25
    }

    def "an unsatisfiable expression yields null (task becomes DONE)"() {
        given: 'Feb 30th never exists'
        CronSchedule schedule = new CronSchedule('0 0 30 2 *', UTC)

        expect:
        schedule.nextRunAfter(utc('2026-01-01T00:00:00')) == null
    }

    @Unroll
    def "invalid expression '#bad' raises InvalidScheduleException"() {
        when:
        new CronSchedule(bad, UTC)

        then:
        thrown(InvalidScheduleException)

        where:
        bad << ['not a cron', '99 * * * *', '0 0 * *', '* * * * * * *', '0 0 * * 8']
    }

    def "next run is strictly after the reference"() {
        given:
        CronSchedule schedule = new CronSchedule('0 * * * *', UTC)
        Instant onTheHour = utc('2026-01-01T10:00:00')

        expect: 'passing an exact match returns the following occurrence'
        schedule.nextRunAfter(onTheHour) == utc('2026-01-01T11:00:00')
    }
}
