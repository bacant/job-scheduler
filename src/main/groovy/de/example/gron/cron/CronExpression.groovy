package de.example.gron.cron

import groovy.transform.CompileStatic

import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

/**
 * Parses cron expressions and calculates the next execution time as
 * {@link java.util.Date} or {@link java.time.Instant}.
 *
 * <p>Supports classic 5-field expressions as well as 6-field expressions with
 * a leading seconds field:</p>
 *
 * <pre>
 *   ┌──────────── second (0-59, optional)
 *   │ ┌────────── minute (0-59)
 *   │ │ ┌──────── hour (0-23)
 *   │ │ │ ┌────── day of month (1-31)
 *   │ │ │ │ ┌──── month (1-12 or JAN-DEC)
 *   │ │ │ │ │ ┌── day of week (0-7 or SUN-SAT; 0 and 7 = Sunday)
 *   │ │ │ │ │ │
 *   * * * * * *
 * </pre>
 *
 * <p>Each field accepts: {@code *} (all values), single values ({@code 5}),
 * lists ({@code 1,3,5}), ranges ({@code 8-17}), step values
 * (<code>*&#47;15</code> or {@code 3-59/15}) as well as names for months and
 * days of the week. {@code ?} is treated like {@code *} (Quartz compatibility).
 * In addition, the macros {@code @yearly}, {@code @annually}, {@code @monthly},
 * {@code @weekly}, {@code @daily}, {@code @midnight} and {@code @hourly} are
 * supported. The Quartz special characters L, W and # are not supported.</p>
 *
 * <p>If both the day-of-month and the day-of-week field are restricted (i.e.
 * not {@code *} or {@code ?}), classic cron semantics apply: a match in either
 * of the two fields is sufficient.</p>
 *
 * <p>Instances are immutable after construction and therefore safe to share
 * between threads. The class is {@link Serializable} and {@link Cloneable};
 * {@link #clone()} returns a deep copy of the parsed state.</p>
 *
 * <p>The class is annotated with {@link groovy.transform.CompileStatic}, so it
 * is statically compiled: all types are checked at compile time and Groovy's
 * dynamic dispatch is bypassed, resulting in Java-like performance.</p>
 *
 * <p>Example:</p>
 * <pre>
 * def cron = new CronExpression('0 9 * * MON-FRI', ZoneId.of('Europe/Berlin'))
 * Date    date    = cron.nextDate()      // next execution as java.util.Date
 * Instant instant = cron.nextInstant()   // next execution as java.time.Instant
 * </pre>
 */
@CompileStatic
class CronExpression implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L

    private static final Map<String, String> MACROS = [
            '@yearly'  : '0 0 1 1 *',
            '@annually': '0 0 1 1 *',
            '@monthly' : '0 0 1 * *',
            '@weekly'  : '0 0 * * 0',
            '@daily'   : '0 0 * * *',
            '@midnight': '0 0 * * *',
            '@hourly'  : '0 * * * *'
    ].asImmutable()

    private static final Map<String, Integer> MONTH_NAMES = [
            JAN: 1, FEB: 2, MAR: 3, APR: 4, MAY: 5, JUN: 6,
            JUL: 7, AUG: 8, SEP: 9, OCT: 10, NOV: 11, DEC: 12
    ].asImmutable()

    private static final Map<String, Integer> DAY_NAMES = [
            SUN: 0, MON: 1, TUE: 2, WED: 3, THU: 4, FRI: 5, SAT: 6
    ].asImmutable()

    /** Empty name map for fields without symbolic names. */
    private static final Map<String, Integer> NO_NAMES =
            new LinkedHashMap<String, Integer>().asImmutable()

    /** Maximum search horizon in years before giving up (e.g. for '0 0 30 2 *'). */
    private static final int MAX_YEARS_AHEAD = 100

    /** The originally supplied cron expression. */
    final String expression

    /** Time zone in which the expression is evaluated. */
    final ZoneId zoneId

    private Set<Integer> seconds
    private Set<Integer> minutes
    private Set<Integer> hours
    private Set<Integer> daysOfMonth
    private Set<Integer> months
    private Set<Integer> daysOfWeek

    /** true if the day-of-month field is restricted (not '*' or '?'). */
    private boolean dayOfMonthRestricted

    /** true if the day-of-week field is restricted (not '*' or '?'). */
    private boolean dayOfWeekRestricted

    /**
     * @param expression cron expression with 5 or 6 fields, or a macro (@daily, @hourly, ...)
     * @param zoneId     time zone used for the calculation (default: system time zone)
     * @throws IllegalArgumentException if the expression is invalid
     */
    CronExpression(String expression, ZoneId zoneId = ZoneId.systemDefault()) {
        if (expression == null || expression.trim().isEmpty()) {
            throw new IllegalArgumentException('Cron expression must not be empty')
        }
        this.expression = expression.trim()
        this.zoneId = zoneId ?: ZoneId.systemDefault()
        parse(MACROS.getOrDefault(this.expression.toLowerCase(), this.expression))
    }

    // ------------------------------------------------------------------ API

    /** Next execution after the current time as {@link java.util.Date}. */
    Date nextDate() {
        return nextDate(new Date())
    }

    /** Next execution strictly after {@code from} as {@link java.util.Date}. */
    Date nextDate(Date from) {
        return Date.from(nextInstant(from.toInstant()))
    }

    /** Next execution after the current time as {@link java.time.Instant}. */
    Instant nextInstant() {
        return nextInstant(Instant.now())
    }

    /** Next execution strictly after {@code from} as {@link java.time.Instant}. */
    Instant nextInstant(Instant from) {
        return nextExecution(from.atZone(zoneId)).toInstant()
    }

    /** The next {@code count} executions starting from {@code from} as a list of instants. */
    List<Instant> nextInstants(int count, Instant from = Instant.now()) {
        List<Instant> result = new ArrayList<>(count)
        Instant current = from
        for (int n = 0; n < count; n++) {
            current = nextInstant(current)
            result.add(current)
        }
        return result
    }

    /**
     * Core calculation: returns the first point in time strictly after
     * {@code from} that matches this expression, as a {@link ZonedDateTime}
     * in the time zone of this expression.
     *
     * @throws IllegalStateException if no matching point in time exists within
     *         {@link #MAX_YEARS_AHEAD} years (e.g. '0 0 30 2 *')
     */
    ZonedDateTime nextExecution(ZonedDateTime from) {
        ZonedDateTime candidate = from.withZoneSameInstant(zoneId).plusSeconds(1).withNano(0)
        ZonedDateTime limit = candidate.plusYears(MAX_YEARS_AHEAD)

        while (candidate.isBefore(limit)) {
            if (!months.contains(candidate.monthValue)) {
                candidate = candidate.plusMonths(1).withDayOfMonth(1)
                        .withHour(0).withMinute(0).withSecond(0)
                continue
            }
            if (!dayMatches(candidate)) {
                candidate = candidate.plusDays(1).withHour(0).withMinute(0).withSecond(0)
                continue
            }
            if (!hours.contains(candidate.hour)) {
                candidate = candidate.plusHours(1).withMinute(0).withSecond(0)
                continue
            }
            if (!minutes.contains(candidate.minute)) {
                candidate = candidate.plusMinutes(1).withSecond(0)
                continue
            }
            if (!seconds.contains(candidate.second)) {
                candidate = candidate.plusSeconds(1)
                continue
            }
            return candidate
        }
        throw new IllegalStateException(
                "No execution found for '${expression}' within ${MAX_YEARS_AHEAD} years")
    }

    /** Checks whether an expression is valid without throwing an exception. */
    static boolean isValid(String expression) {
        try {
            new CronExpression(expression)
            return true
        } catch (IllegalArgumentException ignored) {
            return false
        }
    }

    /**
     * Creates a deep copy of this expression. The parsed value sets are
     * copied; {@code expression} and {@code zoneId} are immutable and
     * therefore shared with the copy.
     */
    @Override
    CronExpression clone() {
        CronExpression copy = (CronExpression) super.clone()
        copy.@seconds     = new TreeSet<Integer>(seconds)
        copy.@minutes     = new TreeSet<Integer>(minutes)
        copy.@hours       = new TreeSet<Integer>(hours)
        copy.@daysOfMonth = new TreeSet<Integer>(daysOfMonth)
        copy.@months      = new TreeSet<Integer>(months)
        copy.@daysOfWeek  = new TreeSet<Integer>(daysOfWeek)
        return copy
    }

    @Override
    String toString() {
        return "CronExpression('${expression}', zone=${zoneId})"
    }

    // ------------------------------------------------------------- Parsing

    private void parse(String resolvedExpression) {
        String[] fields = resolvedExpression.split(/\s+/)
        if (fields.length != 5 && fields.length != 6) {
            throw new IllegalArgumentException(
                    "Invalid cron expression '${expression}': expected 5 fields " +
                    '(minute hour day-of-month month day-of-week) or 6 fields ' +
                    "(with leading seconds field), found: ${fields.length}")
        }

        boolean withSeconds = fields.length == 6
        int index = 0

        seconds = withSeconds ? parseField(fields[index++], 0, 59, NO_NAMES, 'second')
                              : new TreeSet<Integer>([0])
        minutes = parseField(fields[index++], 0, 59, NO_NAMES, 'minute')
        hours   = parseField(fields[index++], 0, 23, NO_NAMES, 'hour')

        String dayOfMonthField = fields[index++]
        daysOfMonth = parseField(dayOfMonthField, 1, 31, NO_NAMES, 'day of month')

        months = parseField(fields[index++], 1, 12, MONTH_NAMES, 'month')

        String dayOfWeekField = fields[index]
        daysOfWeek = parseField(dayOfWeekField, 0, 7, DAY_NAMES, 'day of week')
        if (daysOfWeek.remove(7)) {   // 7 also means Sunday
            daysOfWeek.add(0)
        }

        dayOfMonthRestricted = dayOfMonthField != '*' && dayOfMonthField != '?'
        dayOfWeekRestricted  = dayOfWeekField != '*' && dayOfWeekField != '?'
    }

    /**
     * Splits a single cron field ('*', '?', values, lists, ranges, step
     * values, names such as JAN or MON) into the set of allowed values.
     */
    private Set<Integer> parseField(String field, int min, int max,
                                    Map<String, Integer> names, String fieldName) {
        Set<Integer> values = new TreeSet<>()

        for (String entry in field.split(',')) {
            if (entry.isEmpty()) {
                throw new IllegalArgumentException(
                        "Empty entry in field '${fieldName}' of '${expression}'")
            }

            String rangePart = entry
            int step = 1
            if (entry.contains('/')) {
                String[] parts = entry.split('/', 2)
                rangePart = parts[0]
                step = toInt(parts[1], fieldName)
                if (step < 1) {
                    throw new IllegalArgumentException(
                            "Step value must be at least 1 in field '${fieldName}': '${entry}'")
                }
            }

            int start
            int end
            if (rangePart == '*' || rangePart == '?') {
                start = min
                end = max
            } else if (rangePart.contains('-')) {
                String[] bounds = rangePart.split('-', 2)
                start = resolveValue(bounds[0], names, fieldName)
                end = resolveValue(bounds[1], names, fieldName)
                if (start > end) {
                    throw new IllegalArgumentException(
                            "Invalid range (start > end) in field '${fieldName}': '${rangePart}'")
                }
            } else {
                start = resolveValue(rangePart, names, fieldName)
                // '5/15' means: starting at 5 in steps of 15 up to the maximum
                end = entry.contains('/') ? max : start
            }

            checkRange(start, min, max, fieldName)
            checkRange(end, min, max, fieldName)
            for (int value = start; value <= end; value += step) {
                values.add(value)
            }
        }
        return values
    }

    /** Resolves names (JAN, MON, ...) or numbers to an int. */
    private int resolveValue(String token, Map<String, Integer> names, String fieldName) {
        Integer named = names.get(token.toUpperCase())
        if (named != null) {
            return named
        }
        return toInt(token, fieldName)
    }

    private int toInt(String token, String fieldName) {
        if (!token.isInteger()) {
            throw new IllegalArgumentException(
                    "Invalid value '${token}' in field '${fieldName}' of '${expression}'")
        }
        return token.toInteger()
    }

    private void checkRange(int value, int min, int max, String fieldName) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    "Value ${value} is outside the range ${min}-${max} in field '${fieldName}' of '${expression}'")
        }
    }

    // ------------------------------------------------------------ Matching

    /**
     * Day logic following classic cron semantics: if both day-of-month and
     * day-of-week are restricted, a match in EITHER of the two fields is
     * sufficient.
     */
    private boolean dayMatches(ZonedDateTime dateTime) {
        boolean domMatches = daysOfMonth.contains(dateTime.dayOfMonth)
        boolean dowMatches = daysOfWeek.contains(dateTime.dayOfWeek.value % 7)  // SUN=7 -> 0

        if (dayOfMonthRestricted && dayOfWeekRestricted) {
            return domMatches || dowMatches
        }
        if (dayOfMonthRestricted) {
            return domMatches
        }
        if (dayOfWeekRestricted) {
            return dowMatches
        }
        return true
    }
}
