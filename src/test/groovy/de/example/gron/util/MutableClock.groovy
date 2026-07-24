package de.example.gron.util

import java.time.Clock
import java.time.Instant
import java.time.ZoneId

/**
 * A {@link Clock} whose instant can be advanced manually, for deterministic
 * time-based tests without {@code sleep()}.
 */
class MutableClock extends Clock {

    private volatile Instant instant
    private final ZoneId zone

    MutableClock(Instant start, ZoneId zone = ZoneId.of('UTC')) {
        this.instant = start
        this.zone = zone
    }

    @Override
    ZoneId getZone() { return zone }

    @Override
    Clock withZone(ZoneId z) { return new MutableClock(instant, z) }

    @Override
    Instant instant() { return instant }

    synchronized void advance(java.time.Duration by) { instant = instant.plus(by) }

    synchronized void setInstant(Instant i) { instant = i }
}
