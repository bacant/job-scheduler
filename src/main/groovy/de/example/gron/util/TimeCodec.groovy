package de.example.gron.util

import groovy.transform.CompileStatic

import java.time.Instant

/**
 * Lossless conversion between {@link Instant} and epoch-nanoseconds
 * ({@code Long}) for storage in {@code BIGINT} columns.
 */
@CompileStatic
final class TimeCodec {

    private TimeCodec() { }

    /** @return epoch nanoseconds, or {@code null} for a null instant. */
    static Long toNanos(Instant instant) {
        if (instant == null) {
            return null
        }
        return Math.addExact(Math.multiplyExact(instant.epochSecond, 1_000_000_000L),
                (long) instant.nano)
    }

    /** @return an instant from epoch nanoseconds, or {@code null}. */
    static Instant fromNanos(Number nanos) {
        if (nanos == null) {
            return null
        }
        long n = nanos.longValue()
        long secs = Math.floorDiv(n, 1_000_000_000L)
        long nano = Math.floorMod(n, 1_000_000_000L)
        return Instant.ofEpochSecond(secs, nano)
    }
}
