package de.example.gron.spi

import de.example.gron.api.CatchUpPolicy
import groovy.transform.CompileStatic

import java.time.Clock

/**
 * Environment handed to a {@link TaskStore} on {@link TaskStore#open}. It
 * carries the serializer, the time source, the class loader used to resolve
 * handler classes, a wake-up signaler for the control loop, and scheduler
 * defaults.
 */
@CompileStatic
class StoreContext {

    /** Serializer used to persist and restore tasks. */
    final Serializer serializer

    /** Injected time source (also used for catch-up decisions). */
    final Clock clock

    /** Class loader used to resolve handler classes on deserialization. */
    final ClassLoader classLoader

    /** Signaler to wake the control loop when a run becomes due early. */
    final Signaler signaler

    /** Sink for store-side skip decisions (overlap/catch-up) to reach listeners. */
    final SkipSink skipSink

    /** Default catch-up policy applied when a task does not override it. */
    final CatchUpPolicy defaultCatchUp

    StoreContext(Serializer serializer, Clock clock, ClassLoader classLoader,
                 Signaler signaler, SkipSink skipSink,
                 CatchUpPolicy defaultCatchUp = CatchUpPolicy.SKIP) {
        this.serializer = serializer
        this.clock = clock
        this.classLoader = classLoader
        this.signaler = signaler
        this.skipSink = skipSink
        this.defaultCatchUp = defaultCatchUp
    }
}
