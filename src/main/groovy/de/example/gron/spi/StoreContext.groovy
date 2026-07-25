package de.example.gron.spi

import de.example.gron.api.CatchUpPolicy
import de.example.gron.metrics.MetricsCollector
import de.example.gron.metrics.NoOpMetricsCollector
import groovy.transform.CompileStatic

import java.time.Clock

/**
 * Environment handed to a {@link TaskStore} (and {@link
 * de.example.gron.history.HistoryStore}) on open. It carries the serializer, the
 * time source, the class loader used to resolve handler classes, a wake-up
 * signaler for the control loop, the skip sink, the metrics collector, and
 * scheduler defaults.
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

    /** Metrics collector for store-side timers/counters. */
    final MetricsCollector metrics

    /** Default catch-up policy applied when a task does not override it. */
    final CatchUpPolicy defaultCatchUp

    StoreContext(Serializer serializer, Clock clock, ClassLoader classLoader,
                 Signaler signaler, SkipSink skipSink,
                 MetricsCollector metrics = new NoOpMetricsCollector(),
                 CatchUpPolicy defaultCatchUp = CatchUpPolicy.SKIP) {
        this.serializer = serializer
        this.clock = clock
        this.classLoader = classLoader
        this.signaler = signaler
        this.skipSink = skipSink
        this.metrics = metrics == null ? new NoOpMetricsCollector() : metrics
        this.defaultCatchUp = defaultCatchUp
    }

    /** @return the metrics collector. */
    MetricsCollector metrics() {
        return metrics
    }
}
