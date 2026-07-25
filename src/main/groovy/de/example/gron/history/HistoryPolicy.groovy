package de.example.gron.history

import java.time.Duration

/**
 * Configuration for the history pipeline. A plain Groovy POGO so it can be built
 * with named arguments, e.g.:
 *
 * <pre>
 * new HistoryPolicy(mode: HistoryMode.ALL, retention: Duration.ofDays(30),
 *                   buffer: 10_000, overflow: HistoryOverflow.DROP_OLDEST,
 *                   stackTraceLimit: 4000)
 * </pre>
 */
class HistoryPolicy {

    /** Global history mode (per-task overridable via the DSL). */
    HistoryMode mode = HistoryMode.ALL

    /** How long records are kept before retention deletes them. */
    Duration retention = Duration.ofDays(30)

    /** Bounded queue capacity between the run path and the writer thread. */
    int buffer = 10_000

    /** Behaviour when the queue is full. */
    HistoryOverflow overflow = HistoryOverflow.DROP_OLDEST

    /** Maximum stored stack-trace length (characters). */
    int stackTraceLimit = 4000

    /** Maximum records written per batch by the writer thread. */
    int batchSize = 200

    /** Maximum time the writer waits to fill a batch. */
    Duration flushInterval = Duration.ofMillis(50)

    /** How often retention runs. */
    Duration housekeepingInterval = Duration.ofHours(1)
}
