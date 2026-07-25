package de.example.gron.history

import de.example.gron.metrics.MetricCounter
import de.example.gron.metrics.MetricsCollector
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Asynchronous write pipeline in front of a {@link HistoryStore}. Records handed
 * in by the scheduler go into a bounded queue; a dedicated
 * {@code gron-history-writer} thread drains them in batches. The run path never
 * blocks (except when the {@link HistoryOverflow#BLOCK} policy is chosen
 * explicitly): under overflow the configured policy applies and every lost
 * record increments {@code gron.history.dropped}, logged once per overflow phase.
 */
@CompileStatic
class AsyncHistoryWriter {

    private static final Logger log = LoggerFactory.getLogger(AsyncHistoryWriter)

    private final HistoryStore store
    private final HistoryPolicy policy
    private final LinkedBlockingQueue<RunRecord> queue
    private final MetricCounter dropped
    private final AtomicBoolean overflowLogged = new AtomicBoolean(false)

    private volatile boolean running = false
    private Thread writer

    AsyncHistoryWriter(HistoryStore store, HistoryPolicy policy, MetricsCollector metrics) {
        this.store = store
        this.policy = policy
        this.queue = new LinkedBlockingQueue<>(Math.max(1, policy.buffer))
        this.dropped = metrics.counter('gron.history.dropped', Collections.<String, String> emptyMap())
    }

    void start() {
        running = true
        writer = new Thread({ drainLoop() } as Runnable, 'gron-history-writer')
        writer.setDaemon(true)
        writer.start()
    }

    /** @return the current queue depth (for the {@code gron.queue.depth}-style gauges). */
    int queueDepth() {
        return queue.size()
    }

    /**
     * Offers a record to the pipeline according to the overflow policy. Never
     * blocks for DROP_OLDEST/DROP_NEW; blocks only for BLOCK.
     */
    void submit(RunRecord record) {
        if (!running) {
            return
        }
        switch (policy.overflow) {
            case HistoryOverflow.BLOCK:
                try {
                    queue.put(record)
                    resetOverflow()
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt()
                }
                break
            case HistoryOverflow.DROP_NEW:
                if (queue.offer(record)) {
                    resetOverflow()
                } else {
                    dropped.increment()
                    markOverflow()
                }
                break
            case HistoryOverflow.DROP_OLDEST:
            default:
                while (!queue.offer(record)) {
                    if (queue.poll() != null) {
                        dropped.increment()
                        markOverflow()
                    }
                }
                resetOverflow()
                break
        }
    }

    /** Stops the writer, draining the queue up to the given timeout. */
    void stop(long timeoutMillis) {
        running = false
        if (writer != null) {
            try {
                writer.join(Math.max(1L, timeoutMillis))
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt()
            }
            if (writer.isAlive()) {
                writer.interrupt()
            }
        }
    }

    private void drainLoop() {
        long flushMs = Math.max(1L, policy.flushInterval.toMillis())
        int batchSize = Math.max(1, policy.batchSize)
        List<RunRecord> batch = new ArrayList<>(batchSize)
        boolean batchCapable = store instanceof BatchHistoryStore
        while (running || !queue.isEmpty()) {
            RunRecord first
            try {
                first = queue.poll(flushMs, TimeUnit.MILLISECONDS)
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt()
                if (!running) {
                    break
                }
                continue
            }
            if (first == null) {
                continue
            }
            batch.clear()
            batch.add(first)
            queue.drainTo(batch, batchSize - 1)
            writeBatch(batch, batchCapable)
        }
        // Final drain of anything left after running became false.
        batch.clear()
        queue.drainTo(batch)
        if (!batch.isEmpty()) {
            writeBatch(batch, batchCapable)
        }
    }

    private void writeBatch(List<RunRecord> batch, boolean batchCapable) {
        try {
            if (batchCapable) {
                ((BatchHistoryStore) store).recordBatch(batch)
            } else {
                for (RunRecord r : batch) {
                    store.record(r)
                }
            }
        } catch (Exception e) {
            log.warn('History writer failed to persist {} record(s): {}', batch.size(), e.message)
        }
    }

    private void markOverflow() {
        if (overflowLogged.compareAndSet(false, true)) {
            log.warn('History queue overflow (capacity {}); applying {} policy, ' +
                    'records are being dropped (see gron.history.dropped)',
                    policy.buffer, policy.overflow)
        }
    }

    private void resetOverflow() {
        overflowLogged.set(false)
    }
}
