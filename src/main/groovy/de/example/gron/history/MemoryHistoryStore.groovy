package de.example.gron.history

import de.example.gron.spi.StoreContext
import groovy.transform.CompileStatic

import java.time.Instant
import java.util.concurrent.locks.ReentrantLock

/**
 * In-memory {@link HistoryStore}: a bounded ring buffer (default capacity
 * 10 000). When full, the oldest record is evicted. Records are held
 * newest-first for query efficiency.
 */
@CompileStatic
class MemoryHistoryStore implements HistoryStore {

    private final int capacity
    private final ArrayDeque<RunRecord> records = new ArrayDeque<>()  // head = newest
    private final ReentrantLock lock = new ReentrantLock()

    MemoryHistoryStore(int capacity = 10_000) {
        this.capacity = Math.max(1, capacity)
    }

    @Override
    void open(StoreContext context) { }

    @Override
    void close() { }

    @Override
    void record(RunRecord record) {
        lock.lock()
        try {
            records.addFirst(record)
            while (records.size() > capacity) {
                records.removeLast()
            }
        } finally {
            lock.unlock()
        }
    }

    @Override
    List<RunRecord> query(HistoryQuery query) {
        int limit = HistoryFilter.limit(query)
        List<RunRecord> result = new ArrayList<>()
        lock.lock()
        try {
            for (RunRecord r : records) {   // newest first
                if (HistoryFilter.matches(r, query)) {
                    result.add(r)
                    if (result.size() >= limit) {
                        break
                    }
                }
            }
        } finally {
            lock.unlock()
        }
        return result
    }

    @Override
    long deleteOlderThan(Instant cutoff) {
        long removed = 0L
        lock.lock()
        try {
            Iterator<RunRecord> it = records.iterator()
            while (it.hasNext()) {
                RunRecord r = it.next()
                if (RunRecordJson.recordTime(r).isBefore(cutoff)) {
                    it.remove()
                    removed++
                }
            }
        } finally {
            lock.unlock()
        }
        return removed
    }
}
