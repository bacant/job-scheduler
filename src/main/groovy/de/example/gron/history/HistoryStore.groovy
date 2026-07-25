package de.example.gron.history

import de.example.gron.spi.StoreContext

import java.time.Instant

/**
 * Persistence SPI for run history. Deliberately small (at most 8 methods). The
 * scheduler writes through an asynchronous pipeline, so {@link #record} is
 * called from a dedicated writer thread — it must never block the scheduler's
 * execution path, but it may block the writer.
 */
interface HistoryStore {

    /** Opens the store with its runtime environment. */
    void open(StoreContext context)

    /** Releases resources. */
    void close()

    /**
     * Persists a single record. Called only from the history writer thread; may
     * block that thread but must never be called on (or block) the run path.
     */
    void record(RunRecord record)

    /** Queries records matching the filter, newest first. */
    List<RunRecord> query(HistoryQuery query)

    /**
     * Deletes records older than the cutoff (retention).
     *
     * @return the number of deleted records (best-effort)
     */
    long deleteOlderThan(Instant cutoff)
}
