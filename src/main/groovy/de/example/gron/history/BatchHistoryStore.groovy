package de.example.gron.history

/**
 * Optional capability a {@link HistoryStore} may implement to accept records in
 * batches. The asynchronous writer already groups records, so a batch-aware
 * store (e.g. the JDBC store, which uses JDBC batches) can persist a whole group
 * in one round-trip. Kept out of the core {@link HistoryStore} SPI so it stays
 * small; the writer falls back to per-record {@code record(...)} otherwise.
 */
interface BatchHistoryStore {

    /** Persists a batch of records (oldest first within the batch). */
    void recordBatch(List<RunRecord> records)
}
