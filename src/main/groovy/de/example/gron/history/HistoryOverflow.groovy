package de.example.gron.history

/** What the history pipeline does when its bounded queue is full. */
enum HistoryOverflow {

    /** Drop the oldest queued record to make room (default). */
    DROP_OLDEST,

    /** Drop the newly offered record. */
    DROP_NEW,

    /** Block the calling (writer-feeding) path until room is available. */
    BLOCK
}
