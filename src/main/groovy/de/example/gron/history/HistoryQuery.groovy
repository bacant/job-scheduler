package de.example.gron.history

import de.example.gron.api.RunOutcome

import java.time.Instant

/**
 * Filter for {@link HistoryStore#query}. All fields are optional; results are
 * returned newest first. Pagination is keyset-based (no offset paging): pass the
 * {@code startedAt} of the last seen record as {@link #startedBefore} to fetch
 * the next page.
 *
 * <p>Left as a plain Groovy POGO (with the implicit named-argument constructor)
 * because it is a configuration DTO, not a hot-path type.</p>
 */
class HistoryQuery {

    /** Restrict to a task id; {@code null} = all tasks. */
    String taskId

    /** Restrict to records carrying this tag; {@code null} = any. */
    String tag

    /** Restrict to these outcomes; {@code null} = all. */
    Set<RunOutcome> outcomes

    /** Restrict to this node id; {@code null} = any. */
    String nodeId

    /** Inclusive lower bound on the record time; {@code null} = open. */
    Instant from

    /** Inclusive upper bound on the record time; {@code null} = open. */
    Instant to

    /** Keyset cursor: return only records started strictly before this instant. */
    Instant startedBefore

    /** Maximum number of records to return. */
    int limit = 100
}
