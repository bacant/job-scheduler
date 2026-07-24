package de.example.gron.api

/** What an {@code EXCLUSIVE} run does when no node matches its node selector. */
enum NodeFallback {

    /** Wait indefinitely for a matching node (the run may age into catch-up). */
    WAIT,

    /**
     * After {@link Placement#fallbackAfter}, allow any node to claim the run.
     */
    ANY_NODE
}
