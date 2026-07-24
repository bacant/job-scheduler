package de.example.gron.api

/** Placement run mode: on how many nodes a due run executes. */
enum RunMode {

    /** Exactly one node in the cluster runs each occurrence (claim-based). */
    EXCLUSIVE,

    /**
     * Every active node runs each occurrence locally, without a cluster claim.
     * Requires an active, real replication provider.
     */
    EVERY_NODE
}
