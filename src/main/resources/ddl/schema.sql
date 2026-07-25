-- GronScheduler JDBC schema (ANSI-near; tested against H2 in-memory).
--
-- Time columns store epoch nanoseconds as BIGINT to avoid database time-zone
-- pitfalls and to preserve full java.time.Instant precision. Adjust column
-- types for your database if needed (e.g. CLOB for very large definitions).

CREATE TABLE IF NOT EXISTS GRON_TASK (
    ID            VARCHAR(255)  NOT NULL PRIMARY KEY,
    DEFINITION    VARCHAR(1000000) NOT NULL,   -- task JSON (id, handler, schedule, options)
    TAGS          VARCHAR(4000),               -- comma-delimited, wrapped: ,a,b,
    NEXT_RUN      BIGINT,                       -- cursor: next planned occurrence (nanos), or NULL
    PREVIOUS_RUN  BIGINT,                       -- last executed planned time (nanos), or NULL
    PENDING_WAIT  BIGINT,                       -- deferred WAIT occurrence (nanos), or NULL
    PAUSED        BOOLEAN       NOT NULL DEFAULT FALSE,
    FAILED        BOOLEAN       NOT NULL DEFAULT FALSE
);

-- Supports the optimistic claim path: candidates scanned by (paused/failed, next_run).
-- The claim itself is a compare-and-set on the (NEXT_RUN, PENDING_WAIT) cursor,
-- which serves as the CAS witness in place of a discrete state column.
CREATE INDEX IF NOT EXISTS IDX_GRON_TASK_NEXT_RUN ON GRON_TASK (NEXT_RUN);

-- Active claims / in-flight runs. One row per claimed, not-yet-completed run.
CREATE TABLE IF NOT EXISTS GRON_RUN (
    CLAIM_TOKEN   VARCHAR(64)   NOT NULL PRIMARY KEY,
    TASK_ID       VARCHAR(255)  NOT NULL,
    PLANNED_TIME  BIGINT        NOT NULL,       -- planned time of this run (nanos)
    CLAIMED_BY    VARCHAR(255)  NOT NULL,       -- node id holding the claim
    CLAIMED_AT    BIGINT        NOT NULL,       -- when claimed (nanos)
    RECOVERABLE   BOOLEAN       NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS IDX_GRON_RUN_TASK ON GRON_RUN (TASK_ID);
CREATE INDEX IF NOT EXISTS IDX_GRON_RUN_NODE ON GRON_RUN (CLAIMED_BY);

-- Cluster node registry with heartbeats.
CREATE TABLE IF NOT EXISTS GRON_NODE (
    ID            VARCHAR(255)  NOT NULL PRIMARY KEY,
    TAGS          VARCHAR(4000),
    LAST_SEEN     BIGINT        NOT NULL        -- last heartbeat (nanos)
);

-- Cluster-wide named locks (acquired via SELECT ... FOR UPDATE on the row).
CREATE TABLE IF NOT EXISTS GRON_LOCK (
    NAME          VARCHAR(255)  NOT NULL PRIMARY KEY,
    HOLDER        VARCHAR(255),
    ACQUIRED_AT   BIGINT
);

-- Run history. RECORD_TIME = started_at, else planned_time (never null); it is
-- the column indexed for ordering, keyset paging and retention, because skipped
-- runs have no start time.
CREATE TABLE IF NOT EXISTS GRON_RUN_HISTORY (
    RUN_ID        VARCHAR(64)   NOT NULL PRIMARY KEY,
    TASK_ID       VARCHAR(255)  NOT NULL,
    TAGS          VARCHAR(4000),               -- comma-delimited, wrapped: ,a,b,
    PLANNED_TIME  BIGINT        NOT NULL,
    STARTED_AT    BIGINT,                       -- null for SKIPPED
    FINISHED_AT   BIGINT,                       -- null for SKIPPED
    DURATION_MS   BIGINT        NOT NULL DEFAULT 0,
    NODE_ID       VARCHAR(255),
    ATTEMPT       INTEGER       NOT NULL DEFAULT 1,
    OUTCOME       VARCHAR(16)   NOT NULL,       -- OK | FAILED | SKIPPED
    SKIP_REASON   VARCHAR(16),                  -- OVERLAP | OVERDUE, only for SKIPPED
    RECOVERED     BOOLEAN       NOT NULL DEFAULT FALSE,
    ERROR_TYPE    VARCHAR(512),
    ERROR_MESSAGE VARCHAR(4000),
    ERROR_STACK   VARCHAR(1000000),
    RECORD_TIME   BIGINT        NOT NULL
);

CREATE INDEX IF NOT EXISTS IDX_GRON_HIST_TASK_TIME ON GRON_RUN_HISTORY (TASK_ID, RECORD_TIME);
CREATE INDEX IF NOT EXISTS IDX_GRON_HIST_TIME ON GRON_RUN_HISTORY (RECORD_TIME);
