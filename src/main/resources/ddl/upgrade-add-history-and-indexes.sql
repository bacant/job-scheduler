-- Migration for existing GronScheduler installations: adds run history and the
-- throughput index. Safe to run repeatedly (IF NOT EXISTS everywhere). Tested
-- against H2; adjust types for your database if needed.

-- Throughput: index the schedule cursor used by the optimistic claim path.
CREATE INDEX IF NOT EXISTS IDX_GRON_TASK_NEXT_RUN ON GRON_TASK (NEXT_RUN);

-- Run history table (see schema.sql for column semantics).
CREATE TABLE IF NOT EXISTS GRON_RUN_HISTORY (
    RUN_ID        VARCHAR(64)   NOT NULL PRIMARY KEY,
    TASK_ID       VARCHAR(255)  NOT NULL,
    TAGS          VARCHAR(4000),
    PLANNED_TIME  BIGINT        NOT NULL,
    STARTED_AT    BIGINT,
    FINISHED_AT   BIGINT,
    DURATION_MS   BIGINT        NOT NULL DEFAULT 0,
    NODE_ID       VARCHAR(255),
    ATTEMPT       INTEGER       NOT NULL DEFAULT 1,
    OUTCOME       VARCHAR(16)   NOT NULL,
    SKIP_REASON   VARCHAR(16),
    RECOVERED     BOOLEAN       NOT NULL DEFAULT FALSE,
    ERROR_TYPE    VARCHAR(512),
    ERROR_MESSAGE VARCHAR(4000),
    ERROR_STACK   VARCHAR(1000000),
    RECORD_TIME   BIGINT        NOT NULL
);

CREATE INDEX IF NOT EXISTS IDX_GRON_HIST_TASK_TIME ON GRON_RUN_HISTORY (TASK_ID, RECORD_TIME);
CREATE INDEX IF NOT EXISTS IDX_GRON_HIST_TIME ON GRON_RUN_HISTORY (RECORD_TIME);
