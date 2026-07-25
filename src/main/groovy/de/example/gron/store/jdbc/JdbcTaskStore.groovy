package de.example.gron.store.jdbc

import de.example.gron.api.CatchUpPolicy
import de.example.gron.api.NodeFallback
import de.example.gron.api.OverlapPolicy
import de.example.gron.api.Placement
import de.example.gron.api.RunMode
import de.example.gron.api.RunOutcome
import de.example.gron.api.SkipReason
import de.example.gron.api.Task
import de.example.gron.api.TaskState
import de.example.gron.api.TaskStoreException
import de.example.gron.schedule.Schedule
import de.example.gron.spi.AdHocRunSupport
import de.example.gron.spi.DueRun
import de.example.gron.spi.DueTimeAware
import de.example.gron.spi.NodeInfo
import de.example.gron.spi.Signaler
import de.example.gron.spi.StoreContext
import de.example.gron.spi.StoreStats
import de.example.gron.spi.TaskStore
import de.example.gron.util.TimeCodec
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.time.Clock
import java.time.Duration
import java.time.Instant

/**
 * A persistent and <strong>shared</strong> {@link TaskStore} backed by JDBC.
 *
 * <p><strong>Optimistic claim path.</strong> In normal operation claims do not
 * take the global {@code GRON_LOCK}. Candidate task rows are read without a
 * lock; the claim decision (overlap, catch-up, placement, cursor advance) is
 * computed in memory, then applied with a conditional
 * {@code UPDATE ... WHERE ID = ? AND NEXT_RUN <=> observed AND PENDING_WAIT <=>
 * observed} — a compare-and-set on the schedule cursor. Because the UPDATE takes
 * the row's write lock, concurrent claimers on the same row serialize and only
 * one CAS succeeds, giving cluster-wide exactly-once without a global lock.
 * {@code GRON_LOCK} is used only for recovery/housekeeping.</p>
 */
@CompileStatic
class JdbcTaskStore implements TaskStore, DueTimeAware, AdHocRunSupport, StoreStats {

    private static final Logger log = LoggerFactory.getLogger(JdbcTaskStore)

    private final DataSource dataSource
    private final boolean autoCreate
    private Sql sql
    private StoreContext ctx
    private Clock clock
    private Signaler signaler

    JdbcTaskStore(DataSource dataSource, boolean autoCreate = false) {
        this.dataSource = dataSource
        this.autoCreate = autoCreate
    }

    @Override
    void open(StoreContext context) {
        this.ctx = context
        this.clock = context.clock
        this.signaler = context.signaler
        this.sql = new Sql(dataSource)
        if (autoCreate) {
            createSchema()
        }
    }

    @Override
    void close() {
        try { sql?.close() } catch (Exception ignored) { }
    }

    @Override
    boolean isPersistent() { return true }

    @Override
    boolean isShared() { return true }

    void createSchema() {
        InputStream input = JdbcTaskStore.getResourceAsStream('/ddl/schema.sql')
        if (input == null) {
            throw new TaskStoreException('DDL resource /ddl/schema.sql not found on classpath')
        }
        String script = stripComments(input.getText('UTF-8'))
        for (String raw : script.split(';')) {
            String stmt = raw.trim()
            if (!stmt.isEmpty()) {
                sql.execute(stmt)
            }
        }
    }

    private static String stripComments(String block) {
        StringBuilder sb = new StringBuilder()
        for (String line : block.split('\n')) {
            if (!line.trim().startsWith('--')) {
                sb.append(line).append('\n')
            }
        }
        return sb.toString()
    }

    // ------------------------------------------------------------- CRUD

    @Override
    void save(Task task) {
        String defJson = ctx.serializer.taskToJson(task)
        String tags = wrapTags(task.tags)
        Long nextRun = TimeCodec.toNanos(task.schedule.nextRunAfter(clock.instant()))
        sql.withTransaction {
            GroovyRowResult existing = sql.firstRow('SELECT ID FROM GRON_TASK WHERE ID = ?', [task.id])
            if (existing != null) {
                sql.executeUpdate('DELETE FROM GRON_RUN WHERE TASK_ID = ?', [task.id])
                sql.executeUpdate('''UPDATE GRON_TASK SET DEFINITION = ?, TAGS = ?, NEXT_RUN = ?,
                        PREVIOUS_RUN = NULL, PENDING_WAIT = NULL, PAUSED = ?, FAILED = FALSE
                        WHERE ID = ?''', [defJson, tags, nextRun, task.startPaused, task.id])
            } else {
                sql.executeUpdate('''INSERT INTO GRON_TASK
                        (ID, DEFINITION, TAGS, NEXT_RUN, PREVIOUS_RUN, PENDING_WAIT, PAUSED, FAILED)
                        VALUES (?, ?, ?, ?, NULL, NULL, ?, FALSE)''',
                        [task.id, defJson, tags, nextRun, task.startPaused])
            }
        }
        signaler?.signal()
    }

    @Override
    boolean delete(String taskId) {
        int rows = 0
        sql.withTransaction {
            rows = sql.executeUpdate('DELETE FROM GRON_TASK WHERE ID = ?', [taskId])
            sql.executeUpdate('DELETE FROM GRON_RUN WHERE TASK_ID = ?', [taskId])
        }
        return rows > 0
    }

    @Override
    Task find(String taskId) {
        GroovyRowResult row = sql.firstRow('SELECT DEFINITION FROM GRON_TASK WHERE ID = ?', [taskId])
        return row == null ? null : taskFrom(row.DEFINITION)
    }

    @Override
    List<Task> findAll() {
        List<Task> result = new ArrayList<>()
        for (GroovyRowResult row : sql.rows('SELECT DEFINITION FROM GRON_TASK')) {
            result.add(taskFrom(row.DEFINITION))
        }
        return result
    }

    @Override
    List<Task> findByTag(String tag) {
        List<Task> result = new ArrayList<>()
        for (GroovyRowResult row : sql.rows('SELECT DEFINITION FROM GRON_TASK WHERE TAGS LIKE ?',
                ['%,' + tag + ',%'])) {
            result.add(taskFrom(row.DEFINITION))
        }
        return result
    }

    @Override
    void setPaused(String taskId, boolean paused) {
        if (paused) {
            sql.executeUpdate('UPDATE GRON_TASK SET PAUSED = TRUE WHERE ID = ?', [taskId])
        } else {
            sql.executeUpdate('UPDATE GRON_TASK SET PAUSED = FALSE, FAILED = FALSE WHERE ID = ?',
                    [taskId])
        }
        signaler?.signal()
    }

    @Override
    void setFailed(String taskId) {
        sql.executeUpdate('UPDATE GRON_TASK SET FAILED = TRUE, PENDING_WAIT = NULL WHERE ID = ?',
                [taskId])
    }

    @Override
    TaskState stateOf(String taskId) {
        GroovyRowResult row = sql.firstRow('''SELECT PAUSED, FAILED, NEXT_RUN, PENDING_WAIT,
                (SELECT COUNT(*) FROM GRON_RUN WHERE TASK_ID = ?) AS ACTIVE
                FROM GRON_TASK WHERE ID = ?''', [taskId, taskId])
        if (row == null) {
            return null
        }
        if (Boolean.TRUE == row.FAILED) {
            return TaskState.FAILED
        }
        if (Boolean.TRUE == row.PAUSED) {
            return TaskState.PAUSED
        }
        if (((Number) row.ACTIVE).longValue() > 0) {
            return TaskState.RUNNING
        }
        if (row.NEXT_RUN == null && row.PENDING_WAIT == null) {
            return TaskState.DONE
        }
        return TaskState.SCHEDULED
    }

    // --------------------------------------------------------- claiming

    /** In-memory plan produced for a candidate before the CAS. */
    private static class Plan {
        Long newNext
        Long newPending
        final List<Long> claims = new ArrayList<>()          // planned times (nanos)
        final List<Object[]> skips = new ArrayList<>()       // [taskId, Instant, SkipReason]
        boolean changed
    }

    @Override
    List<DueRun> claimDue(Instant until, int limit, NodeInfo node) {
        List<DueRun> result = new ArrayList<>()
        List<Object[]> firedSkips = new ArrayList<>()
        Instant now = clock.instant()
        Long untilN = TimeCodec.toNanos(until)

        List<GroovyRowResult> candidates = sql.rows('''SELECT ID, DEFINITION, NEXT_RUN, PENDING_WAIT
                FROM GRON_TASK
                WHERE PAUSED = FALSE AND FAILED = FALSE
                  AND ((NEXT_RUN IS NOT NULL AND NEXT_RUN <= ?) OR PENDING_WAIT IS NOT NULL)
                ORDER BY NEXT_RUN NULLS FIRST
                FETCH FIRST ''' + Math.max(1, limit) + ' ROWS ONLY', [untilN])

        for (GroovyRowResult row : candidates) {
            if (result.size() >= limit) {
                break
            }
            String id = (String) row.ID
            Task task = taskFrom(row.DEFINITION)
            Long observedNext = asLong(row.NEXT_RUN)
            Long observedPending = asLong(row.PENDING_WAIT)
            int active = activeCount(id)
            Plan plan = planCandidate(task, node, observedNext, observedPending, active, untilN, now,
                    limit - result.size())
            if (!plan.changed) {
                continue
            }
            // Apply atomically via compare-and-set on the schedule cursor. If the
            // CAS loses (another node advanced the same row first), discard the
            // plan entirely — no claims inserted, no skips fired.
            List<DueRun> claimed = new ArrayList<>()
            boolean won = false
            sql.withTransaction {
                int updated = casUpdate(id, observedNext, observedPending, plan.newNext, plan.newPending)
                if (updated == 1) {
                    won = true
                    for (Long plannedN : plan.claims) {
                        String token = UUID.randomUUID().toString()
                        insertClaim(token, id, plannedN, node.id, now, task.recoverable)
                        claimed.add(new DueRun(task, TimeCodec.fromNanos(plannedN), node.id, token))
                    }
                }
            }
            if (won) {
                result.addAll(claimed)
                firedSkips.addAll(plan.skips)
            }
        }

        for (Object[] s : firedSkips) {
            ctx.skipSink?.skipped((String) s[0], (Instant) s[1], (SkipReason) s[2])
        }
        return result
    }

    /**
     * Applies the compare-and-set update on the cursor columns.
     * @return number of rows updated (1 if this claimer won, 0 on contention)
     */
    private int casUpdate(String id, Long observedNext, Long observedPending,
                          Long newNext, Long newPending) {
        StringBuilder sb = new StringBuilder(
                'UPDATE GRON_TASK SET NEXT_RUN = ?, PENDING_WAIT = ? WHERE ID = ?')
        List<Object> params = new ArrayList<>()
        params.add(newNext)
        params.add(newPending)
        params.add(id)
        if (observedNext == null) {
            sb.append(' AND NEXT_RUN IS NULL')
        } else {
            sb.append(' AND NEXT_RUN = ?'); params.add(observedNext)
        }
        if (observedPending == null) {
            sb.append(' AND PENDING_WAIT IS NULL')
        } else {
            sb.append(' AND PENDING_WAIT = ?'); params.add(observedPending)
        }
        return sql.executeUpdate(sb.toString(), params)
    }

    /** Mirrors the in-memory reference semantics without touching the database. */
    private Plan planCandidate(Task task, NodeInfo node, Long observedNext, Long observedPending,
                               int active, Long untilN, Instant now, int remaining) {
        Schedule schedule = task.schedule
        Plan plan = new Plan()
        Long nextN = observedNext
        Long pendingN = observedPending
        int localActive = active
        int slots = remaining

        // Deferred WAIT run ready to start.
        if (pendingN != null && localActive == 0 && slots > 0) {
            plan.claims.add(pendingN)
            pendingN = null
            localActive++
            slots--
            plan.changed = true
        }

        while (nextN != null && nextN <= untilN && slots > 0) {
            Instant planned = TimeCodec.fromNanos(nextN)

            if (!placementAllows(task, node, planned, now)) {
                break   // leave for a matching node
            }

            if (localActive > 0) {
                if (task.overlap == OverlapPolicy.SKIP) {
                    plan.skips.add([task.id, planned, SkipReason.OVERLAP] as Object[])
                    nextN = TimeCodec.toNanos(schedule.nextRunAfter(planned))
                    plan.changed = true
                    continue
                } else if (task.overlap == OverlapPolicy.WAIT) {
                    pendingN = nextN
                    nextN = TimeCodec.toNanos(schedule.nextRunAfter(planned))
                    plan.changed = true
                    continue
                }
            }

            boolean overdue = Duration.between(planned, now).compareTo(task.overdueAfter) > 0
            Long cursor
            if (overdue) {
                if (task.catchUp == CatchUpPolicy.SKIP) {
                    plan.skips.add([task.id, planned, SkipReason.OVERDUE] as Object[])
                    nextN = TimeCodec.toNanos(schedule.nextRunAfter(now))
                    plan.changed = true
                    continue
                } else if (task.catchUp == CatchUpPolicy.RUN_ONCE) {
                    cursor = TimeCodec.toNanos(schedule.nextRunAfter(now))
                } else {
                    cursor = TimeCodec.toNanos(schedule.nextRunAfter(planned))
                }
            } else {
                cursor = TimeCodec.toNanos(schedule.nextRunAfter(planned))
            }

            plan.claims.add(nextN)
            localActive++
            slots--
            nextN = cursor
            plan.changed = true
        }

        plan.newNext = nextN
        plan.newPending = pendingN
        return plan
    }

    private void insertClaim(String token, String taskId, Long plannedN, String nodeId, Instant now,
                             boolean recoverable) {
        sql.executeUpdate('''INSERT INTO GRON_RUN
                (CLAIM_TOKEN, TASK_ID, PLANNED_TIME, CLAIMED_BY, CLAIMED_AT, RECOVERABLE)
                VALUES (?, ?, ?, ?, ?, ?)''',
                [token, taskId, plannedN, nodeId, TimeCodec.toNanos(now), recoverable])
    }

    private int activeCount(String taskId) {
        GroovyRowResult r = sql.firstRow('SELECT COUNT(*) AS C FROM GRON_RUN WHERE TASK_ID = ?',
                [taskId])
        return ((Number) r.C).intValue()
    }

    @Override
    void release(DueRun run) {
        Long plannedN = TimeCodec.toNanos(run.plannedTime)
        sql.withTransaction {
            sql.executeUpdate('DELETE FROM GRON_RUN WHERE CLAIM_TOKEN = ?', [run.claimToken])
            sql.executeUpdate('''UPDATE GRON_TASK SET NEXT_RUN =
                    CASE WHEN NEXT_RUN IS NULL OR ? < NEXT_RUN THEN ? ELSE NEXT_RUN END
                    WHERE ID = ?''', [plannedN, plannedN, run.task.id])
        }
        signaler?.signal()
    }

    @Override
    void complete(DueRun run, RunOutcome outcome, Instant nextRun) {
        Long prev = TimeCodec.toNanos(run.plannedTime)
        sql.withTransaction {
            sql.executeUpdate('DELETE FROM GRON_RUN WHERE CLAIM_TOKEN = ?', [run.claimToken])
            sql.executeUpdate('UPDATE GRON_TASK SET PREVIOUS_RUN = ? WHERE ID = ?',
                    [prev, run.task.id])
        }
        signaler?.signal()
    }

    @Override
    List<DueRun> reclaimFromDeadNode(String nodeId) {
        List<DueRun> recovered = new ArrayList<>()
        sql.withTransaction {
            List<GroovyRowResult> rows = sql.rows('''SELECT R.CLAIM_TOKEN, R.TASK_ID,
                    R.PLANNED_TIME, R.RECOVERABLE, T.DEFINITION
                    FROM GRON_RUN R JOIN GRON_TASK T ON T.ID = R.TASK_ID
                    WHERE R.CLAIMED_BY = ? FOR UPDATE''', [nodeId])
            for (GroovyRowResult row : rows) {
                Task task = taskFrom(row.DEFINITION)
                Long plannedN = asLong(row.PLANNED_TIME)
                if (Boolean.TRUE == row.RECOVERABLE) {
                    recovered.add(new DueRun(task, TimeCodec.fromNanos(plannedN), nodeId,
                            (String) row.CLAIM_TOKEN))
                } else {
                    sql.executeUpdate('DELETE FROM GRON_RUN WHERE CLAIM_TOKEN = ?',
                            [row.CLAIM_TOKEN])
                    sql.executeUpdate('''UPDATE GRON_TASK SET NEXT_RUN =
                            CASE WHEN NEXT_RUN IS NULL OR ? < NEXT_RUN THEN ? ELSE NEXT_RUN END
                            WHERE ID = ?''', [plannedN, plannedN, task.id])
                }
            }
        }
        return recovered
    }

    // ------------------------------------------------------- capabilities

    @Override
    Instant nextDueTime() {
        GroovyRowResult pending = sql.firstRow('''SELECT COUNT(*) AS C FROM GRON_TASK T
                WHERE T.PENDING_WAIT IS NOT NULL AND T.PAUSED = FALSE AND T.FAILED = FALSE
                  AND NOT EXISTS (SELECT 1 FROM GRON_RUN R WHERE R.TASK_ID = T.ID)''')
        if (((Number) pending.C).longValue() > 0) {
            return Instant.EPOCH
        }
        GroovyRowResult row = sql.firstRow('''SELECT MIN(NEXT_RUN) AS M FROM GRON_TASK
                WHERE PAUSED = FALSE AND FAILED = FALSE AND NEXT_RUN IS NOT NULL''')
        return row == null ? null : TimeCodec.fromNanos(asLong(row.M))
    }

    @Override
    void triggerNow(String taskId) {
        Long nowN = TimeCodec.toNanos(clock.instant())
        sql.executeUpdate('''UPDATE GRON_TASK SET NEXT_RUN =
                CASE WHEN NEXT_RUN IS NULL OR ? < NEXT_RUN THEN ? ELSE NEXT_RUN END
                WHERE ID = ? AND PAUSED = FALSE AND FAILED = FALSE''', [nowN, nowN, taskId])
        signaler?.signal()
    }

    @Override
    int taskCount() {
        return count('SELECT COUNT(*) AS C FROM GRON_TASK')
    }

    @Override
    int pausedCount() {
        return count('SELECT COUNT(*) AS C FROM GRON_TASK WHERE PAUSED = TRUE')
    }

    @Override
    int backlogCount(Instant until) {
        GroovyRowResult r = sql.firstRow('''SELECT COUNT(*) AS C FROM GRON_TASK
                WHERE PAUSED = FALSE AND FAILED = FALSE AND NEXT_RUN IS NOT NULL AND NEXT_RUN <= ?''',
                [TimeCodec.toNanos(until)])
        return ((Number) r.C).intValue()
    }

    private int count(String query) {
        GroovyRowResult r = sql.firstRow(query)
        return ((Number) r.C).intValue()
    }

    // -------------------------------------------------------------- utils

    private Task taskFrom(Object definition) {
        return ctx.serializer.taskFromJson(String.valueOf(definition), ctx.classLoader)
    }

    private static Long asLong(Object value) {
        return value == null ? null : ((Number) value).longValue()
    }

    private static String wrapTags(Set<String> tags) {
        if (tags == null || tags.isEmpty()) {
            return ','
        }
        return ',' + tags.join(',') + ','
    }

    private boolean placementAllows(Task task, NodeInfo node, Instant planned, Instant now) {
        Placement p = task.placement
        if (p.mode == RunMode.EVERY_NODE) {
            return true
        }
        if (!p.hasSelector()) {
            return true
        }
        String exact = p.selectorNodeId()
        if (exact != null && exact == node.id) {
            return true
        }
        String tag = p.selectorTag()
        if (tag != null && node.hasTag(tag)) {
            return true
        }
        if (p.fallback == NodeFallback.ANY_NODE
                && Duration.between(planned, now).compareTo(p.fallbackAfter) >= 0) {
            return true
        }
        return false
    }
}
