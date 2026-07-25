package de.example.gron.history

import de.example.gron.api.RunOutcome
import de.example.gron.api.SkipReason
import de.example.gron.api.TaskStoreException
import de.example.gron.spi.StoreContext
import de.example.gron.util.TimeCodec
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.time.Instant

/**
 * Persistent, centrally shared {@link HistoryStore} backed by JDBC (table
 * {@code GRON_RUN_HISTORY}). Inserts go through JDBC batches; {@code
 * deleteOlderThan} deletes in bounded chunks to avoid long locks.
 */
@CompileStatic
class JdbcHistoryStore implements HistoryStore, BatchHistoryStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcHistoryStore)
    private static final int DELETE_CHUNK = 5000

    private static final String INSERT = '''INSERT INTO GRON_RUN_HISTORY
        (RUN_ID, TASK_ID, TAGS, PLANNED_TIME, STARTED_AT, FINISHED_AT, DURATION_MS, NODE_ID,
         ATTEMPT, OUTCOME, SKIP_REASON, RECOVERED, ERROR_TYPE, ERROR_MESSAGE, ERROR_STACK,
         RECORD_TIME)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

    private final DataSource dataSource
    private final boolean autoCreate
    private Sql sql

    JdbcHistoryStore(DataSource dataSource, boolean autoCreate = false) {
        this.dataSource = dataSource
        this.autoCreate = autoCreate
    }

    @Override
    void open(StoreContext context) {
        this.sql = new Sql(dataSource)
        if (autoCreate) {
            createSchema()
        }
    }

    @Override
    void close() {
        try { sql?.close() } catch (Exception ignored) { }
    }

    /** Runs the bundled DDL (idempotent). */
    void createSchema() {
        InputStream input = JdbcHistoryStore.getResourceAsStream('/ddl/schema.sql')
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

    @Override
    void record(RunRecord record) {
        recordBatch(Collections.singletonList(record))
    }

    @Override
    void recordBatch(List<RunRecord> records) {
        if (records.isEmpty()) {
            return
        }
        try {
            sql.withTransaction {
                sql.withBatch(records.size(), INSERT) { ps ->
                    for (RunRecord r : records) {
                        Instant rt = RunRecordJson.recordTime(r)
                        ps.addBatch([
                                r.runId, r.taskId, wrapTags(r.tags),
                                TimeCodec.toNanos(r.plannedTime), TimeCodec.toNanos(r.startedAt),
                                TimeCodec.toNanos(r.finishedAt), r.durationMillis, r.nodeId,
                                r.attempt, r.outcome?.name(), r.skipReason?.name(), r.recovered,
                                r.errorType, r.errorMessage, r.errorStackTrace,
                                TimeCodec.toNanos(rt)
                        ])
                    }
                }
            }
        } catch (Exception e) {
            log.warn('Failed to batch-insert {} history record(s): {}', records.size(), e.message)
        }
    }

    @Override
    List<RunRecord> query(HistoryQuery query) {
        int limit = HistoryFilter.limit(query)
        StringBuilder where = new StringBuilder(' WHERE 1=1')
        List<Object> params = new ArrayList<>()
        if (query.taskId != null) {
            where.append(' AND TASK_ID = ?'); params.add(query.taskId)
        }
        if (query.tag != null) {
            where.append(' AND TAGS LIKE ?'); params.add('%,' + query.tag + ',%')
        }
        if (query.nodeId != null) {
            where.append(' AND NODE_ID = ?'); params.add(query.nodeId)
        }
        if (query.outcomes != null && !query.outcomes.isEmpty()) {
            List<String> names = new ArrayList<>()
            for (RunOutcome o : query.outcomes) {
                names.add("'" + o.name() + "'")
            }
            where.append(' AND OUTCOME IN (').append(names.join(',')).append(')')
        }
        if (query.from != null) {
            where.append(' AND RECORD_TIME >= ?'); params.add(TimeCodec.toNanos(query.from))
        }
        if (query.to != null) {
            where.append(' AND RECORD_TIME <= ?'); params.add(TimeCodec.toNanos(query.to))
        }
        if (query.startedBefore != null) {
            where.append(' AND RECORD_TIME < ?'); params.add(TimeCodec.toNanos(query.startedBefore))
        }
        String q = 'SELECT * FROM GRON_RUN_HISTORY' + where.toString() +
                ' ORDER BY RECORD_TIME DESC FETCH FIRST ' + limit + ' ROWS ONLY'
        List<RunRecord> result = new ArrayList<>()
        for (GroovyRowResult row : sql.rows(q, params)) {
            result.add(toRecord(row))
        }
        return result
    }

    @Override
    long deleteOlderThan(Instant cutoff) {
        long cutoffN = TimeCodec.toNanos(cutoff)
        long total = 0L
        while (true) {
            int affected = 0
            sql.withTransaction {
                affected = sql.executeUpdate('''DELETE FROM GRON_RUN_HISTORY WHERE RUN_ID IN
                        (SELECT RUN_ID FROM GRON_RUN_HISTORY WHERE RECORD_TIME < ?
                         FETCH FIRST ''' + DELETE_CHUNK + ' ROWS ONLY)', [cutoffN])
            }
            total += affected
            if (affected < DELETE_CHUNK) {
                break
            }
        }
        return total
    }

    private static RunRecord toRecord(GroovyRowResult row) {
        return new RunRecord([
                runId          : row.RUN_ID,
                taskId         : row.TASK_ID,
                tags           : unwrapTags((String) row.TAGS),
                plannedTime    : TimeCodec.fromNanos((Number) row.PLANNED_TIME),
                startedAt      : TimeCodec.fromNanos((Number) row.STARTED_AT),
                finishedAt     : TimeCodec.fromNanos((Number) row.FINISHED_AT),
                durationMillis : row.DURATION_MS,
                nodeId         : row.NODE_ID,
                attempt        : row.ATTEMPT,
                outcome        : row.OUTCOME == null ? null : RunOutcome.valueOf((String) row.OUTCOME),
                skipReason     : row.SKIP_REASON == null ? null : SkipReason.valueOf((String) row.SKIP_REASON),
                recovered      : row.RECOVERED,
                errorType      : row.ERROR_TYPE,
                errorMessage   : row.ERROR_MESSAGE,
                errorStackTrace: row.ERROR_STACK
        ])
    }

    private static String wrapTags(Set<String> tags) {
        if (tags == null || tags.isEmpty()) {
            return ','
        }
        return ',' + tags.join(',') + ','
    }

    private static Set<String> unwrapTags(String wrapped) {
        Set<String> tags = new LinkedHashSet<>()
        if (wrapped != null) {
            for (String t : wrapped.split(',')) {
                if (!t.isEmpty()) {
                    tags.add(t)
                }
            }
        }
        return tags
    }
}
