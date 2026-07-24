package de.example.gron.cluster

import de.example.gron.api.ClusterLockTimeoutException
import de.example.gron.spi.ClusterCoordinator
import de.example.gron.spi.NodeInfo
import de.example.gron.util.TimeCodec
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Statement
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit

/**
 * JDBC-based {@link ClusterCoordinator} using the {@code GRON_NODE} and
 * {@code GRON_LOCK} tables. It maintains its own heartbeat (default every 7.5s)
 * and detects dead nodes when {@code now - lastSeen > interval * factor +
 * tolerance}. Cluster-wide locks are acquired via {@code SELECT ... FOR UPDATE}
 * on a {@code GRON_LOCK} row held for the lifetime of the returned handle.
 */
@CompileStatic
class JdbcClusterCoordinator implements ClusterCoordinator {

    private static final Logger log = LoggerFactory.getLogger(JdbcClusterCoordinator)

    private final DataSource dataSource
    private final String nodeId
    private final Set<String> tags
    private final Clock clock
    private final Duration heartbeatInterval
    private final double deadFactor
    private final Duration deadTolerance

    private Sql sql
    private ScheduledExecutorService heartbeat
    private final Set<String> reportedDead = new HashSet<>()

    JdbcClusterCoordinator(DataSource dataSource, String nodeId,
                           Set<String> tags = Collections.<String> emptySet(),
                           Clock clock = Clock.systemUTC(),
                           Duration heartbeatInterval = Duration.ofMillis(7500),
                           double deadFactor = 2.0d,
                           Duration deadTolerance = Duration.ofSeconds(1)) {
        this.dataSource = dataSource
        this.nodeId = nodeId
        this.tags = tags == null ? Collections.<String> emptySet() : new LinkedHashSet<>(tags)
        this.clock = clock
        this.heartbeatInterval = heartbeatInterval
        this.deadFactor = deadFactor
        this.deadTolerance = deadTolerance
    }

    @Override
    NodeInfo localNode() {
        return new NodeInfo(nodeId, tags, clock.instant())
    }

    @Override
    void start() {
        this.sql = new Sql(dataSource)
        writeHeartbeat()
        heartbeat = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            Thread newThread(Runnable r) {
                Thread t = new Thread(r, "gron-heartbeat-${nodeId}")
                t.setDaemon(true)
                return t
            }
        })
        long ms = Math.max(1L, heartbeatInterval.toMillis())
        heartbeat.scheduleWithFixedDelay({ safeHeartbeat() } as Runnable, ms, ms, TimeUnit.MILLISECONDS)
        log.info('Cluster coordinator started for node {} (tags={})', nodeId, tags)
    }

    @Override
    void stop() {
        heartbeat?.shutdownNow()
        try {
            sql?.executeUpdate('DELETE FROM GRON_NODE WHERE ID = ?', [nodeId])
        } catch (Exception e) {
            log.warn('Failed to deregister node {}: {}', nodeId, e.message)
        }
        try { sql?.close() } catch (Exception ignored) { }
        log.info('Cluster coordinator stopped for node {}', nodeId)
    }

    private void safeHeartbeat() {
        try {
            writeHeartbeat()
        } catch (Exception e) {
            log.warn('Heartbeat failed for node {}: {}', nodeId, e.message)
        }
    }

    private void writeHeartbeat() {
        Long now = TimeCodec.toNanos(clock.instant())
        String tagStr = tags.join(',')
        int updated = sql.executeUpdate(
                'UPDATE GRON_NODE SET TAGS = ?, LAST_SEEN = ? WHERE ID = ?', [tagStr, now, nodeId])
        if (updated == 0) {
            sql.executeUpdate('INSERT INTO GRON_NODE (ID, TAGS, LAST_SEEN) VALUES (?, ?, ?)',
                    [nodeId, tagStr, now])
        }
    }

    @Override
    List<NodeInfo> activeNodes() {
        long threshold = deadThresholdNanos()
        List<NodeInfo> result = new ArrayList<>()
        for (GroovyRowResult row : sql.rows(
                'SELECT ID, TAGS, LAST_SEEN FROM GRON_NODE WHERE LAST_SEEN > ?', [threshold])) {
            result.add(toNode(row))
        }
        return result
    }

    @Override
    List<NodeInfo> newlyDeadNodes() {
        long threshold = deadThresholdNanos()
        List<NodeInfo> dead = new ArrayList<>()
        Set<String> currentlyDead = new HashSet<>()
        for (GroovyRowResult row : sql.rows(
                'SELECT ID, TAGS, LAST_SEEN FROM GRON_NODE WHERE LAST_SEEN <= ? AND ID <> ?',
                [threshold, nodeId])) {
            NodeInfo n = toNode(row)
            currentlyDead.add(n.id)
            if (!reportedDead.contains(n.id)) {
                dead.add(n)
            }
        }
        // Allow a recovered node to be reported again if it dies later.
        reportedDead.retainAll(currentlyDead)
        for (NodeInfo n : dead) {
            reportedDead.add(n.id)
        }
        return dead
    }

    @Override
    AutoCloseable lock(String name, Duration timeout) {
        ensureLockRow(name)
        Connection conn = null
        try {
            conn = dataSource.getConnection()
            conn.setAutoCommit(false)
            Statement setTimeout = conn.createStatement()
            try {
                setTimeout.execute('SET LOCK_TIMEOUT ' + Math.max(0L, timeout.toMillis()))
            } finally {
                setTimeout.close()
            }
            PreparedStatement ps = conn.prepareStatement(
                    'SELECT NAME FROM GRON_LOCK WHERE NAME = ? FOR UPDATE')
            ps.setString(1, name)
            ps.executeQuery().close()
            ps.close()
            // Record holder (best-effort, within the same transaction).
            PreparedStatement upd = conn.prepareStatement(
                    'UPDATE GRON_LOCK SET HOLDER = ?, ACQUIRED_AT = ? WHERE NAME = ?')
            upd.setString(1, nodeId)
            upd.setLong(2, TimeCodec.toNanos(clock.instant()))
            upd.setString(3, name)
            upd.executeUpdate()
            upd.close()
            return lockHandle(conn, name)
        } catch (Exception e) {
            closeQuietly(conn)
            throw new ClusterLockTimeoutException(
                    "Could not acquire cluster lock '${name}' within ${timeout}", e)
        }
    }

    private AutoCloseable lockHandle(Connection conn, String name) {
        return new AutoCloseable() {
            @Override
            void close() {
                try {
                    conn.commit()
                } catch (Exception e) {
                    log.warn('Failed to release lock {}: {}', name, e.message)
                } finally {
                    closeQuietly(conn)
                }
            }
        }
    }

    private void ensureLockRow(String name) {
        int updated = sql.executeUpdate(
                'UPDATE GRON_LOCK SET NAME = NAME WHERE NAME = ?', [name])
        if (updated == 0) {
            try {
                sql.executeUpdate('INSERT INTO GRON_LOCK (NAME) VALUES (?)', [name])
            } catch (Exception ignored) {
                // Concurrent insert by another node; the row now exists.
            }
        }
    }

    private long deadThresholdNanos() {
        long window = (long) (heartbeatInterval.toMillis() * deadFactor) + deadTolerance.toMillis()
        Instant cutoff = clock.instant().minusMillis(window)
        return TimeCodec.toNanos(cutoff)
    }

    private static NodeInfo toNode(GroovyRowResult row) {
        String tagStr = (String) row.TAGS
        Set<String> tags = new LinkedHashSet<>()
        if (tagStr != null && !tagStr.isEmpty()) {
            for (String t : tagStr.split(',')) {
                if (!t.isEmpty()) {
                    tags.add(t)
                }
            }
        }
        return new NodeInfo((String) row.ID, tags, TimeCodec.fromNanos((Number) row.LAST_SEEN))
    }

    private static void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.setAutoCommit(true)
            } catch (Exception ignored) { }
            try {
                conn.close()
            } catch (Exception ignored) { }
        }
    }
}
