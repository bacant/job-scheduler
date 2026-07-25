package de.example.gron.store

import de.example.gron.api.CatchUpPolicy
import de.example.gron.api.NodeFallback
import de.example.gron.api.OverlapPolicy
import de.example.gron.api.Placement
import de.example.gron.api.RunMode
import de.example.gron.api.RunOutcome
import de.example.gron.api.SkipReason
import de.example.gron.api.Task
import de.example.gron.api.TaskState
import de.example.gron.schedule.Schedule
import de.example.gron.spi.AdHocRunSupport
import de.example.gron.spi.DueRun
import de.example.gron.spi.DueTimeAware
import de.example.gron.spi.NodeInfo
import de.example.gron.spi.StoreContext
import de.example.gron.spi.StoreStats
import de.example.gron.spi.TaskStore
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.locks.ReentrantLock

/**
 * Shared reference implementation of the overlap / catch-up / placement claim
 * semantics for single-node stores.
 *
 * <p><strong>Due index:</strong> a {@link ConcurrentSkipListMap} keyed by
 * ({@code plannedTime}, {@code taskId}) gives O(log n) submit/reschedule and an
 * ordered head for finding due runs — there is never a full scan per tick, so
 * throughput scales with the number of tasks.</p>
 *
 * <p>Subclasses supply persistence by overriding {@link #persist(Entry)},
 * {@link #removePersisted(String)} and {@link #loadOnOpen()}.</p>
 *
 * <p><strong>Lock ordering:</strong> a single {@link ReentrantLock} guards the
 * compound state transitions (claiming advances a cursor, mutates claim
 * bookkeeping and the index together). It is a leaf lock — the store never
 * invokes listeners, handlers or the signaler while holding it; skips are
 * collected under the lock and dispatched after releasing it. Critical sections
 * are kept short and are O(1)/O(log n), independent of task count.</p>
 */
@CompileStatic
abstract class AbstractInMemoryStore implements TaskStore, DueTimeAware, AdHocRunSupport, StoreStats {

    private static final Logger log = LoggerFactory.getLogger(AbstractInMemoryStore)

    /** Ordering key for the due index. */
    protected static class DueKey implements Comparable<DueKey> {
        final Instant plannedTime
        final String taskId

        DueKey(Instant plannedTime, String taskId) {
            this.plannedTime = plannedTime
            this.taskId = taskId
        }

        @Override
        int compareTo(DueKey o) {
            int c = plannedTime <=> o.plannedTime
            return c != 0 ? c : (taskId <=> o.taskId)
        }
    }

    /** Per-task mutable state. Active claims are runtime-only (never persisted). */
    protected static class Entry {
        Task task
        Instant nextRun
        Instant previousRun
        boolean paused
        boolean failed
        Instant pendingWait
        boolean warnedNoNode
        DueKey indexKey                 // current key in the due index, or null
        final Map<String, Instant> activeClaims = new LinkedHashMap<>()

        int activeCount() { return activeClaims.size() }
    }

    protected final Map<String, Entry> tasks = new HashMap<>()
    private final ConcurrentSkipListMap<DueKey, Entry> dueIndex = new ConcurrentSkipListMap<>()
    private final Set<Entry> readyWait = new LinkedHashSet<>()
    protected final ReentrantLock lock = new ReentrantLock()

    private int pausedCount = 0

    protected Clock clock
    protected de.example.gron.spi.SkipSink skipSink
    protected de.example.gron.spi.Signaler signaler
    protected StoreContext storeContext

    // --------------------------------------------------- subclass hooks

    protected void persist(Entry entry) { }

    protected void removePersisted(String taskId) { }

    protected void loadOnOpen() { }

    // -------------------------------------------------------- lifecycle

    @Override
    void open(StoreContext context) {
        this.storeContext = context
        this.clock = context.clock
        this.skipSink = context.skipSink
        this.signaler = context.signaler
        lock.lock()
        try {
            loadOnOpen()
            for (Entry e : tasks.values()) {
                e.activeClaims.clear()
                if (e.paused) {
                    pausedCount++
                }
                indexAdd(e)
            }
        } finally {
            lock.unlock()
        }
    }

    @Override
    void close() { }

    // ------------------------------------------------------------- CRUD

    @Override
    void save(Task task) {
        Entry entry
        lock.lock()
        try {
            entry = tasks.get(task.id)
            if (entry == null) {
                entry = new Entry()
                tasks.put(task.id, entry)
            } else {
                indexRemove(entry)
                readyWait.remove(entry)
                if (entry.paused) {
                    pausedCount--
                }
                entry.activeClaims.clear()
                entry.pendingWait = null
            }
            entry.task = task
            entry.paused = task.startPaused
            if (entry.paused) {
                pausedCount++
            }
            entry.failed = false
            entry.previousRun = null
            entry.warnedNoNode = false
            entry.nextRun = task.schedule.nextRunAfter(clock.instant())
            indexAdd(entry)
            persist(entry)
        } finally {
            lock.unlock()
        }
        signaler?.signal()
    }

    @Override
    boolean delete(String taskId) {
        lock.lock()
        try {
            Entry entry = tasks.remove(taskId)
            if (entry != null) {
                indexRemove(entry)
                readyWait.remove(entry)
                if (entry.paused) {
                    pausedCount--
                }
                removePersisted(taskId)
                return true
            }
            return false
        } finally {
            lock.unlock()
        }
    }

    @Override
    Task find(String taskId) {
        lock.lock()
        try {
            return tasks.get(taskId)?.task
        } finally {
            lock.unlock()
        }
    }

    @Override
    List<Task> findAll() {
        lock.lock()
        try {
            List<Task> result = new ArrayList<>(tasks.size())
            for (Entry e : tasks.values()) {
                result.add(e.task)
            }
            return result
        } finally {
            lock.unlock()
        }
    }

    @Override
    List<Task> findByTag(String tag) {
        lock.lock()
        try {
            List<Task> result = new ArrayList<>()
            for (Entry e : tasks.values()) {
                if (e.task.tags.contains(tag)) {
                    result.add(e.task)
                }
            }
            return result
        } finally {
            lock.unlock()
        }
    }

    @Override
    void setPaused(String taskId, boolean paused) {
        lock.lock()
        try {
            Entry e = tasks.get(taskId)
            if (e == null) {
                return
            }
            if (paused && !e.paused) {
                pausedCount++
            } else if (!paused && e.paused) {
                pausedCount--
            }
            e.paused = paused
            if (paused) {
                indexRemove(e)
                readyWait.remove(e)
            } else {
                e.failed = false
                indexAdd(e)
            }
            persist(e)
        } finally {
            lock.unlock()
        }
        signaler?.signal()
    }

    @Override
    void setFailed(String taskId) {
        lock.lock()
        try {
            Entry e = tasks.get(taskId)
            if (e == null) {
                return
            }
            e.failed = true
            e.pendingWait = null
            indexRemove(e)
            readyWait.remove(e)
            persist(e)
        } finally {
            lock.unlock()
        }
    }

    @Override
    TaskState stateOf(String taskId) {
        lock.lock()
        try {
            Entry e = tasks.get(taskId)
            if (e == null) {
                return null
            }
            if (e.failed) {
                return TaskState.FAILED
            }
            if (e.paused) {
                return TaskState.PAUSED
            }
            if (e.activeCount() > 0) {
                return TaskState.RUNNING
            }
            if (e.nextRun == null && e.pendingWait == null) {
                return TaskState.DONE
            }
            return TaskState.SCHEDULED
        } finally {
            lock.unlock()
        }
    }

    // --------------------------------------------------------- claiming

    @Override
    List<DueRun> claimDue(Instant until, int limit, NodeInfo node) {
        List<DueRun> result = new ArrayList<>()
        List<Object[]> skips = new ArrayList<>()
        Set<Entry> touched = new LinkedHashSet<>()
        Instant now = clock.instant()
        lock.lock()
        try {
            Iterator<Entry> wi = readyWait.iterator()
            while (wi.hasNext() && result.size() < limit) {
                Entry e = wi.next()
                if (e.paused || e.failed || e.pendingWait == null || e.activeCount() > 0) {
                    wi.remove()
                    continue
                }
                Instant planned = e.pendingWait
                e.pendingWait = null
                wi.remove()
                result.add(claim(e, planned, node))
                touched.add(e)
            }

            List<Entry> dueEntries = new ArrayList<>()
            for (Entry e : dueIndex.values()) {
                if (e.nextRun == null || e.nextRun.isAfter(until)) {
                    break
                }
                dueEntries.add(e)
                if (dueEntries.size() >= limit) {
                    break   // no need to look further than the claim limit
                }
            }
            for (Entry e : dueEntries) {
                processDue(e, until, limit, node, now, result, skips, touched)
                if (result.size() >= limit) {
                    break
                }
            }
            for (Entry e : touched) {
                persist(e)
            }
        } finally {
            lock.unlock()
        }
        for (Object[] s : skips) {
            skipSink?.skipped((String) s[0], (Instant) s[1], (SkipReason) s[2])
        }
        return result
    }

    private void processDue(Entry e, Instant until, int limit, NodeInfo node, Instant now,
                            List<DueRun> result, List<Object[]> skips, Set<Entry> touched) {
        Task task = e.task
        Schedule schedule = task.schedule
        while (e.nextRun != null && !e.nextRun.isAfter(until) && result.size() < limit
                && !e.paused && !e.failed) {
            Instant planned = e.nextRun

            if (!placementAllows(task, node, planned, now)) {
                if (!e.warnedNoNode) {
                    log.warn('Task {} has node selector {} not matched by node {}; ' +
                            'skipping occurrence at {} (single-node store)',
                            task.id, task.placement.nodeSelector, node.id, planned)
                    e.warnedNoNode = true
                }
                advance(e, schedule.nextRunAfter(planned))
                touched.add(e)
                continue
            }
            e.warnedNoNode = false

            if (e.activeCount() > 0) {
                if (task.overlap == OverlapPolicy.SKIP) {
                    skips.add([task.id, planned, SkipReason.OVERLAP] as Object[])
                    advance(e, schedule.nextRunAfter(planned))
                    touched.add(e)
                    continue
                } else if (task.overlap == OverlapPolicy.WAIT) {
                    e.pendingWait = planned
                    advance(e, schedule.nextRunAfter(planned))
                    touched.add(e)
                    continue
                }
            }

            boolean overdue = Duration.between(planned, now).compareTo(task.overdueAfter) > 0
            Instant nextCursor
            if (overdue) {
                if (task.catchUp == CatchUpPolicy.SKIP) {
                    skips.add([task.id, planned, SkipReason.OVERDUE] as Object[])
                    advance(e, schedule.nextRunAfter(now))
                    touched.add(e)
                    continue
                } else if (task.catchUp == CatchUpPolicy.RUN_ONCE) {
                    nextCursor = schedule.nextRunAfter(now)
                } else {
                    nextCursor = schedule.nextRunAfter(planned)
                }
            } else {
                nextCursor = schedule.nextRunAfter(planned)
            }

            result.add(claim(e, planned, node))
            advance(e, nextCursor)
            touched.add(e)
        }
    }

    private DueRun claim(Entry e, Instant planned, NodeInfo node) {
        String token = UUID.randomUUID().toString()
        e.activeClaims.put(token, planned)
        return new DueRun(e.task, planned, node.id, token)
    }

    @Override
    void release(DueRun run) {
        lock.lock()
        try {
            Entry e = tasks.get(run.task.id)
            if (e == null) {
                return
            }
            e.activeClaims.remove(run.claimToken)
            if (e.nextRun == null || run.plannedTime.isBefore(e.nextRun)) {
                advance(e, run.plannedTime)
            }
            persist(e)
        } finally {
            lock.unlock()
        }
        signaler?.signal()
    }

    @Override
    void complete(DueRun run, RunOutcome outcome, Instant nextRun) {
        boolean wake = false
        lock.lock()
        try {
            Entry e = tasks.get(run.task.id)
            if (e == null) {
                return
            }
            e.activeClaims.remove(run.claimToken)
            e.previousRun = run.plannedTime
            if (e.activeCount() == 0 && e.pendingWait != null && !e.paused && !e.failed) {
                readyWait.add(e)
                wake = true
            }
            persist(e)
        } finally {
            lock.unlock()
        }
        if (wake) {
            signaler?.signal()
        }
    }

    @Override
    List<DueRun> reclaimFromDeadNode(String nodeId) {
        return Collections.<DueRun> emptyList()
    }

    // ------------------------------------------------------- capabilities

    @Override
    Instant nextDueTime() {
        lock.lock()
        try {
            if (!readyWait.isEmpty()) {
                return Instant.EPOCH
            }
            Map.Entry<DueKey, Entry> first = dueIndex.firstEntry()
            return first == null ? null : first.key.plannedTime
        } finally {
            lock.unlock()
        }
    }

    @Override
    void triggerNow(String taskId) {
        lock.lock()
        try {
            Entry e = tasks.get(taskId)
            if (e == null || e.paused || e.failed) {
                return
            }
            Instant now = clock.instant()
            if (e.nextRun == null || now.isBefore(e.nextRun)) {
                advance(e, now)
                persist(e)
            }
        } finally {
            lock.unlock()
        }
        signaler?.signal()
    }

    @Override
    int taskCount() {
        lock.lock()
        try {
            return tasks.size()
        } finally {
            lock.unlock()
        }
    }

    @Override
    int pausedCount() {
        lock.lock()
        try {
            return pausedCount
        } finally {
            lock.unlock()
        }
    }

    @Override
    int backlogCount(Instant until) {
        lock.lock()
        try {
            int n = 0
            for (Entry e : dueIndex.values()) {
                if (e.nextRun == null || e.nextRun.isAfter(until)) {
                    break
                }
                n++
            }
            return n
        } finally {
            lock.unlock()
        }
    }

    // ------------------------------------------------------------- index

    private void advance(Entry e, Instant newNextRun) {
        indexRemove(e)
        e.nextRun = newNextRun
        indexAdd(e)
    }

    private void indexAdd(Entry e) {
        if (!e.paused && !e.failed && e.nextRun != null) {
            DueKey key = new DueKey(e.nextRun, e.task.id)
            e.indexKey = key
            dueIndex.put(key, e)
        } else {
            e.indexKey = null
        }
    }

    private void indexRemove(Entry e) {
        if (e.indexKey != null) {
            dueIndex.remove(e.indexKey)
            e.indexKey = null
        }
    }

    // ---------------------------------------------------------- placement

    private boolean placementAllows(Task task, NodeInfo node, Instant planned, Instant now) {
        Placement p = task.placement
        if (p.mode == RunMode.EVERY_NODE) {
            return true
        }
        if (!p.hasSelector()) {
            return true
        }
        if (selectorMatches(p, node)) {
            return true
        }
        if (p.fallback == NodeFallback.ANY_NODE
                && Duration.between(planned, now).compareTo(p.fallbackAfter) >= 0) {
            return true
        }
        return false
    }

    private boolean selectorMatches(Placement p, NodeInfo node) {
        String exact = p.selectorNodeId()
        if (exact != null) {
            return exact == node.id
        }
        String tag = p.selectorTag()
        return tag != null && node.hasTag(tag)
    }
}
