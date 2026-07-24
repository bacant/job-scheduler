package de.example.gron.store.memory

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
import de.example.gron.spi.SkipSink
import de.example.gron.spi.StoreContext
import de.example.gron.spi.TaskStore
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock

/**
 * In-memory {@link TaskStore}: the semantic reference implementation. Backed by
 * a {@code HashMap} of tasks plus a {@code TreeSet} index ordered by
 * ({@code nextRun}, id) so due lookups are logarithmic rather than a full scan.
 *
 * <p>Not persistent and not shared. Placement fallback aging is a shared-store
 * feature; on this single-node store a non-matching node selector simply skips
 * the occurrence with a warning.</p>
 *
 * <p><strong>Lock ordering:</strong> a single {@link ReentrantLock} guards all
 * mutable state and is a leaf lock — the store never invokes listeners,
 * handlers or the signaler while holding it. Skip notifications are collected
 * under the lock and dispatched to the {@link SkipSink} after releasing it, and
 * the signaler is called after unlocking. This prevents deadlocks between the
 * control loop, workers and the store.</p>
 */
@CompileStatic
class MemoryTaskStore implements TaskStore, DueTimeAware, AdHocRunSupport {

    private static final Logger log = LoggerFactory.getLogger(MemoryTaskStore)

    /** Per-task mutable state. */
    private static class Entry {
        Task task
        Instant nextRun            // cursor: planned time of the next occurrence, or null
        Instant previousRun
        boolean paused
        boolean failed
        Instant pendingWait        // deferred WAIT occurrence, or null
        boolean warnedNoNode
        final Map<String, Instant> activeClaims = new LinkedHashMap<>()

        int activeCount() { return activeClaims.size() }
    }

    private final Map<String, Entry> tasks = new HashMap<>()
    private final Comparator<Entry> byDue = new Comparator<Entry>() {
        @Override
        int compare(Entry a, Entry b) {
            int c = a.nextRun <=> b.nextRun
            return c != 0 ? c : (a.task.id <=> b.task.id)
        }
    }
    private final TreeSet<Entry> dueIndex = new TreeSet<>(byDue)
    private final Set<Entry> readyWait = new LinkedHashSet<>()
    private final ReentrantLock lock = new ReentrantLock()

    private Clock clock
    private SkipSink skipSink
    private de.example.gron.spi.Signaler signaler

    @Override
    void open(StoreContext context) {
        this.clock = context.clock
        this.skipSink = context.skipSink
        this.signaler = context.signaler
    }

    @Override
    void close() { }

    @Override
    boolean isPersistent() { return false }

    @Override
    boolean isShared() { return false }

    // ------------------------------------------------------------- CRUD

    @Override
    void save(Task task) {
        lock.lock()
        try {
            Entry entry = tasks.get(task.id)
            if (entry == null) {
                entry = new Entry()
                tasks.put(task.id, entry)
            } else {
                indexRemove(entry)
                readyWait.remove(entry)
                entry.activeClaims.clear()
                entry.pendingWait = null
            }
            entry.task = task
            entry.paused = task.startPaused
            entry.failed = false
            entry.previousRun = null
            entry.warnedNoNode = false
            entry.nextRun = task.schedule.nextRunAfter(clock.instant())
            indexAdd(entry)
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
            Entry e = tasks.get(taskId)
            return e?.task
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
            e.paused = paused
            if (paused) {
                indexRemove(e)
                readyWait.remove(e)
            } else {
                e.failed = false   // resume also clears a failed state
                indexAdd(e)
            }
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
        Instant now = clock.instant()
        lock.lock()
        try {
            // 1) Deferred WAIT runs that are now free to start.
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
            }

            // 2) Regular due occurrences (sorted; stop at the first future entry).
            List<Entry> dueEntries = new ArrayList<>()
            for (Entry e : dueIndex) {
                if (e.nextRun == null || e.nextRun.isAfter(until)) {
                    break
                }
                dueEntries.add(e)
            }
            for (Entry e : dueEntries) {
                processDue(e, until, limit, node, now, result, skips)
                if (result.size() >= limit) {
                    break
                }
            }
        } finally {
            lock.unlock()
        }
        for (Object[] s : skips) {
            skipSink?.skipped((String) s[0], (Instant) s[1], (SkipReason) s[2])
        }
        return result
    }

    /**
     * Processes one due entry, possibly claiming several successive occurrences
     * (PARALLEL) or skipping/deferring them (SKIP/WAIT/catch-up).
     */
    private void processDue(Entry e, Instant until, int limit, NodeInfo node,
                            Instant now, List<DueRun> result, List<Object[]> skips) {
        Task task = e.task
        Schedule schedule = task.schedule
        while (e.nextRun != null && !e.nextRun.isAfter(until) && result.size() < limit
                && !e.paused && !e.failed) {
            Instant planned = e.nextRun

            // Placement filter.
            if (!placementAllows(task, node, planned, now)) {
                if (!e.warnedNoNode) {
                    log.warn('Task {} has node selector {} not matched by node {}; ' +
                            'skipping occurrence at {} (single-node store)',
                            task.id, task.placement.nodeSelector, node.id, planned)
                    e.warnedNoNode = true
                }
                advance(e, schedule.nextRunAfter(planned))
                continue
            }
            e.warnedNoNode = false

            // Overlap policy when a run is already active.
            if (e.activeCount() > 0) {
                if (task.overlap == OverlapPolicy.SKIP) {
                    skips.add([task.id, planned, SkipReason.OVERLAP] as Object[])
                    advance(e, schedule.nextRunAfter(planned))
                    continue
                } else if (task.overlap == OverlapPolicy.WAIT) {
                    e.pendingWait = planned   // merge: keep the most recent due time
                    advance(e, schedule.nextRunAfter(planned))
                    continue
                }
                // PARALLEL falls through to claim.
            }

            // Catch-up policy for overdue occurrences.
            boolean overdue = Duration.between(planned, now).compareTo(task.overdueAfter) > 0
            Instant nextCursor
            if (overdue) {
                if (task.catchUp == CatchUpPolicy.SKIP) {
                    skips.add([task.id, planned, SkipReason.OVERDUE] as Object[])
                    advance(e, schedule.nextRunAfter(now))
                    continue
                } else if (task.catchUp == CatchUpPolicy.RUN_ONCE) {
                    nextCursor = schedule.nextRunAfter(now)   // one catch-up, then skip ahead
                } else {
                    nextCursor = schedule.nextRunAfter(planned) // RUN_ALL: one at a time
                }
            } else {
                nextCursor = schedule.nextRunAfter(planned)
            }

            result.add(claim(e, planned, node))
            advance(e, nextCursor)
        }
    }

    /** Records a claim on the entry and returns the corresponding {@link DueRun}. */
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
            // Make the released occurrence claimable again.
            if (e.nextRun == null || run.plannedTime.isBefore(e.nextRun)) {
                advance(e, run.plannedTime)
            }
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
            // The cursor was already advanced at claim time; the nextRun
            // parameter is advisory and intentionally not re-applied here.
            if (e.activeCount() == 0 && e.pendingWait != null && !e.paused && !e.failed) {
                readyWait.add(e)
                wake = true
            }
        } finally {
            lock.unlock()
        }
        if (wake) {
            signaler?.signal()
        }
    }

    @Override
    List<DueRun> reclaimFromDeadNode(String nodeId) {
        // Not a shared store: no foreign nodes to reclaim from.
        return Collections.<DueRun> emptyList()
    }

    // ------------------------------------------------------- capabilities

    @Override
    Instant nextDueTime() {
        lock.lock()
        try {
            if (!readyWait.isEmpty()) {
                return Instant.EPOCH   // a deferred run is ready immediately
            }
            Entry first = dueIndex.isEmpty() ? null : dueIndex.first()
            return first?.nextRun
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
            }
        } finally {
            lock.unlock()
        }
        signaler?.signal()
    }

    // ------------------------------------------------------------- index

    private void advance(Entry e, Instant newNextRun) {
        indexRemove(e)
        e.nextRun = newNextRun
        indexAdd(e)
    }

    private void indexAdd(Entry e) {
        if (!e.paused && !e.failed && e.nextRun != null) {
            dueIndex.add(e)
        }
    }

    private void indexRemove(Entry e) {
        dueIndex.remove(e)
    }

    // ---------------------------------------------------------- placement

    private boolean placementAllows(Task task, NodeInfo node, Instant planned, Instant now) {
        Placement p = task.placement
        if (p.mode == RunMode.EVERY_NODE) {
            return true   // runs locally on every node; no cluster claim
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
