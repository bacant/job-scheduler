package de.example.gron.core

import de.example.gron.api.GronException
import de.example.gron.api.RunOutcome
import de.example.gron.api.RunMode
import de.example.gron.api.SkipReason
import de.example.gron.api.Task
import de.example.gron.api.TaskContext
import de.example.gron.api.TaskFailedException
import de.example.gron.api.TaskHandler
import de.example.gron.api.TaskListener
import de.example.gron.api.TaskScheduler
import de.example.gron.api.TaskState
import de.example.gron.api.UnsupportedPlacementException
import de.example.gron.cluster.SingleNodeCoordinator
import de.example.gron.history.AsyncHistoryWriter
import de.example.gron.history.HistoryMode
import de.example.gron.history.HistoryPolicy
import de.example.gron.history.HistoryQuery
import de.example.gron.history.HistoryStore
import de.example.gron.history.MemoryHistoryStore
import de.example.gron.history.RunRecord
import de.example.gron.metrics.MetricCounter
import de.example.gron.metrics.MetricTimer
import de.example.gron.metrics.MetricsCollector
import de.example.gron.metrics.MetricsSnapshot
import de.example.gron.metrics.ReadableMetrics
import de.example.gron.metrics.SimpleMetrics
import de.example.gron.replication.NoOpReplicationProvider
import de.example.gron.spi.AdHocRunSupport
import de.example.gron.spi.ClusterCoordinator
import de.example.gron.spi.DueRun
import de.example.gron.spi.DueTimeAware
import de.example.gron.spi.HandlerFactory
import de.example.gron.spi.NodeInfo
import de.example.gron.spi.ReplicationContext
import de.example.gron.spi.ReplicationProvider
import de.example.gron.spi.Serializer
import de.example.gron.spi.Signaler
import de.example.gron.spi.SkipSink
import de.example.gron.spi.StoreContext
import de.example.gron.spi.StoreStats
import de.example.gron.spi.TaskChangeEvent
import de.example.gron.spi.TaskStore
import de.example.gron.store.memory.MemoryTaskStore
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.PrintWriter
import java.io.StringWriter
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer
import java.util.function.Supplier

/**
 * The default {@link TaskScheduler}. A single control thread claims due runs
 * from the {@link TaskStore} in batches and hands them to a bounded worker pool;
 * a maintenance thread drives heartbeats, fail-over and history retention.
 *
 * <p><strong>Throughput.</strong> The loop never claims more than the free
 * worker capacity ({@code claimBatch} capped by open slots), so claims are never
 * hoarded. History is written asynchronously and metrics use lock-free adders,
 * so neither slows the run path. Hot-path methods are {@code @CompileStatic}.</p>
 *
 * <p><strong>Lock ordering:</strong> scheduler → store, with no reverse edge;
 * the store never calls back while holding its lock. Listeners run inline and
 * must be fast.</p>
 */
@CompileStatic
class GronScheduler implements TaskScheduler {

    private static final Logger log = LoggerFactory.getLogger(GronScheduler)

    private final String name
    private final String nodeId
    private final int workers
    private final int claimBatch
    private final int workerQueue
    private final int maxInflight
    private final TaskStore store
    private final ClusterCoordinator coordinator
    private final ReplicationProvider replication
    private final Serializer serializer
    private final HandlerFactory handlerFactory
    private final Clock clock
    private final ClassLoader classLoader
    private final Duration maintenanceInterval
    private final Duration maxSleep
    private final MetricsCollector metrics
    private final HistoryStore historyStore
    private final HistoryPolicy historyPolicy

    private final List<TaskListener> listeners = new CopyOnWriteArrayList<>()
    private final Map<String, TaskContext> activeContexts = new ConcurrentHashMap<>()
    private final Map<String, DueRun> inflight = new ConcurrentHashMap<>()
    private final AtomicLong sequence = new AtomicLong(0L)

    private AsyncHistoryWriter historyWriter

    // Cached hot-path metric handles (tag values that never change per scheduler).
    private MetricCounter mStarted
    private MetricCounter mCompletedOk
    private MetricCounter mCompletedFailed
    private MetricCounter mRetried
    private MetricCounter mRecovered
    private MetricCounter mSkipOverlap
    private MetricCounter mSkipOverdue
    private MetricTimer mDurationOk
    private MetricTimer mDurationFailed
    private MetricTimer mStoreClaim
    private MetricTimer mStoreComplete

    private volatile boolean running = false
    private Thread controlThread
    private ThreadPoolExecutor workerPool
    private ScheduledExecutorService retryExecutor
    private ScheduledExecutorService maintenanceExecutor

    private final Object signalMonitor = new Object()
    private boolean signalled = false

    protected GronScheduler(String name, String nodeId, int workers, int claimBatch, int workerQueue,
                            TaskStore store, ClusterCoordinator coordinator,
                            ReplicationProvider replication, Serializer serializer,
                            HandlerFactory handlerFactory, Clock clock, ClassLoader classLoader,
                            Duration maintenanceInterval, Duration maxSleep,
                            MetricsCollector metrics, HistoryStore historyStore,
                            HistoryPolicy historyPolicy) {
        this.name = name
        this.nodeId = nodeId
        this.workers = workers
        this.claimBatch = claimBatch
        this.workerQueue = workerQueue
        this.maxInflight = workers + workerQueue
        this.store = store
        this.coordinator = coordinator
        this.replication = replication
        this.serializer = serializer
        this.handlerFactory = handlerFactory
        this.clock = clock
        this.classLoader = classLoader
        this.maintenanceInterval = maintenanceInterval
        this.maxSleep = maxSleep
        this.metrics = metrics
        this.historyStore = historyStore
        this.historyPolicy = historyPolicy
    }

    static Builder builder() {
        return new Builder()
    }

    @Override
    String getName() { return name }

    @Override
    String getNodeId() { return nodeId }

    // ------------------------------------------------------- lifecycle

    @Override
    synchronized void start() {
        if (running) {
            return
        }
        cacheMetricHandles()

        Signaler signaler = new Signaler() {
            @Override
            void signal() { wakeLoop() }
        }
        SkipSink skipSink = new SkipSink() {
            @Override
            void skipped(String taskId, Instant plannedTime, SkipReason reason) {
                onSkipped(taskId, plannedTime, reason)
            }
        }
        StoreContext ctx = new StoreContext(serializer, clock, classLoader, signaler, skipSink, metrics)
        store.open(ctx)
        historyStore.open(ctx)
        historyWriter = new AsyncHistoryWriter(historyStore, historyPolicy, metrics)
        historyWriter.start()

        coordinator.start()
        replication.start(new ReplicationContext(serializer, clock, nodeId))
        replication.onRemoteChange(new Consumer<TaskChangeEvent>() {
            @Override
            void accept(TaskChangeEvent event) { applyRemote(event) }
        })

        running = true
        this.workerPool = new ThreadPoolExecutor(workers, workers, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(Math.max(1, workerQueue)), namedFactory('gron-worker'))
        this.retryExecutor = new ScheduledThreadPoolExecutor(1, namedFactory('gron-retry'))
        this.maintenanceExecutor = new ScheduledThreadPoolExecutor(1, namedFactory('gron-maint'))

        registerGauges()

        this.controlThread = new Thread({ controlLoop() } as Runnable, "gron-loop-${name}")
        controlThread.setDaemon(true)
        controlThread.start()

        long ms = Math.max(1L, maintenanceInterval.toMillis())
        maintenanceExecutor.scheduleWithFixedDelay({ maintenance() } as Runnable, ms, ms,
                TimeUnit.MILLISECONDS)
        long hk = Math.max(1000L, historyPolicy.housekeepingInterval.toMillis())
        maintenanceExecutor.scheduleWithFixedDelay({ housekeeping() } as Runnable, hk, hk,
                TimeUnit.MILLISECONDS)

        log.info('GronScheduler {} started as node {} with {} workers (claimBatch={}, queue={})',
                name, nodeId, workers, claimBatch, workerQueue)
    }

    @Override
    boolean isRunning() { return running }

    @Override
    void stop(Duration awaitRunning) {
        synchronized (this) {
            if (!running) {
                return
            }
            running = false
        }
        wakeLoop()
        try {
            controlThread?.join(Math.max(1000L, maxSleep.toMillis()))
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt()
        }
        maintenanceExecutor?.shutdownNow()
        retryExecutor?.shutdownNow()

        long awaitMs = awaitRunning == null ? 0L : awaitRunning.toMillis()
        if (awaitRunning == null || awaitRunning.isZero() || awaitRunning.isNegative()) {
            workerPool?.shutdownNow()
        } else {
            workerPool?.shutdown()
            try {
                if (!workerPool.awaitTermination(awaitMs, TimeUnit.MILLISECONDS)) {
                    workerPool.shutdownNow()
                }
            } catch (InterruptedException ignored) {
                workerPool.shutdownNow()
                Thread.currentThread().interrupt()
            }
        }

        for (DueRun run : new ArrayList<DueRun>(inflight.values())) {
            try {
                store.release(run)
            } catch (Exception e) {
                log.warn('Failed to release claim {} on stop: {}', run.claimToken, e.message)
            }
        }
        inflight.clear()
        activeContexts.clear()

        // Drain the history queue up to the stop timeout, then close.
        historyWriter?.stop(Math.max(1000L, awaitMs))
        try { historyStore.close() } catch (Exception ignored) { }

        try { replication.stop() } catch (Exception ignored) { }
        try { coordinator.stop() } catch (Exception ignored) { }
        try { store.close() } catch (Exception ignored) { }
        log.info('GronScheduler {} stopped', name)
    }

    // --------------------------------------------------------- mutations

    @Override
    void submit(Task task) {
        if (task.placement.mode == RunMode.EVERY_NODE && !replication.isReal()) {
            throw new UnsupportedPlacementException(
                    "Task '${task.id}' uses EVERY_NODE placement, which requires an active, " +
                            'real ReplicationProvider so every node learns the task definition; ' +
                            'the configured provider is a no-op')
        }
        if (task.isClosureRunner() && store.isPersistent()) {
            throw new GronException(
                    "Task '${task.id}' uses a closure runner, which is only supported by the " +
                            'in-memory store (it cannot be persisted)')
        }
        store.save(task)
        publish(TaskChangeEvent.Type.TASK_SAVED, task)
        wakeLoop()
    }

    @Override
    boolean cancel(String taskId) {
        boolean removed = store.delete(taskId)
        if (removed) {
            publishId(TaskChangeEvent.Type.TASK_DELETED, taskId)
        }
        return removed
    }

    @Override
    void pause(String taskId) {
        store.setPaused(taskId, true)
        publishId(TaskChangeEvent.Type.TASK_PAUSED, taskId)
    }

    @Override
    void resume(String taskId) {
        store.setPaused(taskId, false)
        publishId(TaskChangeEvent.Type.TASK_RESUMED, taskId)
        wakeLoop()
    }

    @Override
    void runNow(String taskId) {
        if (store instanceof AdHocRunSupport) {
            ((AdHocRunSupport) store).triggerNow(taskId)
            wakeLoop()
        } else {
            throw new GronException(
                    "The configured store does not support ad-hoc runNow for task '${taskId}'")
        }
    }

    @Override
    Task find(String taskId) { return store.find(taskId) }

    @Override
    List<Task> findAll() { return store.findAll() }

    @Override
    List<Task> findByTag(String tag) { return store.findByTag(tag) }

    @Override
    TaskState stateOf(String taskId) { return store.stateOf(taskId) }

    @Override
    List<TaskContext> activeRuns() { return new ArrayList<TaskContext>(activeContexts.values()) }

    @Override
    void addListener(TaskListener listener) { listeners.add(listener) }

    @Override
    void removeListener(TaskListener listener) { listeners.remove(listener) }

    // --------------------------------------------------------- history/metrics

    @Override
    List<RunRecord> historyOf(String taskId, int limit) {
        HistoryQuery q = new HistoryQuery()
        q.taskId = taskId
        q.limit = limit
        return historyStore.query(q)
    }

    @Override
    List<RunRecord> queryHistory(HistoryQuery query) {
        return historyStore.query(query)
    }

    @Override
    MetricsSnapshot metricsSnapshot() {
        if (metrics instanceof ReadableMetrics) {
            return ((ReadableMetrics) metrics).snapshot()
        }
        return MetricsSnapshot.empty()
    }

    // ------------------------------------------------------- control loop

    private void controlLoop() {
        while (running) {
            try {
                int capacity = maxInflight - inflight.size()
                if (capacity <= 0) {
                    // Fully saturated: do not hoard claims; wait for capacity.
                    parkFor(Math.min(maxSleep.toMillis(), 50L))
                    continue
                }
                int limit = Math.min(claimBatch, capacity)
                Instant now = clock.instant()
                NodeInfo node = coordinator.localNode()
                long t0 = System.nanoTime()
                List<DueRun> due = store.claimDue(now, limit, node)
                mStoreClaim.record(System.nanoTime() - t0)
                for (DueRun run : due) {
                    inflight.put(run.claimToken, run)
                    dispatch(run, 1, false)
                }
                if (due.size() >= limit && (maxInflight - inflight.size()) > 0) {
                    continue   // full batch and still capacity: claim again now
                }
                sleepUntilNextDue()
            } catch (Throwable t) {
                if (running) {
                    log.error('Control loop iteration failed', t)
                }
                quietSleep(100L)
            }
        }
    }

    private void sleepUntilNextDue() {
        Instant next = (store instanceof DueTimeAware) ? ((DueTimeAware) store).nextDueTime() : null
        long waitMs
        if (next == null) {
            waitMs = maxSleep.toMillis()
        } else {
            waitMs = Duration.between(clock.instant(), next).toMillis()
            if (waitMs <= 0L) {
                waitMs = Math.min(maxSleep.toMillis(), 100L)
            } else {
                waitMs = Math.min(waitMs, maxSleep.toMillis())
            }
        }
        parkFor(waitMs)
    }

    private void parkFor(long waitMs) {
        synchronized (signalMonitor) {
            if (!signalled && waitMs > 0L) {
                try {
                    signalMonitor.wait(waitMs)
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt()
                }
            }
            signalled = false
        }
    }

    private void wakeLoop() {
        synchronized (signalMonitor) {
            signalled = true
            signalMonitor.notifyAll()
        }
    }

    // ------------------------------------------------------- execution

    private void dispatch(DueRun run, int attempt, boolean recovered) {
        try {
            workerPool.execute({ runOnce(run, attempt, recovered) } as Runnable)
        } catch (RejectedExecutionException e) {
            inflight.remove(run.claimToken)
            try { store.release(run) } catch (Exception ignored) { }
        }
    }

    private void runOnce(DueRun run, int attempt, boolean recovered) {
        Task task = run.task
        if (recovered && attempt == 1) {
            mRecovered.increment()
        }
        mStarted.increment()
        recordTaskTagCounter('gron.runs.started', task, null)

        Instant started = clock.instant()
        Instant nextRun = task.schedule.nextRunAfter(run.plannedTime)
        TaskContext ctx = new TaskContext([
                taskId     : task.id,
                params     : task.params,
                plannedTime: run.plannedTime,
                startedAt  : started,
                previousRun: null,
                nextRun    : nextRun,
                attempt    : attempt,
                nodeId     : nodeId,
                scheduler  : this
        ])
        activeContexts.put(run.claimToken, ctx)
        fireBeforeRun(ctx)
        long t0 = System.nanoTime()
        try {
            invokeHandler(task, ctx)
            long elapsed = System.nanoTime() - t0
            activeContexts.remove(run.claimToken)
            inflight.remove(run.claimToken)
            completeStore(run, RunOutcome.OK)
            mCompletedOk.increment()
            mDurationOk.record(elapsed)
            recordTaskTagCounter('gron.runs.completed', task, 'ok')
            recordTaskTagTimer(task, 'ok', elapsed)
            recordHistory(task, run, ctx, started, RunOutcome.OK, attempt, recovered, null)
            fireAfterRun(ctx, RunOutcome.OK)
        } catch (Throwable err) {
            long elapsed = System.nanoTime() - t0
            activeContexts.remove(run.claimToken)
            mCompletedFailed.increment()
            mDurationFailed.record(elapsed)
            recordTaskTagCounter('gron.runs.completed', task, 'failed')
            recordTaskTagTimer(task, 'failed', elapsed)
            recordHistory(task, run, ctx, started, RunOutcome.FAILED, attempt, recovered, err)
            fireError(ctx, err)
            if (attempt <= task.maxRetries) {
                mRetried.increment()
                log.warn('Task {} attempt {} failed ({}); retrying in {}',
                        task.id, attempt, err.toString(), task.retryDelay)
                scheduleRetry(run, attempt + 1, recovered, task.retryDelay)
            } else {
                inflight.remove(run.claimToken)
                completeStore(run, RunOutcome.FAILED)
                fireAfterRun(ctx, RunOutcome.FAILED)
                if (err instanceof TaskFailedException && ((TaskFailedException) err).abortSchedule) {
                    log.error('Task {} aborted its schedule (moved to FAILED)', task.id)
                    store.setFailed(task.id)
                } else {
                    log.error('Task {} failed after {} attempt(s)', task.id, attempt, err)
                }
            }
        }
    }

    private void completeStore(DueRun run, RunOutcome outcome) {
        long t0 = System.nanoTime()
        store.complete(run, outcome, run.task.schedule.nextRunAfter(run.plannedTime))
        mStoreComplete.record(System.nanoTime() - t0)
    }

    private void invokeHandler(Task task, TaskContext ctx) {
        if (task.isClosureRunner()) {
            task.runner.call(ctx)
        } else {
            TaskHandler handler = handlerFactory.create(task.handlerType)
            handler.run(ctx)
        }
    }

    private void scheduleRetry(DueRun run, int nextAttempt, boolean recovered, Duration delay) {
        try {
            retryExecutor.schedule({ dispatch(run, nextAttempt, recovered) } as Runnable,
                    Math.max(0L, delay.toMillis()), TimeUnit.MILLISECONDS)
        } catch (RejectedExecutionException e) {
            inflight.remove(run.claimToken)
            try { store.release(run) } catch (Exception ignored) { }
        }
    }

    // ------------------------------------------------------- skip handling

    private void onSkipped(String taskId, Instant plannedTime, SkipReason reason) {
        if (reason == SkipReason.OVERLAP) {
            mSkipOverlap.increment()
        } else {
            mSkipOverdue.increment()
        }
        Task task = store.find(taskId)
        recordSkipHistory(task, taskId, plannedTime, reason)
        fireSkip(taskId, plannedTime, reason)
    }

    // ------------------------------------------------------- history

    private HistoryMode effectiveMode(Task task) {
        if (task != null && task.historyMode != null) {
            return task.historyMode
        }
        return historyPolicy.mode
    }

    private boolean shouldRecord(HistoryMode mode, RunOutcome outcome) {
        if (mode == HistoryMode.OFF) {
            return false
        }
        if (mode == HistoryMode.ALL) {
            return true
        }
        return outcome == RunOutcome.FAILED || outcome == RunOutcome.SKIPPED
    }

    private void recordHistory(Task task, DueRun run, TaskContext ctx, Instant started,
                               RunOutcome outcome, int attempt, boolean recovered, Throwable err) {
        if (historyWriter == null || !shouldRecord(effectiveMode(task), outcome)) {
            return
        }
        Instant finished = clock.instant()
        Map<String, Object> args = new LinkedHashMap<>()
        args.put('runId', run.claimToken)
        args.put('taskId', task.id)
        args.put('tags', task.tags)
        args.put('plannedTime', run.plannedTime)
        args.put('startedAt', started)
        args.put('finishedAt', finished)
        args.put('durationMillis', Duration.between(started, finished).toMillis())
        args.put('nodeId', nodeId)
        args.put('attempt', attempt)
        args.put('outcome', outcome)
        args.put('recovered', recovered)
        if (err != null) {
            args.put('errorType', err.getClass().name)
            args.put('errorMessage', truncate(err.message, 1000))
            args.put('errorStackTrace', truncate(stackTrace(err), historyPolicy.stackTraceLimit))
        }
        historyWriter.submit(new RunRecord(args))
    }

    private void recordSkipHistory(Task task, String taskId, Instant plannedTime, SkipReason reason) {
        if (historyWriter == null || !shouldRecord(effectiveMode(task), RunOutcome.SKIPPED)) {
            return
        }
        Map<String, Object> args = new LinkedHashMap<>()
        args.put('runId', UUID.randomUUID().toString())
        args.put('taskId', taskId)
        args.put('tags', task == null ? ([] as Set) : task.tags)
        args.put('plannedTime', plannedTime)
        args.put('nodeId', nodeId)
        args.put('attempt', 1)
        args.put('outcome', RunOutcome.SKIPPED)
        args.put('skipReason', reason)
        historyWriter.submit(new RunRecord(args))
    }

    // ------------------------------------------------------- metrics helpers

    private void cacheMetricHandles() {
        Map<String, String> nodeTag = Collections.singletonMap('node', nodeId)
        mStarted = metrics.counter('gron.runs.started', nodeTag)
        mCompletedOk = metrics.counter('gron.runs.completed', outcomeNode('ok'))
        mCompletedFailed = metrics.counter('gron.runs.completed', outcomeNode('failed'))
        mRetried = metrics.counter('gron.runs.retried', Collections.<String, String> emptyMap())
        mRecovered = metrics.counter('gron.runs.recovered', Collections.<String, String> emptyMap())
        mSkipOverlap = metrics.counter('gron.runs.skipped', Collections.singletonMap('reason', 'overlap'))
        mSkipOverdue = metrics.counter('gron.runs.skipped', Collections.singletonMap('reason', 'overdue'))
        mDurationOk = metrics.timer('gron.run.duration', Collections.singletonMap('outcome', 'ok'))
        mDurationFailed = metrics.timer('gron.run.duration', Collections.singletonMap('outcome', 'failed'))
        mStoreClaim = metrics.timer('gron.store.claim.duration', Collections.<String, String> emptyMap())
        mStoreComplete = metrics.timer('gron.store.complete.duration', Collections.<String, String> emptyMap())
    }

    private Map<String, String> outcomeNode(String outcome) {
        Map<String, String> m = new LinkedHashMap<>()
        m.put('outcome', outcome)
        m.put('node', nodeId)
        return m
    }

    private void recordTaskTagCounter(String name, Task task, String outcome) {
        if (!task.metricsTaskTag) {
            return
        }
        Map<String, String> tags = new LinkedHashMap<>()
        tags.put('node', nodeId)
        if (outcome != null) {
            tags.put('outcome', outcome)
        }
        tags.put('task', task.id)
        metrics.counter(name, tags).increment()
    }

    private void recordTaskTagTimer(Task task, String outcome, long nanos) {
        if (!task.metricsTaskTag) {
            return
        }
        Map<String, String> tags = new LinkedHashMap<>()
        tags.put('outcome', outcome)
        tags.put('task', task.id)
        metrics.timer('gron.run.duration', tags).record(nanos)
    }

    private void registerGauges() {
        Map<String, String> none = Collections.<String, String> emptyMap()
        metrics.gauge('gron.runs.active', none, { (Number) activeContexts.size() } as Supplier<Number>)
        metrics.gauge('gron.runs.overdue', none, {
            (store instanceof StoreStats) ? (Number) ((StoreStats) store).backlogCount(clock.instant()) : (Number) 0
        } as Supplier<Number>)
        metrics.gauge('gron.tasks.total', none, {
            (store instanceof StoreStats) ? (Number) ((StoreStats) store).taskCount() : (Number) store.findAll().size()
        } as Supplier<Number>)
        metrics.gauge('gron.tasks.paused', none, {
            (store instanceof StoreStats) ? (Number) ((StoreStats) store).pausedCount() : (Number) 0
        } as Supplier<Number>)
        metrics.gauge('gron.workers.busy', none, {
            (Number) (workerPool == null ? 0 : workerPool.getActiveCount())
        } as Supplier<Number>)
        metrics.gauge('gron.queue.depth', none, {
            (Number) (workerPool == null ? 0 : workerPool.getQueue().size())
        } as Supplier<Number>)
        metrics.gauge('gron.cluster.nodes.active', none, {
            (Number) coordinator.activeNodes().size()
        } as Supplier<Number>)
    }

    // ------------------------------------------------------- maintenance

    private void maintenance() {
        try {
            List<NodeInfo> dead = coordinator.newlyDeadNodes()
            for (NodeInfo node : dead) {
                recoverNode(node.id)
            }
        } catch (Throwable t) {
            log.warn('Maintenance cycle failed: {}', t.toString())
        }
    }

    private void housekeeping() {
        try {
            Duration retention = historyPolicy.retention
            if (retention != null && !retention.isZero() && !retention.isNegative()) {
                Instant cutoff = clock.instant().minus(retention)
                long deleted = historyStore.deleteOlderThan(cutoff)
                if (deleted > 0) {
                    log.debug('History retention deleted {} record(s) older than {}', deleted, cutoff)
                }
            }
        } catch (Throwable t) {
            log.warn('History housekeeping failed: {}', t.toString())
        }
    }

    private void recoverNode(String deadNodeId) {
        log.warn('Detected dead node {}; running fail-over recovery', deadNodeId)
        AutoCloseable lock = null
        try {
            lock = coordinator.lock('recovery', Duration.ofSeconds(30))
            List<DueRun> recovered = store.reclaimFromDeadNode(deadNodeId)
            for (DueRun run : recovered) {
                inflight.put(run.claimToken, run)
                dispatch(run, 1, true)
            }
            if (!recovered.isEmpty()) {
                log.warn('Recovered {} recoverable run(s) from dead node {}',
                        recovered.size(), deadNodeId)
            }
        } catch (Exception e) {
            log.warn('Recovery for dead node {} failed: {}', deadNodeId, e.message)
        } finally {
            if (lock != null) {
                try { lock.close() } catch (Exception ignored) { }
            }
        }
        wakeLoop()
    }

    // ------------------------------------------------------- replication

    private void publish(TaskChangeEvent.Type type, Task task) {
        if (!replication.isReal()) {
            return
        }
        if (task.isClosureRunner()) {
            log.debug('Skipping replication of closure-runner task {}', task.id)
            return
        }
        try {
            String payload = serializer.taskToJson(task)
            replication.publish(new TaskChangeEvent(type, payload, sequence.incrementAndGet(), nodeId))
        } catch (Exception e) {
            log.warn('Failed to publish {} for task {}: {}', type, task.id, e.message)
        }
    }

    private void publishId(TaskChangeEvent.Type type, String taskId) {
        if (!replication.isReal()) {
            return
        }
        replication.publish(new TaskChangeEvent(type, taskId, sequence.incrementAndGet(), nodeId))
    }

    private void applyRemote(TaskChangeEvent event) {
        if (event.originNodeId == nodeId) {
            return
        }
        try {
            switch (event.type) {
                case TaskChangeEvent.Type.TASK_SAVED:
                    store.save(serializer.taskFromJson(event.payload, classLoader))
                    wakeLoop()
                    break
                case TaskChangeEvent.Type.TASK_DELETED:
                    store.delete(event.payload)
                    break
                case TaskChangeEvent.Type.TASK_PAUSED:
                    store.setPaused(event.payload, true)
                    break
                case TaskChangeEvent.Type.TASK_RESUMED:
                    store.setPaused(event.payload, false)
                    wakeLoop()
                    break
                case TaskChangeEvent.Type.RUN_COMPLETED:
                    break
            }
        } catch (Exception e) {
            log.warn('Failed to apply remote change {}: {}', event, e.message)
        }
    }

    // ------------------------------------------------------- listeners

    private void fireBeforeRun(TaskContext ctx) {
        for (TaskListener l : listeners) {
            try { l.beforeRun(ctx) } catch (Exception e) { warnListener('beforeRun', e) }
        }
    }

    private void fireAfterRun(TaskContext ctx, RunOutcome outcome) {
        for (TaskListener l : listeners) {
            try { l.afterRun(ctx, outcome) } catch (Exception e) { warnListener('afterRun', e) }
        }
    }

    private void fireError(TaskContext ctx, Throwable error) {
        for (TaskListener l : listeners) {
            try { l.onError(ctx, error) } catch (Exception e) { warnListener('onError', e) }
        }
    }

    private void fireSkip(String taskId, Instant plannedTime, SkipReason reason) {
        for (TaskListener l : listeners) {
            try { l.onSkip(taskId, plannedTime, reason) } catch (Exception e) { warnListener('onSkip', e) }
        }
    }

    private void warnListener(String callback, Exception e) {
        log.warn('Listener {} threw {}: {}', callback, e.getClass().simpleName, e.message)
    }

    // ------------------------------------------------------- utils

    private static String stackTrace(Throwable t) {
        StringWriter sw = new StringWriter()
        t.printStackTrace(new PrintWriter(sw))
        return sw.toString()
    }

    private static String truncate(String s, int limit) {
        if (s == null) {
            return null
        }
        return s.length() > limit ? s.substring(0, limit) : s
    }

    private void quietSleep(long ms) {
        try {
            Thread.sleep(ms)
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt()
        }
    }

    private static ThreadFactory namedFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger(0)
        return new ThreadFactory() {
            @Override
            Thread newThread(Runnable r) {
                Thread t = new Thread(r, "${prefix}-${counter.incrementAndGet()}")
                t.setDaemon(true)
                return t
            }
        }
    }

    /** Fluent builder for {@link GronScheduler}. */
    @CompileStatic
    static class Builder {
        protected String name = 'gron'
        protected String nodeId = 'node-' + UUID.randomUUID().toString().substring(0, 8)
        protected Set<String> nodeTags = new LinkedHashSet<>()
        protected int workers = 4
        protected int claimBatch = 100
        protected int workerQueue = -1                 // default: workers * 2
        protected TaskStore store
        protected ClusterCoordinator coordinator
        protected ReplicationProvider replication = new NoOpReplicationProvider()
        protected Serializer serializer = new JsonSerializer()
        protected HandlerFactory handlerFactory = new DefaultHandlerFactory()
        protected Clock clock = Clock.systemUTC()
        protected ClassLoader classLoader = Thread.currentThread().contextClassLoader
        protected Duration maintenanceInterval = Duration.ofSeconds(5)
        protected Duration maxSleep = Duration.ofSeconds(30)
        protected MetricsCollector metrics = new SimpleMetrics()
        protected HistoryStore historyStore
        protected HistoryPolicy historyPolicy = new HistoryPolicy()

        Builder name(String value) { this.name = value; return this }

        Builder nodeId(String value) { this.nodeId = value; return this }

        Builder nodeTags(Set<String> value) { this.nodeTags = new LinkedHashSet<>(value); return this }

        Builder workers(int value) { this.workers = value; return this }

        Builder claimBatch(int value) { this.claimBatch = value; return this }

        Builder workerQueue(int value) { this.workerQueue = value; return this }

        Builder store(TaskStore value) { this.store = value; return this }

        Builder coordinator(ClusterCoordinator value) { this.coordinator = value; return this }

        Builder replication(ReplicationProvider value) { this.replication = value; return this }

        Builder serializer(Serializer value) { this.serializer = value; return this }

        Builder handlerFactory(HandlerFactory value) { this.handlerFactory = value; return this }

        Builder clock(Clock value) { this.clock = value; return this }

        Builder classLoader(ClassLoader value) { this.classLoader = value; return this }

        Builder maintenanceInterval(Duration value) { this.maintenanceInterval = value; return this }

        Builder maxSleep(Duration value) { this.maxSleep = value; return this }

        Builder metrics(MetricsCollector value) { this.metrics = value; return this }

        Builder history(HistoryStore value) { this.historyStore = value; return this }

        Builder historyPolicy(HistoryPolicy value) { this.historyPolicy = value; return this }

        GronScheduler build() {
            if (store == null) {
                store = new MemoryTaskStore()
            }
            if (classLoader == null) {
                classLoader = GronScheduler.classLoader
            }
            if (coordinator == null) {
                coordinator = new SingleNodeCoordinator(nodeId, nodeTags, clock)
            }
            if (metrics == null) {
                metrics = new SimpleMetrics()
            }
            if (historyPolicy == null) {
                historyPolicy = new HistoryPolicy()
            }
            if (historyStore == null) {
                historyStore = new MemoryHistoryStore(historyPolicy.buffer)
            }
            if (workers < 1) {
                throw new IllegalArgumentException('workers must be at least 1')
            }
            int queue = workerQueue > 0 ? workerQueue : workers * 2
            return new GronScheduler(name, nodeId, workers, claimBatch, queue, store, coordinator,
                    replication, serializer, handlerFactory, clock, classLoader,
                    maintenanceInterval, maxSleep, metrics, historyStore, historyPolicy)
        }
    }
}
