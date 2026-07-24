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
import de.example.gron.spi.TaskChangeEvent
import de.example.gron.spi.TaskStore
import de.example.gron.store.memory.MemoryTaskStore
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

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

/**
 * The default {@link TaskScheduler}. A single control thread claims due runs
 * from the {@link TaskStore} in batches and hands them to a worker pool; a
 * maintenance thread drives heartbeats and fail-over.
 *
 * <p><strong>Lock ordering:</strong> the scheduler never holds its own monitors
 * while calling into the store, and the store never calls back while holding its
 * lock (skips and signals are dispatched after unlocking). The wake-up monitor
 * ({@code signalMonitor}) is a leaf. This ordering — scheduler → store, with no
 * reverse edge — keeps the loop, workers and store deadlock-free.</p>
 *
 * <p>The worker pool is a plain {@link ThreadPoolExecutor} with a configurable
 * size and {@code gron-worker-N} thread names. A thread-pool SPI is
 * deliberately omitted: the pool has one job and a size knob covers it.</p>
 */
@CompileStatic
class GronScheduler implements TaskScheduler {

    private static final Logger log = LoggerFactory.getLogger(GronScheduler)

    private final String name
    private final String nodeId
    private final int workers
    private final int batchLimit
    private final TaskStore store
    private final ClusterCoordinator coordinator
    private final ReplicationProvider replication
    private final Serializer serializer
    private final HandlerFactory handlerFactory
    private final Clock clock
    private final ClassLoader classLoader
    private final Duration maintenanceInterval
    private final Duration maxSleep

    private final List<TaskListener> listeners = new CopyOnWriteArrayList<>()
    private final Map<String, TaskContext> activeContexts = new ConcurrentHashMap<>()
    private final Map<String, DueRun> inflight = new ConcurrentHashMap<>()
    private final AtomicLong sequence = new AtomicLong(0L)

    private volatile boolean running = false
    private Thread controlThread
    private ThreadPoolExecutor workerPool
    private ScheduledExecutorService retryExecutor
    private ScheduledExecutorService maintenanceExecutor

    private final Object signalMonitor = new Object()
    private boolean signalled = false

    protected GronScheduler(String name, String nodeId, int workers, int batchLimit,
                            TaskStore store, ClusterCoordinator coordinator,
                            ReplicationProvider replication, Serializer serializer,
                            HandlerFactory handlerFactory, Clock clock, ClassLoader classLoader,
                            Duration maintenanceInterval, Duration maxSleep) {
        this.name = name
        this.nodeId = nodeId
        this.workers = workers
        this.batchLimit = batchLimit
        this.store = store
        this.coordinator = coordinator
        this.replication = replication
        this.serializer = serializer
        this.handlerFactory = handlerFactory
        this.clock = clock
        this.classLoader = classLoader
        this.maintenanceInterval = maintenanceInterval
        this.maxSleep = maxSleep
    }

    /** @return a new scheduler builder. */
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
        Signaler signaler = new Signaler() {
            @Override
            void signal() { wakeLoop() }
        }
        SkipSink skipSink = new SkipSink() {
            @Override
            void skipped(String taskId, Instant plannedTime, SkipReason reason) {
                fireSkip(taskId, plannedTime, reason)
            }
        }
        store.open(new StoreContext(serializer, clock, classLoader, signaler, skipSink))
        coordinator.start()
        replication.start(new ReplicationContext(serializer, clock, nodeId))
        replication.onRemoteChange(new Consumer<TaskChangeEvent>() {
            @Override
            void accept(TaskChangeEvent event) { applyRemote(event) }
        })

        running = true
        this.workerPool = new ThreadPoolExecutor(workers, workers, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), namedFactory('gron-worker'))
        this.retryExecutor = new ScheduledThreadPoolExecutor(1, namedFactory('gron-retry'))
        this.maintenanceExecutor = new ScheduledThreadPoolExecutor(1, namedFactory('gron-maint'))

        this.controlThread = new Thread({ controlLoop() } as Runnable, "gron-loop-${name}")
        controlThread.setDaemon(true)
        controlThread.start()

        long ms = Math.max(1L, maintenanceInterval.toMillis())
        maintenanceExecutor.scheduleWithFixedDelay({ maintenance() } as Runnable, ms, ms,
                TimeUnit.MILLISECONDS)

        log.info('GronScheduler {} started as node {} with {} workers', name, nodeId, workers)
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

        if (awaitRunning == null || awaitRunning.isZero() || awaitRunning.isNegative()) {
            workerPool?.shutdownNow()
        } else {
            workerPool?.shutdown()
            try {
                if (!workerPool.awaitTermination(awaitRunning.toMillis(), TimeUnit.MILLISECONDS)) {
                    workerPool.shutdownNow()
                }
            } catch (InterruptedException ignored) {
                workerPool.shutdownNow()
                Thread.currentThread().interrupt()
            }
        }

        // Release any claims that never ran (or were interrupted).
        for (DueRun run : new ArrayList<DueRun>(inflight.values())) {
            try {
                store.release(run)
            } catch (Exception e) {
                log.warn('Failed to release claim {} on stop: {}', run.claimToken, e.message)
            }
        }
        inflight.clear()
        activeContexts.clear()

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

    // ------------------------------------------------------- control loop

    private void controlLoop() {
        while (running) {
            try {
                Instant now = clock.instant()
                NodeInfo node = coordinator.localNode()
                List<DueRun> due = store.claimDue(now, batchLimit, node)
                for (DueRun run : due) {
                    inflight.put(run.claimToken, run)
                    dispatch(run, 1)
                }
                if (due.size() >= batchLimit) {
                    continue   // batch was full; more work likely due — claim again now
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
                // A run is due but was not claimable by this node (e.g. a node
                // selector on a shared store). Sleep a short floor to avoid a
                // busy spin; a signal still wakes us immediately.
                waitMs = Math.min(maxSleep.toMillis(), 100L)
            } else {
                waitMs = Math.min(waitMs, maxSleep.toMillis())
            }
        }
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

    private void dispatch(DueRun run, int attempt) {
        try {
            workerPool.execute({ runOnce(run, attempt) } as Runnable)
        } catch (RejectedExecutionException e) {
            // Pool is shutting down; release the claim so it is not lost.
            inflight.remove(run.claimToken)
            try { store.release(run) } catch (Exception ignored) { }
        }
    }

    private void runOnce(DueRun run, int attempt) {
        Task task = run.task
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
        try {
            invokeHandler(task, ctx)
            activeContexts.remove(run.claimToken)
            inflight.remove(run.claimToken)
            store.complete(run, RunOutcome.OK, nextRun)
            fireAfterRun(ctx, RunOutcome.OK)
        } catch (Throwable t) {
            activeContexts.remove(run.claimToken)
            fireError(ctx, t)
            if (attempt <= task.maxRetries) {
                log.warn('Task {} attempt {} failed ({}); retrying in {}',
                        task.id, attempt, t.toString(), task.retryDelay)
                scheduleRetry(run, attempt + 1, task.retryDelay)
            } else {
                inflight.remove(run.claimToken)
                store.complete(run, RunOutcome.FAILED, nextRun)
                fireAfterRun(ctx, RunOutcome.FAILED)
                if (t instanceof TaskFailedException && ((TaskFailedException) t).abortSchedule) {
                    log.error('Task {} aborted its schedule (moved to FAILED)', task.id)
                    store.setFailed(task.id)
                } else {
                    log.error('Task {} failed after {} attempt(s)', task.id, attempt, t)
                }
            }
        }
    }

    private void invokeHandler(Task task, TaskContext ctx) {
        if (task.isClosureRunner()) {
            task.runner.call(ctx)
        } else {
            TaskHandler handler = handlerFactory.create(task.handlerType)
            handler.run(ctx)
        }
    }

    private void scheduleRetry(DueRun run, int nextAttempt, Duration delay) {
        try {
            retryExecutor.schedule({ dispatch(run, nextAttempt) } as Runnable,
                    Math.max(0L, delay.toMillis()), TimeUnit.MILLISECONDS)
        } catch (RejectedExecutionException e) {
            inflight.remove(run.claimToken)
            try { store.release(run) } catch (Exception ignored) { }
        }
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

    private void recoverNode(String deadNodeId) {
        log.warn('Detected dead node {}; running fail-over recovery', deadNodeId)
        AutoCloseable lock = null
        try {
            lock = coordinator.lock('recovery', Duration.ofSeconds(30))
            List<DueRun> recovered = store.reclaimFromDeadNode(deadNodeId)
            for (DueRun run : recovered) {
                inflight.put(run.claimToken, run)
                dispatch(run, 1)
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
            return   // ignore our own echoes
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
                    // EVERY_NODE nodes run locally; nothing to apply.
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

    /**
     * Fluent builder for {@link GronScheduler}. All configuration goes through
     * here (no properties machinery).
     */
    @CompileStatic
    static class Builder {
        // Package-visible (not private) so the enclosing constructor can read
        // them under @CompileStatic, mirroring the Task.Builder pattern.
        protected String name = 'gron'
        protected String nodeId = 'node-' + UUID.randomUUID().toString().substring(0, 8)
        protected Set<String> nodeTags = new LinkedHashSet<>()
        protected int workers = 4
        protected int batchLimit = 100
        protected TaskStore store
        protected ClusterCoordinator coordinator
        protected ReplicationProvider replication = new NoOpReplicationProvider()
        protected Serializer serializer = new JsonSerializer()
        protected HandlerFactory handlerFactory = new DefaultHandlerFactory()
        protected Clock clock = Clock.systemUTC()
        protected ClassLoader classLoader = Thread.currentThread().contextClassLoader
        protected Duration maintenanceInterval = Duration.ofSeconds(5)
        protected Duration maxSleep = Duration.ofSeconds(30)

        Builder name(String value) { this.name = value; return this }

        Builder nodeId(String value) { this.nodeId = value; return this }

        Builder nodeTags(Set<String> value) { this.nodeTags = new LinkedHashSet<>(value); return this }

        Builder workers(int value) { this.workers = value; return this }

        Builder batchLimit(int value) { this.batchLimit = value; return this }

        Builder store(TaskStore value) { this.store = value; return this }

        Builder coordinator(ClusterCoordinator value) { this.coordinator = value; return this }

        Builder replication(ReplicationProvider value) { this.replication = value; return this }

        Builder serializer(Serializer value) { this.serializer = value; return this }

        Builder handlerFactory(HandlerFactory value) { this.handlerFactory = value; return this }

        Builder clock(Clock value) { this.clock = value; return this }

        Builder classLoader(ClassLoader value) { this.classLoader = value; return this }

        Builder maintenanceInterval(Duration value) { this.maintenanceInterval = value; return this }

        Builder maxSleep(Duration value) { this.maxSleep = value; return this }

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
            if (workers < 1) {
                throw new IllegalArgumentException('workers must be at least 1')
            }
            return new GronScheduler(name, nodeId, workers, batchLimit, store, coordinator,
                    replication, serializer, handlerFactory, clock, classLoader,
                    maintenanceInterval, maxSleep)
        }
    }
}
