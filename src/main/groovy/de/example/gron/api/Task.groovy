package de.example.gron.api

import de.example.gron.history.HistoryMode
import de.example.gron.schedule.CronSchedule
import de.example.gron.schedule.IntervalSchedule
import de.example.gron.schedule.OneTimeSchedule
import de.example.gron.schedule.Schedule
import groovy.transform.CompileStatic

import java.time.Duration
import java.time.Instant
import java.time.ZoneId

/**
 * The central GronScheduler entity. A {@code Task} is a single immutable value
 * object that unites work (a {@link TaskHandler} or a closure runner), a
 * {@link Schedule}, parameters and run options. There are no separate
 * job/detail/trigger entities and no 1:n relation between work and schedules —
 * to run one handler on two schedules, define two tasks.
 *
 * <p>Create tasks with the DSL:</p>
 * <pre>
 * Task nightly = Task.define('nightly-report') {
 *     tags 'reports', 'billing'
 *     handler ReportHandler
 *     params region: 'EU', maxRows: 500
 *     cron '0 30 6 * * *', ZoneId.of('Europe/Berlin')
 *     overlap OverlapPolicy.SKIP
 *     catchUp CatchUpPolicy.RUN_ONCE
 *     retries 3, Duration.ofSeconds(10)
 *     recoverable()
 * }
 * </pre>
 */
@CompileStatic
class Task implements Serializable {

    private static final long serialVersionUID = 1L

    /** Required, unique task id. */
    final String id

    /** Optional labels used for filtering. */
    final Set<String> tags

    /** Handler class instantiated per run, or {@code null} if a runner is set. */
    final Class<? extends TaskHandler> handlerType

    /** In-process closure runner (memory store only), or {@code null}. */
    final Closure runner

    /** Task parameters, exposed to the handler as an immutable view. */
    final Map<String, Object> params

    /** When the task runs. */
    final Schedule schedule

    /** Overlap behaviour; default {@link OverlapPolicy#SKIP}. */
    final OverlapPolicy overlap

    /** Catch-up behaviour for overdue runs; default {@link CatchUpPolicy#SKIP}. */
    final CatchUpPolicy catchUp

    /** How long after {@code plannedTime} a run counts as overdue; default 60s. */
    final Duration overdueAfter

    /** Maximum retries after a failing run; default 0. */
    final int maxRetries

    /** Delay between retries; only relevant if {@code maxRetries > 0}; default 10s. */
    final Duration retryDelay

    /** Whether an interrupted run is re-run after a node failure; default false. */
    final boolean recoverable

    /** Where in the cluster the task runs; default exclusive on any node. */
    final Placement placement

    /** Whether the task starts paused; default false. */
    final boolean startPaused

    /** Per-task history mode override; {@code null} = use the global policy. */
    final HistoryMode historyMode

    /** Whether run metrics for this task carry an extra {@code task} tag; default false. */
    final boolean metricsTaskTag

    private Task(Builder b) {
        this.id = b.id
        this.tags = Collections.unmodifiableSet(new LinkedHashSet<String>(b.tags))
        this.handlerType = b.handlerType
        this.runner = b.runner
        this.params = Collections.unmodifiableMap(new LinkedHashMap<String, Object>(b.params))
        this.schedule = b.schedule
        this.overlap = b.overlap
        this.catchUp = b.catchUp
        this.overdueAfter = b.overdueAfter
        this.maxRetries = b.maxRetries
        this.retryDelay = b.retryDelay
        this.recoverable = b.recoverable
        this.placement = b.placement
        this.startPaused = b.startPaused
        this.historyMode = b.historyMode
        this.metricsTaskTag = b.metricsTaskTag
    }

    /**
     * Entry point of the task DSL.
     *
     * @param id  the unique task id
     * @param spec a closure configuring the task via the DSL verbs
     * @return the immutable task
     */
    static Task define(
            String id,
            @DelegatesTo(value = Builder, strategy = Closure.DELEGATE_FIRST) Closure spec) {
        Builder b = new Builder(id)
        Closure copy = (Closure) spec.clone()
        copy.setResolveStrategy(Closure.DELEGATE_FIRST)
        copy.setDelegate(b)
        copy.call()
        return b.build()
    }

    /** @return true if this task uses a closure runner rather than a handler class. */
    boolean isClosureRunner() {
        return runner != null
    }

    @Override
    String toString() {
        return "Task(id=${id}, schedule=${schedule}, overlap=${overlap}, " +
                "catchUp=${catchUp}, placement=${placement})"
    }

    /**
     * Mutable builder backing the DSL. The verbs mirror section 5 of the
     * specification. Not statically compiled property-magic — every verb is an
     * explicit method.
     */
    @CompileStatic
    static class Builder {
        final String id
        Set<String> tags = new LinkedHashSet<>()
        Class<? extends TaskHandler> handlerType
        Closure runner
        Map<String, Object> params = new LinkedHashMap<>()
        Schedule schedule
        OverlapPolicy overlap = OverlapPolicy.SKIP
        CatchUpPolicy catchUp = CatchUpPolicy.SKIP
        Duration overdueAfter = Duration.ofSeconds(60)
        int maxRetries = 0
        Duration retryDelay = Duration.ofSeconds(10)
        boolean recoverable = false
        Placement placement = Placement.defaults()
        boolean startPaused = false
        HistoryMode historyMode = null
        boolean metricsTaskTag = false

        Builder(String id) {
            if (id == null || id.trim().isEmpty()) {
                throw new IllegalArgumentException('Task id must not be empty')
            }
            this.id = id.trim()
        }

        /** Adds one or more filtering tags. */
        void tags(String... labels) {
            for (String label : labels) {
                if (label != null && !label.trim().isEmpty()) {
                    tags.add(label.trim())
                }
            }
        }

        /** Sets the handler class instantiated per run. */
        void handler(Class<? extends TaskHandler> type) {
            this.handlerType = type
            this.runner = null
        }

        /** Sets an in-process closure runner (memory store only). */
        void handler(Closure closure) {
            this.runner = closure
            this.handlerType = null
        }

        /** Sets an in-process closure runner (memory store only). */
        void run(Closure closure) {
            handler(closure)
        }

        /** Merges the given entries into the task parameters. */
        void params(Map<String, ?> values) {
            if (values != null) {
                this.params.putAll((Map<String, Object>) values)
            }
        }

        /** Uses a cron schedule in the system default zone. */
        void cron(String expression) {
            this.schedule = new CronSchedule(expression)
        }

        /** Uses a cron schedule in the given zone. */
        void cron(String expression, ZoneId zone) {
            this.schedule = new CronSchedule(expression, zone)
        }

        /** Uses a fixed-rate interval schedule starting now. */
        void every(Duration period) {
            this.schedule = new IntervalSchedule(period, null, null)
        }

        /** Uses a fixed-rate interval schedule anchored at {@code start}. */
        void every(Duration period, Instant start) {
            this.schedule = new IntervalSchedule(period, start, null)
        }

        /** Uses a bounded fixed-rate interval schedule. */
        void every(Duration period, Instant start, Integer maxRuns) {
            this.schedule = new IntervalSchedule(period, start, maxRuns)
        }

        /** Uses a one-time schedule that runs exactly once at {@code at}. */
        void once(Instant at) {
            this.schedule = new OneTimeSchedule(at)
        }

        /** Sets an explicit schedule instance. */
        void schedule(Schedule s) {
            this.schedule = s
        }

        /** Sets the overlap policy. */
        void overlap(OverlapPolicy policy) {
            this.overlap = policy
        }

        /** Sets the catch-up policy. */
        void catchUp(CatchUpPolicy policy) {
            this.catchUp = policy
        }

        /** Sets how long after the planned time a run becomes overdue. */
        void overdueAfter(Duration duration) {
            this.overdueAfter = duration
        }

        /** Configures retries and the delay between them. */
        void retries(int max, Duration delay) {
            this.maxRetries = max
            this.retryDelay = delay
        }

        /** Configures the number of retries, keeping the default delay. */
        void retries(int max) {
            this.maxRetries = max
        }

        /** Marks the task as recoverable (re-run once after a node failure). */
        void recoverable() {
            this.recoverable = true
        }

        /** Sets whether the task is recoverable. */
        void recoverable(boolean value) {
            this.recoverable = value
        }

        /** Overrides the history mode for this task (e.g. {@code OFF} for noisy heartbeats). */
        void history(HistoryMode mode) {
            this.historyMode = mode
        }

        /**
         * Configures per-task metric options. Currently supports
         * {@code metrics taskTag: true}, which adds a {@code task} tag to this
         * task's {@code gron.runs.*}/{@code gron.run.duration} series. Opt-in
         * because a high task count would otherwise explode metric cardinality.
         */
        void metrics(Map<String, ?> options) {
            if (options != null && Boolean.TRUE == options.get('taskTag')) {
                this.metricsTaskTag = true
            }
        }

        /** Marks the task to start paused. */
        void startPaused() {
            this.startPaused = true
        }

        /** Sets whether the task starts paused. */
        void startPaused(boolean value) {
            this.startPaused = value
        }

        /** Configures placement via the placement DSL. */
        void runOn(
                @DelegatesTo(value = PlacementBuilder, strategy = Closure.DELEGATE_FIRST)
                        Closure spec) {
            PlacementBuilder pb = new PlacementBuilder()
            Closure copy = (Closure) spec.clone()
            copy.setResolveStrategy(Closure.DELEGATE_FIRST)
            copy.setDelegate(pb)
            copy.call()
            this.placement = pb.build()
        }

        /** Sets an explicit placement instance. */
        void placement(Placement p) {
            this.placement = p
        }

        Task build() {
            if (handlerType == null && runner == null) {
                throw new IllegalArgumentException(
                        "Task '${id}' must define a handler class or a closure runner")
            }
            if (schedule == null) {
                throw new IllegalArgumentException("Task '${id}' must define a schedule")
            }
            if (overdueAfter == null || overdueAfter.isNegative()) {
                throw new IllegalArgumentException("Task '${id}' overdueAfter must be >= 0")
            }
            if (maxRetries < 0) {
                throw new IllegalArgumentException("Task '${id}' maxRetries must be >= 0")
            }
            return new Task(this)
        }
    }

    /** Mutable builder backing the {@code runOn { ... }} placement DSL. */
    @CompileStatic
    static class PlacementBuilder {
        private RunMode mode = RunMode.EXCLUSIVE
        private String nodeSelector = null
        private NodeFallback fallback = NodeFallback.WAIT
        private Duration fallbackAfter = Duration.ofMinutes(5)

        /** Requires an exact node id. */
        void node(String nodeId) {
            this.nodeSelector = nodeId
        }

        /** Requires nodes carrying the given tag. */
        void tag(String tag) {
            this.nodeSelector = 'tag:' + tag
        }

        /** Switches the placement to {@link RunMode#EVERY_NODE}. */
        void everyNode() {
            this.mode = RunMode.EVERY_NODE
        }

        /** Sets the fallback behaviour when no node matches. */
        void fallback(NodeFallback fb) {
            this.fallback = fb
        }

        /** Sets the fallback behaviour and its grace period. */
        void fallback(NodeFallback fb, Duration after) {
            this.fallback = fb
            this.fallbackAfter = after
        }

        Placement build() {
            return new Placement(mode, nodeSelector, fallback, fallbackAfter)
        }
    }
}
