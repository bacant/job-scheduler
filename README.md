# GronScheduler

A lightweight, cron-capable task scheduler for the JVM, written in Groovy and
targeting **Java 8**. GronScheduler borrows the core ideas of heavier
schedulers — cron-driven execution, persistence, clustering, extensibility —
but deliberately keeps a small, opinionated domain model and its own
vocabulary.

- **One entity, the `Task`.** A task unites work (a handler), a schedule,
  parameters and run options in a single immutable value object. There are no
  separate job / detail / trigger entities and no 1:n relationship between work
  and schedules — if you need two schedules, define two tasks with the same
  handler.
- **`java.time` throughout** (`Instant`, `Duration`, `ZoneId`, `Clock`) — never
  `java.util.Date` in the public API.
- **One external runtime dependency:** `org.slf4j:slf4j-api`. Everything else is
  the JDK and Groovy standard library.
- **Unchecked exceptions** (`GronException` hierarchy) — no checked-exception
  ceremony.

## What it deliberately does *not* have

Name/group key pairs, holiday calendars, trigger priorities, trigger vetoes,
standby mode, multiple listener types with matchers, a misfire-instruction
matrix, eight trigger states, or checked exceptions. The vocabulary is its own:
`Task`, `TaskHandler`, `Schedule`, `CatchUpPolicy`, `OverlapPolicy`,
`TaskStore`, `nodeId` — no Quartz terms.

---

## Quickstart

```groovy
import de.example.gron.api.*
import de.example.gron.core.GronScheduler
import java.time.Duration
import java.time.ZoneId

TaskScheduler scheduler = GronScheduler.builder()
        .name('billing')
        .workers(8)
        .build()          // defaults: MemoryTaskStore, SingleNodeCoordinator, JsonSerializer
scheduler.start()

// Handler class (instantiated per run via the HandlerFactory)
class ReportHandler implements TaskHandler {
    void run(TaskContext ctx) {
        println "report for ${ctx.params.region} at ${ctx.startedAt}"
    }
}

Task nightly = Task.define('nightly-report') {
    tags 'reports', 'billing'
    handler ReportHandler
    params region: 'EU', maxRows: 500
    cron '0 30 6 * * *', ZoneId.of('Europe/Berlin')
    overlap OverlapPolicy.SKIP
    catchUp CatchUpPolicy.RUN_ONCE
    retries 3, Duration.ofSeconds(10)
    recoverable()
}
scheduler.submit(nightly)

// A quick interval task using an in-process closure runner (memory store only):
scheduler.submit(Task.define('heartbeat') {
    handler { TaskContext ctx -> println "tick ${ctx.startedAt}" }
    every Duration.ofSeconds(5)
})

scheduler.stop(Duration.ofSeconds(10))   // graceful; Duration.ZERO = stop now
```

See `examples/` for runnable `QuickstartDemo`, `PersistenceDemo`, `ClusterDemo`
and `EveryNodeDemo`.

---

## Task DSL reference

`Task.define(id) { ... }` builds an immutable `Task`. Verbs:

| Verb | Meaning |
|---|---|
| `tags 'a', 'b'` | Optional filtering labels. |
| `handler MyHandler` | Handler class, instantiated per run. |
| `handler { TaskContext ctx -> ... }` | In-process closure runner (**memory store only**). |
| `params key: value, ...` | Parameters exposed to the handler as an immutable map. |
| `cron '0 30 6 * * *'[, zone]` | Cron schedule (see syntax below). |
| `every Duration[, start[, maxRuns]]` | Fixed-rate interval schedule. |
| `once Instant` | One-time schedule. |
| `overlap OverlapPolicy.X` | Overlap behaviour; default `SKIP`. |
| `catchUp CatchUpPolicy.X` | Catch-up behaviour; default `SKIP`. |
| `overdueAfter Duration` | When a run counts as overdue; default 60s. |
| `retries n, delay` | Retry count and delay; default 0 retries, 10s delay. |
| `recoverable()` | Re-run once after a node failure (at-least-once). |
| `startPaused()` | Register the task paused. |
| `runOn { ... }` | Placement (see clustering). |

## Scheduler builder reference

```groovy
GronScheduler.builder()
    .name('billing')                          // scheduler name
    .nodeId('node-a')                         // this node's id (default: random)
    .nodeTags(['eu'] as Set)                  // node tags (for tag: selectors)
    .workers(8)                               // worker pool size
    .batchLimit(100)                          // max claims per loop tick
    .store(new JdbcTaskStore(dataSource))     // default: new MemoryTaskStore()
    .coordinator(new JdbcClusterCoordinator(dataSource, 'node-a', ['eu'] as Set))
    .replication(new NoOpReplicationProvider())   // default
    .handlerFactory(myDiAwareFactory)         // default: no-arg constructor
    .serializer(new JsonSerializer())         // default
    .clock(Clock.systemUTC())                 // injectable for tests
    .classLoader(cl)                          // default: context class loader
    .maintenanceInterval(Duration.ofSeconds(5))   // heartbeat/failover cadence
    .maxSleep(Duration.ofSeconds(30))         // loop sleep cap
    .build()
```

---

## Supported cron syntax

The cron parser is the bundled, unmodified `CronExpression` class. Supported
syntax (exactly this — nothing more, nothing less):

- **5 fields** (`minute hour day-of-month month day-of-week`) or **6 fields**
  with a leading seconds field.
- Day of week is **0–7 or SUN–SAT, where 0 and 7 both mean Sunday** (Unix
  convention, not Quartz).
- Per field: `*`, single values, lists (`1,3,5`), ranges (`8-17`), steps
  (`*/15`, `3-59/15`), and names for months and weekdays. `?` is treated like
  `*`.
- Macros: `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`,
  `@hourly`.
- If **both** day-of-month and day-of-week are restricted, classic cron OR
  semantics apply (a match in either field is enough).
- The Quartz special characters `L`, `W` and `#` are **not** supported.

If a cron expression has no next match within the parser's search horizon (e.g.
`0 0 30 2 *`), `CronSchedule.nextRunAfter(...)` returns `null` and the task ends
in state `DONE`. Invalid expressions raise `InvalidScheduleException`.

## Schedules

| Schedule | Behaviour |
|---|---|
| `CronSchedule(expr, zone)` | Wraps `CronExpression`; delegates to `nextInstant`. |
| `IntervalSchedule(period, start?, maxRuns?)` | Fixed rate. **No drift:** the next run is derived from the fixed anchor, not from when a run actually finished (the store always feeds the previous *planned* time). If `start` is null the first run is one period after submission, and `maxRuns` requires a non-null anchor. |
| `OneTimeSchedule(at)` | Runs once, then `null`. |

---

## Execution semantics

### OverlapPolicy (per task, cluster-wide with a shared store)

- `PARALLEL` — runs may overlap.
- `SKIP` (default) — occurrences that fall due during an active run are skipped
  (`onSkip(OVERLAP)`); the schedule continues normally.
- `WAIT` — at most **one** pending run is remembered and started right after the
  active run finishes; further occurrences during the wait collapse into it.

### CatchUpPolicy (when `now - plannedTime > overdueAfter`)

- `SKIP` — discard overdue occurrences; only schedule the next regular run.
- `RUN_ONCE` — run one catch-up occurrence, then continue regularly.
- `RUN_ALL` — run every missed occurrence in order (bounded by the batch limit
  and, under `SKIP` overlap, by completion of the previous run).

Catch-up is applied store-side, both while claiming **and** implicitly when a
persistent store is reopened after a restart (the persisted cursor may be
overdue, and the first claim applies the policy).

### Retry

If a run throws and `attempt <= maxRetries`, the same run is re-scheduled after
`retryDelay` with `attempt + 1`. Retry counters live in the memory of the
executing node; after a fail-over a recovered run restarts at `attempt = 1`.
After the last attempt the outcome is `FAILED` and `onError` fires; the schedule
continues — unless the handler threw `TaskFailedException` with
`abortSchedule = true`, which moves the task to `FAILED` (no further runs until
`resume` or a fresh `submit`).

### Listeners

One listener type with four callbacks (`beforeRun`, `afterRun`, `onSkip`,
`onError`). Extend `TaskListenerAdapter` to override only what you need.
Listener exceptions are logged at WARN and never affect the scheduler.

---

## Persistence

Three stores, all satisfying the same store contract:

| Store | Persistent | Shared | Notes |
|---|---|---|---|
| `MemoryTaskStore` (default) | no | no | `ConcurrentHashMap` + `TreeSet` due index; the semantic reference. |
| `FileTaskStore` | yes | **no** | One JSON file per task; atomic writes (temp + `ATOMIC_MOVE`); recovery of interrupted writes on open. |
| `JdbcTaskStore` | yes | **yes** | `java.sql`/`groovy.sql` only; transactional `SELECT ... FOR UPDATE` claims; bundled DDL. |

**Allowed `params` value types for persistent stores:** `String`, `Number`,
`Boolean`, `List`, `Map`, and `Instant` (encoded as ISO-8601). Any other type —
and closure runners — are rejected on `save` with a clear message. Handler
classes are resolved through the builder's class loader.

**Why `FileTaskStore` is not shared:** it deliberately avoids cross-process file
locking, which is unreliable over network file systems. Use `JdbcTaskStore` for
multi-node deployments.

**JDBC DDL:** see `src/main/resources/ddl/schema.sql` (tables `GRON_TASK`,
`GRON_RUN`, `GRON_NODE`, `GRON_LOCK`), tested against H2. Time columns store
epoch nanoseconds as `BIGINT` to avoid time-zone pitfalls and preserve full
`Instant` precision. Pass `autoCreate = true` to `JdbcTaskStore` to run the DDL
on open, or apply it yourself. Run all transitions at `READ_COMMITTED` or
stronger — the claim path relies on row locks (`FOR UPDATE`) for cluster-wide
exactly-once.

---

## Clustering and placement

With a shared store and an active `ClusterCoordinator`, every due occurrence
runs **exactly once** cluster-wide (claim-based). For `recoverable` tasks,
fail-over gives **at-least-once** semantics — so **write recoverable handlers to
be idempotent.**

- `SingleNodeCoordinator` (default) — no-op locks, one node.
- `JdbcClusterCoordinator` — heartbeats via `GRON_NODE`, cluster-wide locks via
  `SELECT ... FOR UPDATE` on `GRON_LOCK`. A node is dead when
  `now - lastSeen > interval * factor + tolerance` (interval default 7.5s;
  factor and tolerance configurable). On detecting a dead node, the discovering
  node reclaims its recoverable runs (as immediate one-off runs carrying the
  original planned time) under the `recovery` lock; other claims are freed and
  the catch-up policy applies.

**Clocks:** all nodes must be time-synchronized (NTP). The time source
everywhere is the injected `java.time.Clock`.

### Placement DSL

```groovy
runOn { node 'node-a' }                                    // exact node
runOn { tag 'eu'; fallback NodeFallback.ANY_NODE, Duration.ofMinutes(10) }
runOn { everyNode() }                                      // EVERY_NODE (needs replication)
```

- **`EXCLUSIVE`** (default): a `nodeSelector` (`null` = any node, an exact id, or
  `tag:<tag>`) filters which nodes may claim. `fallback == ANY_NODE` lets any
  node claim an occurrence that has gone unclaimed longer than `fallbackAfter`;
  `WAIT` waits for a matching node. On a single-node store a non-matching
  selector logs a warning and skips the occurrence.
- **`EVERY_NODE`** (bonus): the task runs on every active node locally, without a
  cluster claim. This requires an active, **real** `ReplicationProvider` so every
  node has the task definition; otherwise `submit` throws
  `UnsupportedPlacementException`.

---

## Replication (SPI)

`ReplicationProvider` distributes task changes between nodes. A network
transport is intentionally out of scope; provide your own. Bundled:

- `NoOpReplicationProvider` (default) — does nothing; `EVERY_NODE` is rejected.
- `InMemoryReplicationProvider` — couples several schedulers within one JVM
  through a shared `Bus`; the reference implementation and the basis for the
  `EVERY_NODE` demo.

Events (`TASK_SAVED`, `TASK_DELETED`, `TASK_PAUSED`, `TASK_RESUMED`,
`RUN_COMPLETED`) carry a serializer-encoded payload, a monotonic sequence number
and the origin node id. Conflict resolution is last-writer-wins; applying an
incoming event is idempotent. Replication is driven at the scheduler level
(`submit`/`cancel`/`pause`/`resume`), keeping stores free of replication
concerns.

> Note: replication distributes task **definitions**. Exactly-once for
> `EXCLUSIVE` tasks comes from a *shared* store, not from replication —
> replicating an `EXCLUSIVE` task across non-shared memory stores would run it on
> every node. Use replication for `EVERY_NODE`, and a shared `JdbcTaskStore` for
> `EXCLUSIVE` exactly-once.

---

## The five SPIs

| SPI | Purpose | Default |
|---|---|---|
| `TaskStore` | Persistence and run state | `MemoryTaskStore` |
| `ClusterCoordinator` | Nodes, heartbeats, cluster locks | `SingleNodeCoordinator` |
| `ReplicationProvider` | Distribution of task changes | `NoOpReplicationProvider` |
| `Serializer` | JSON (de)serialization of tasks/schedules | `JsonSerializer` (groovy.json) |
| `HandlerFactory` | Instantiation of handlers (DI hook) | no-arg constructor |

There are deliberately **no** SPIs for thread pools, class-loader helpers,
connection providers or the time source: the pool and class loader are builder
options, connections arrive as a `DataSource`, and the time source is a
`java.time.Clock`.

### Extending each SPI

- **`TaskStore`** — implement the 16 methods. Overlap, catch-up and placement
  are your responsibility on the claim path; extend `AbstractInMemoryStore` to
  reuse the reference single-node logic and just add persistence hooks
  (`persist`, `removePersisted`, `loadOnOpen`), as `FileTaskStore` does.
  Optionally implement `DueTimeAware` (precise loop sleep) and `AdHocRunSupport`
  (`runNow`).
- **`ClusterCoordinator`** — provide node registry, heartbeats and a
  cluster-wide `lock(name, timeout)` returning an `AutoCloseable`.
- **`ReplicationProvider`** — `publish` local changes and deliver remote ones to
  the registered consumer; make apply idempotent and report `isReal()`.
- **`Serializer`** — task/document JSON; encode any custom `params` types you
  allow.
- **`HandlerFactory`** — plug in your DI container's lookup.

> A small note on the `TaskStore` size: the SPI is intentionally minimal.
> Beyond the specified operations it adds one method, `setFailed(taskId)`,
> because the fixed `complete(run, outcome, nextRun)` signature cannot
> distinguish an aborted task (`FAILED`) from a naturally exhausted schedule
> (`DONE`, `nextRun == null`). `DueTimeAware`, `AdHocRunSupport` and `SkipSink`
> are separate optional interfaces so the core `TaskStore` stays small.

---

## Building and testing

```bash
./gradlew test        # compiles to Java 8 bytecode, runs the full suite
```

- Target platform: JDK 8 (`bytecode 1.8`). The Groovy compiler emits 1.8
  bytecode; `javac` (for the joint compile) uses `--release 8`.
- Runtime dependencies: Groovy and `slf4j-api` only. Test scope adds Spock,
  JUnit 4, H2 and `slf4j-simple`.
- Time-based core logic is tested deterministically with an injected `Clock`
  (`MutableClock`); real-time integration tests (cluster, replication) are
  clearly separated.

## Operational checklist

- **NTP:** keep cluster clocks synchronized; all timing uses the injected clock.
- **Idempotency:** make `recoverable` handlers idempotent — fail-over is
  at-least-once.
- **Transaction isolation:** `READ_COMMITTED` or stronger for the JDBC store.
- **FileTaskStore is single-node:** never point two processes at the same
  directory.
