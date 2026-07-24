package de.example.gron.examples

import de.example.gron.api.CatchUpPolicy
import de.example.gron.api.OverlapPolicy
import de.example.gron.api.Task
import de.example.gron.api.TaskContext
import de.example.gron.api.TaskHandler
import de.example.gron.api.TaskScheduler
import de.example.gron.core.GronScheduler

import java.time.Duration
import java.time.ZoneId

/**
 * Minimal end-to-end example: build a scheduler with the in-memory store, submit
 * a couple of tasks (one cron, one interval, one closure runner) and let them
 * run for a few seconds.
 *
 * <p>Run with: {@code groovy -cp <classpath> QuickstartDemo} or from your IDE.</p>
 */
class QuickstartDemo {

    /** A trivial handler that prints its context. */
    static class ReportHandler implements TaskHandler {
        @Override
        void run(TaskContext context) {
            println "[${context.startedAt}] report for region=${context.params.region} " +
                    "(planned=${context.plannedTime}, attempt=${context.attempt})"
        }
    }

    static void main(String[] args) {
        TaskScheduler scheduler = GronScheduler.builder()
                .name('quickstart')
                .workers(4)
                .build()   // defaults: MemoryTaskStore, SingleNodeCoordinator, JsonSerializer

        scheduler.start()

        // A cron task: every 2 seconds (6-field expression) in Berlin time.
        scheduler.submit(Task.define('report') {
            tags 'reports'
            handler ReportHandler
            params region: 'EU'
            cron '*/2 * * * * *', ZoneId.of('Europe/Berlin')
            overlap OverlapPolicy.SKIP
            catchUp CatchUpPolicy.RUN_ONCE
        })

        // An interval task using a closure runner (memory store only).
        scheduler.submit(Task.define('heartbeat') {
            handler { TaskContext ctx -> println "heartbeat at ${ctx.startedAt}" }
            every Duration.ofSeconds(3)
        })

        println 'Scheduler running; press Ctrl+C to stop (auto-stops after ~8s).'
        Thread.sleep(8000)

        scheduler.stop(Duration.ofSeconds(5))
        println 'Stopped.'
    }
}
