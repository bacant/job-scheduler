package de.example.gron.examples

import de.example.gron.api.Task
import de.example.gron.api.TaskContext
import de.example.gron.api.TaskHandler
import de.example.gron.api.TaskScheduler
import de.example.gron.core.GronScheduler
import de.example.gron.store.file.FileTaskStore

import java.nio.file.Files
import java.time.Duration

/**
 * Demonstrates the persistent {@link FileTaskStore}: a task submitted in one
 * scheduler instance survives a restart of a second instance over the same
 * directory.
 */
class PersistenceDemo {

    static class PrintHandler implements TaskHandler {
        @Override
        void run(TaskContext context) {
            println "run of ${context.taskId} at ${context.startedAt}"
        }
    }

    static void main(String[] args) {
        def dir = Files.createTempDirectory('gron-demo')

        TaskScheduler first = GronScheduler.builder()
                .name('first')
                .store(new FileTaskStore(dir))
                .build()
        first.start()
        first.submit(Task.define('persisted') {
            handler PrintHandler
            every Duration.ofSeconds(2)
        })
        println "Submitted; tasks on disk: ${dir.toFile().list()}"
        first.stop(Duration.ofSeconds(2))

        // Restart over the same directory: the task is recovered.
        TaskScheduler second = GronScheduler.builder()
                .name('second')
                .store(new FileTaskStore(dir))
                .build()
        second.start()
        println "After restart, known tasks: ${second.findAll().collect { it.id }}"
        Thread.sleep(5000)
        second.stop(Duration.ofSeconds(2))
    }
}
