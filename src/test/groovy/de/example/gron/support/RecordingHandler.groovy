package de.example.gron.support

import de.example.gron.api.TaskContext
import de.example.gron.api.TaskHandler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * A persistable handler (resolvable by FQCN) that counts runs per task id in a
 * shared static map, for use across store and cluster tests.
 */
class RecordingHandler implements TaskHandler {

    static final ConcurrentHashMap<String, AtomicInteger> COUNTS = new ConcurrentHashMap<>()

    static int count(String taskId) {
        return COUNTS.getOrDefault(taskId, new AtomicInteger(0)).get()
    }

    static void reset() {
        COUNTS.clear()
    }

    @Override
    void run(TaskContext context) {
        COUNTS.computeIfAbsent(context.taskId, { new AtomicInteger(0) }).incrementAndGet()
    }
}
