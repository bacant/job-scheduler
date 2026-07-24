package de.example.gron.store.file

import de.example.gron.api.Task
import de.example.gron.api.TaskStoreException
import de.example.gron.spi.Serializer
import de.example.gron.store.AbstractInMemoryStore
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.util.stream.Stream

/**
 * A persistent {@link de.example.gron.spi.TaskStore} that keeps one JSON file
 * per task in a directory. Writes are atomic (write to a temporary file, then
 * {@link Files#move} with {@code ATOMIC_MOVE}); leftover temporary files from
 * an interrupted write are recovered (discarded) on {@link #open}.
 *
 * <p>Persistent but <strong>not shared</strong>: it deliberately does not
 * attempt cross-process file locking (which is unreliable over network file
 * systems). Use the JDBC store for multi-node operation. Inherits all claim
 * semantics from {@link AbstractInMemoryStore}; runtime claims are not
 * persisted, so on restart overdue occurrences are handled by the task's
 * catch-up policy.</p>
 */
@CompileStatic
class FileTaskStore extends AbstractInMemoryStore {

    private static final Logger log = LoggerFactory.getLogger(FileTaskStore)
    private static final String SUFFIX = '.json'
    private static final String TMP_SUFFIX = '.tmp'

    private final Path directory

    FileTaskStore(Path directory) {
        this.directory = directory
    }

    FileTaskStore(String directory) {
        this(Paths.get(directory))
    }

    FileTaskStore(File directory) {
        this(directory.toPath())
    }

    @Override
    boolean isPersistent() { return true }

    @Override
    boolean isShared() { return false }

    private Serializer serializer() { return storeContext.serializer }

    // -------------------------------------------------------- persistence

    @Override
    protected void loadOnOpen() {
        try {
            Files.createDirectories(directory)
        } catch (IOException e) {
            throw new TaskStoreException("Cannot create store directory ${directory}", e)
        }
        // Recover from interrupted writes: drop stale temp files.
        eachFile(TMP_SUFFIX) { Path p ->
            try {
                Files.deleteIfExists(p)
                log.warn('Discarded incomplete write {}', p)
            } catch (IOException ignored) { }
        }
        eachFile(SUFFIX) { Path p ->
            try {
                loadEntryFile(p)
            } catch (Exception e) {
                log.warn('Skipping unreadable task file {}: {}', p, e.message)
            }
        }
        log.info('Loaded {} task(s) from {}', tasks.size(), directory)
    }

    private void loadEntryFile(Path file) {
        String json = new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
        Map<String, Object> doc = serializer().decode(json)
        String taskJson = (String) doc.get('task')
        Task task = serializer().taskFromJson(taskJson, storeContext.classLoader)
        Entry e = new Entry()
        e.task = task
        e.nextRun = asInstant(doc.get('nextRun'))
        e.previousRun = asInstant(doc.get('previousRun'))
        e.paused = Boolean.TRUE == doc.get('paused')
        e.failed = Boolean.TRUE == doc.get('failed')
        e.pendingWait = asInstant(doc.get('pendingWait'))
        tasks.put(task.id, e)
    }

    @Override
    protected void persist(Entry entry) {
        Map<String, Object> doc = new LinkedHashMap<>()
        doc.put('task', serializer().taskToJson(entry.task))
        doc.put('nextRun', entry.nextRun?.toString())
        doc.put('previousRun', entry.previousRun?.toString())
        doc.put('paused', entry.paused)
        doc.put('failed', entry.failed)
        doc.put('pendingWait', entry.pendingWait?.toString())
        String json = serializer().encode(doc)

        Path target = fileFor(entry.task.id)
        Path tmp = directory.resolve(safeName(entry.task.id) + '.' + UUID.randomUUID() + TMP_SUFFIX)
        try {
            Files.write(tmp, json.getBytes(StandardCharsets.UTF_8))
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING)
            } catch (Exception atomicUnsupported) {
                // Fall back to a non-atomic replace if the FS lacks ATOMIC_MOVE.
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING)
            }
        } catch (IOException e) {
            throw new TaskStoreException("Failed to persist task ${entry.task.id}", e)
        } finally {
            try { Files.deleteIfExists(tmp) } catch (IOException ignored) { }
        }
    }

    @Override
    protected void removePersisted(String taskId) {
        try {
            Files.deleteIfExists(fileFor(taskId))
        } catch (IOException e) {
            log.warn('Failed to delete task file for {}: {}', taskId, e.message)
        }
    }

    // -------------------------------------------------------------- utils

    private Path fileFor(String id) {
        return directory.resolve(safeName(id) + SUFFIX)
    }

    /** Deterministic file-safe encoding of a task id (the real id lives inside the file). */
    private static String safeName(String id) {
        StringBuilder sb = new StringBuilder()
        for (char c : id.toCharArray()) {
            if (Character.isLetterOrDigit(c) || c == '.' || c == '-' || c == '_') {
                sb.append(c)
            } else {
                sb.append('_').append(Integer.toHexString((int) c))
            }
        }
        return sb.toString()
    }

    private static Instant asInstant(Object value) {
        return value == null ? null : Instant.parse((String) value)
    }

    private void eachFile(String suffix, Closure<?> action) {
        Stream<Path> stream = null
        try {
            stream = Files.list(directory)
            stream.filter { Path p -> p.getFileName().toString().endsWith(suffix) }
                    .forEach { Path p -> action.call(p) }
        } catch (IOException e) {
            throw new TaskStoreException("Cannot list store directory ${directory}", e)
        } finally {
            stream?.close()
        }
    }
}
