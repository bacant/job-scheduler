package de.example.gron.history

import de.example.gron.api.TaskStoreException
import de.example.gron.spi.StoreContext
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.stream.Stream

/**
 * Append-only JSONL {@link HistoryStore}: one file per UTC day
 * ({@code runs-YYYY-MM-DD.jsonl}). Retention deletes whole day files.
 *
 * <p><strong>Query scan limits:</strong> queries read the newest day file first
 * and walk backwards, reversing each file's lines (files are appended
 * oldest-first) until {@code limit} matches are collected. There is no global
 * index, so a highly selective filter may scan many files; for heavy querying
 * use the JDBC history store.</p>
 */
@CompileStatic
class FileHistoryStore implements HistoryStore {

    private static final Logger log = LoggerFactory.getLogger(FileHistoryStore)
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern('yyyy-MM-dd')
    private static final String PREFIX = 'runs-'
    private static final String SUFFIX = '.jsonl'

    private final Path directory

    FileHistoryStore(Path directory) {
        this.directory = directory
    }

    FileHistoryStore(String directory) {
        this(Paths.get(directory))
    }

    FileHistoryStore(File directory) {
        this(directory.toPath())
    }

    @Override
    void open(StoreContext context) {
        try {
            Files.createDirectories(directory)
        } catch (IOException e) {
            throw new TaskStoreException("Cannot create history directory ${directory}", e)
        }
    }

    @Override
    void close() { }

    @Override
    void record(RunRecord record) {
        String day = DAY.format(RunRecordJson.recordTime(record).atZone(ZoneOffset.UTC))
        Path file = directory.resolve(PREFIX + day + SUFFIX)
        String line = RunRecordJson.toJson(record) + '\n'
        try {
            Files.write(file, line.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        } catch (IOException e) {
            log.warn('Failed to append history record {}: {}', record.runId, e.message)
        }
    }

    @Override
    List<RunRecord> query(HistoryQuery query) {
        int limit = HistoryFilter.limit(query)
        List<RunRecord> result = new ArrayList<>()
        List<Path> files = dayFilesDescending()
        for (Path file : files) {
            if (result.size() >= limit) {
                break
            }
            List<String> lines
            try {
                lines = Files.readAllLines(file, StandardCharsets.UTF_8)
            } catch (IOException e) {
                log.warn('Failed to read history file {}: {}', file, e.message)
                continue
            }
            for (int i = lines.size() - 1; i >= 0 && result.size() < limit; i--) {
                String line = lines.get(i).trim()
                if (line.isEmpty()) {
                    continue
                }
                RunRecord r
                try {
                    r = RunRecordJson.fromJson(line)
                } catch (Exception ignored) {
                    continue
                }
                if (HistoryFilter.matches(r, query)) {
                    result.add(r)
                }
            }
        }
        return result
    }

    @Override
    long deleteOlderThan(Instant cutoff) {
        String cutoffDay = DAY.format(cutoff.atZone(ZoneOffset.UTC))
        long removed = 0L
        for (Path file : dayFilesDescending()) {
            String day = dayOf(file)
            if (day != null && day < cutoffDay) {
                try {
                    long lines = Files.readAllLines(file, StandardCharsets.UTF_8).size()
                    Files.deleteIfExists(file)
                    removed += lines
                } catch (IOException e) {
                    log.warn('Failed to delete history file {}: {}', file, e.message)
                }
            }
        }
        return removed
    }

    private List<Path> dayFilesDescending() {
        List<Path> files = new ArrayList<>()
        Stream<Path> stream = null
        try {
            stream = Files.list(directory)
            stream.filter { Path p ->
                String n = p.getFileName().toString()
                n.startsWith(PREFIX) && n.endsWith(SUFFIX)
            }.forEach { Path p -> files.add(p) }
        } catch (IOException e) {
            log.warn('Failed to list history directory {}: {}', directory, e.message)
        } finally {
            stream?.close()
        }
        files.sort { Path a, Path b -> dayOf(b) <=> dayOf(a) }   // newest first
        return files
    }

    private static String dayOf(Path file) {
        String n = file.getFileName().toString()
        if (n.startsWith(PREFIX) && n.endsWith(SUFFIX)) {
            return n.substring(PREFIX.length(), n.length() - SUFFIX.length())
        }
        return null
    }
}
