package de.example.gron.core

import de.example.gron.api.CatchUpPolicy
import de.example.gron.api.NodeFallback
import de.example.gron.api.OverlapPolicy
import de.example.gron.api.Placement
import de.example.gron.api.RunMode
import de.example.gron.api.Task
import de.example.gron.api.TaskHandler
import de.example.gron.api.TaskStoreException
import de.example.gron.schedule.CronSchedule
import de.example.gron.schedule.IntervalSchedule
import de.example.gron.schedule.OneTimeSchedule
import de.example.gron.schedule.Schedule
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic

import java.time.Duration
import java.time.Instant
import java.time.ZoneId

/**
 * Default {@link de.example.gron.spi.Serializer} based on {@code groovy.json}.
 *
 * <p>Tasks are persisted as a JSON document (id, tags, handler FQCN, params,
 * schedule with a type tag, and options). Allowed {@code params} values for
 * persistent stores are {@code String}, {@code Number}, {@code Boolean},
 * {@code List}, {@code Map} and {@code Instant} (encoded as ISO-8601). Any
 * other type — and closure runners — are rejected on serialization with a
 * clear message.</p>
 */
@CompileStatic
class JsonSerializer implements de.example.gron.spi.Serializer {

    private static final String INSTANT_MARKER = '__gron_type__'

    @Override
    String taskToJson(Task task) {
        if (task.isClosureRunner()) {
            throw new TaskStoreException(
                    "Task '${task.id}' uses a closure runner and cannot be persisted; " +
                            'closure runners are only supported by the in-memory store')
        }
        return JsonOutput.toJson(taskToMap(task))
    }

    /** Builds the raw document map for a task (also used for change-event payloads). */
    Map<String, Object> taskToMap(Task task) {
        Map<String, Object> doc = new LinkedHashMap<>()
        doc.put('id', task.id)
        doc.put('tags', new ArrayList<String>(task.tags))
        doc.put('handlerType', task.handlerType.name)
        doc.put('params', (Object) encodeValue(task.params))
        doc.put('schedule', scheduleToMap(task.schedule))
        doc.put('overlap', task.overlap.name())
        doc.put('catchUp', task.catchUp.name())
        doc.put('overdueAfter', task.overdueAfter.toString())
        doc.put('maxRetries', task.maxRetries)
        doc.put('retryDelay', task.retryDelay.toString())
        doc.put('recoverable', task.recoverable)
        doc.put('startPaused', task.startPaused)
        doc.put('placement', placementToMap(task.placement))
        return doc
    }

    @Override
    Task taskFromJson(String json, ClassLoader classLoader) {
        Object parsed = new JsonSlurper().parseText(json)
        return taskFromMap((Map<String, Object>) parsed, classLoader)
    }

    @SuppressWarnings('unchecked')
    Task taskFromMap(Map<String, Object> doc, ClassLoader classLoader) {
        String id = (String) doc.get('id')
        String handlerName = (String) doc.get('handlerType')
        Class<?> handlerClass
        try {
            handlerClass = Class.forName(handlerName, true, classLoader)
        } catch (ClassNotFoundException e) {
            throw new TaskStoreException(
                    "Handler class '${handlerName}' for task '${id}' not found", e)
        }

        Task.Builder b = new Task.Builder(id)
        List<String> tags = (List<String>) doc.get('tags')
        if (tags != null) {
            b.tags(tags.toArray(new String[0]))
        }
        b.handler((Class<? extends TaskHandler>) handlerClass)
        Object params = decodeValue(doc.get('params'))
        if (params instanceof Map) {
            b.params((Map<String, Object>) params)
        }
        b.schedule(scheduleFromMap((Map<String, Object>) doc.get('schedule')))
        b.overlap(OverlapPolicy.valueOf((String) doc.get('overlap')))
        b.catchUp(CatchUpPolicy.valueOf((String) doc.get('catchUp')))
        b.overdueAfter(Duration.parse((String) doc.get('overdueAfter')))
        b.retries(((Number) doc.get('maxRetries')).intValue(),
                Duration.parse((String) doc.get('retryDelay')))
        b.recoverable((Boolean) doc.get('recoverable'))
        b.startPaused((Boolean) doc.get('startPaused'))
        b.placement(placementFromMap((Map<String, Object>) doc.get('placement')))
        return b.build()
    }

    @Override
    String encode(Map<String, Object> document) {
        return JsonOutput.toJson(encodeValue(document))
    }

    @Override
    @SuppressWarnings('unchecked')
    Map<String, Object> decode(String json) {
        Object parsed = new JsonSlurper().parseText(json)
        return (Map<String, Object>) decodeValue(parsed)
    }

    // --------------------------------------------------------- schedules

    private Map<String, Object> scheduleToMap(Schedule schedule) {
        Map<String, Object> m = new LinkedHashMap<>()
        if (schedule instanceof CronSchedule) {
            CronSchedule c = (CronSchedule) schedule
            m.put('type', CronSchedule.TYPE)
            m.put('expression', c.expression)
            m.put('zone', c.zone.id)
        } else if (schedule instanceof IntervalSchedule) {
            IntervalSchedule i = (IntervalSchedule) schedule
            m.put('type', IntervalSchedule.TYPE)
            m.put('period', i.period.toString())
            m.put('start', i.start == null ? null : i.start.toString())
            m.put('maxRuns', i.maxRuns)
        } else if (schedule instanceof OneTimeSchedule) {
            OneTimeSchedule o = (OneTimeSchedule) schedule
            m.put('type', OneTimeSchedule.TYPE)
            m.put('at', o.at.toString())
        } else {
            throw new TaskStoreException(
                    "Unknown schedule type: ${schedule?.getClass()?.name}")
        }
        return m
    }

    private Schedule scheduleFromMap(Map<String, Object> m) {
        String type = (String) m.get('type')
        switch (type) {
            case CronSchedule.TYPE:
                return new CronSchedule((String) m.get('expression'),
                        ZoneId.of((String) m.get('zone')))
            case IntervalSchedule.TYPE:
                String start = (String) m.get('start')
                Number maxRuns = (Number) m.get('maxRuns')
                return new IntervalSchedule(
                        Duration.parse((String) m.get('period')),
                        start == null ? null : Instant.parse(start),
                        maxRuns == null ? null : maxRuns.intValue())
            case OneTimeSchedule.TYPE:
                return new OneTimeSchedule(Instant.parse((String) m.get('at')))
            default:
                throw new TaskStoreException("Unknown schedule type tag: '${type}'")
        }
    }

    // --------------------------------------------------------- placement

    private Map<String, Object> placementToMap(Placement p) {
        Map<String, Object> m = new LinkedHashMap<>()
        m.put('mode', p.mode.name())
        m.put('nodeSelector', p.nodeSelector)
        m.put('fallback', p.fallback.name())
        m.put('fallbackAfter', p.fallbackAfter.toString())
        return m
    }

    private Placement placementFromMap(Map<String, Object> m) {
        return new Placement(
                RunMode.valueOf((String) m.get('mode')),
                (String) m.get('nodeSelector'),
                NodeFallback.valueOf((String) m.get('fallback')),
                Duration.parse((String) m.get('fallbackAfter')))
    }

    // ------------------------------------------------------ value coding

    /** Recursively encodes a value, tagging {@link Instant}s and rejecting unsupported types. */
    private Object encodeValue(Object value) {
        if (value == null || value instanceof String
                || value instanceof Boolean || value instanceof Number) {
            return value
        }
        if (value instanceof Instant) {
            Map<String, Object> m = new LinkedHashMap<>()
            m.put(INSTANT_MARKER, 'instant')
            m.put('value', value.toString())
            return m
        }
        if (value instanceof Map) {
            Map<String, Object> out = new LinkedHashMap<>()
            ((Map<?, ?>) value).each { k, v ->
                out.put(String.valueOf(k), encodeValue(v))
            }
            return out
        }
        if (value instanceof List) {
            List<Object> out = new ArrayList<>()
            for (Object v : (List<?>) value) {
                out.add(encodeValue(v))
            }
            return out
        }
        throw new TaskStoreException(
                "Unsupported param value type for persistence: ${value.getClass().name}; " +
                        'allowed: String, Number, Boolean, List, Map, Instant')
    }

    /** Reverses {@link #encodeValue}, restoring tagged {@link Instant}s. */
    private Object decodeValue(Object value) {
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value
            if ('instant' == map.get(INSTANT_MARKER)) {
                return Instant.parse((String) map.get('value'))
            }
            Map<String, Object> out = new LinkedHashMap<>()
            map.each { k, v -> out.put(String.valueOf(k), decodeValue(v)) }
            return out
        }
        if (value instanceof List) {
            List<Object> out = new ArrayList<>()
            for (Object v : (List<?>) value) {
                out.add(decodeValue(v))
            }
            return out
        }
        return value
    }
}
