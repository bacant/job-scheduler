package de.example.gron.history

import de.example.gron.api.RunOutcome
import de.example.gron.api.SkipReason
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic

import java.time.Instant

/**
 * JSON (de)serialization for {@link RunRecord}, used by the file history store.
 */
@CompileStatic
final class RunRecordJson {

    private RunRecordJson() { }

    /** @return the effective time used for ordering/retention (start, else planned). */
    static Instant recordTime(RunRecord r) {
        return r.startedAt != null ? r.startedAt : r.plannedTime
    }

    static String toJson(RunRecord r) {
        Map<String, Object> m = new LinkedHashMap<>()
        m.put('runId', r.runId)
        m.put('taskId', r.taskId)
        m.put('tags', new ArrayList<String>(r.tags))
        m.put('plannedTime', str(r.plannedTime))
        m.put('startedAt', str(r.startedAt))
        m.put('finishedAt', str(r.finishedAt))
        m.put('durationMillis', r.durationMillis)
        m.put('nodeId', r.nodeId)
        m.put('attempt', r.attempt)
        m.put('outcome', r.outcome?.name())
        m.put('skipReason', r.skipReason?.name())
        m.put('recovered', r.recovered)
        m.put('errorType', r.errorType)
        m.put('errorMessage', r.errorMessage)
        m.put('errorStackTrace', r.errorStackTrace)
        return JsonOutput.toJson(m)
    }

    @SuppressWarnings('unchecked')
    static RunRecord fromJson(String json) {
        Map<String, Object> m = (Map<String, Object>) new JsonSlurper().parseText(json)
        return new RunRecord([
                runId          : m.get('runId'),
                taskId         : m.get('taskId'),
                tags           : m.get('tags') == null ? ([] as Set)
                        : new LinkedHashSet<String>((List<String>) m.get('tags')),
                plannedTime    : inst(m.get('plannedTime')),
                startedAt      : inst(m.get('startedAt')),
                finishedAt     : inst(m.get('finishedAt')),
                durationMillis : m.get('durationMillis'),
                nodeId         : m.get('nodeId'),
                attempt        : m.get('attempt'),
                outcome        : m.get('outcome') == null ? null : RunOutcome.valueOf((String) m.get('outcome')),
                skipReason     : m.get('skipReason') == null ? null : SkipReason.valueOf((String) m.get('skipReason')),
                recovered      : m.get('recovered'),
                errorType      : m.get('errorType'),
                errorMessage   : m.get('errorMessage'),
                errorStackTrace: m.get('errorStackTrace')
        ])
    }

    private static String str(Instant i) { return i == null ? null : i.toString() }

    private static Instant inst(Object o) { return o == null ? null : Instant.parse((String) o) }
}
