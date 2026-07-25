package de.example.gron.metrics

import groovy.transform.CompileStatic

/**
 * Builds a stable string key from a metric name and its tags. Tags are sorted
 * by key so the same name/tag pair always maps to the same key regardless of
 * insertion order (e.g. {@code gron.runs.completed{node=a,outcome=ok}}).
 */
@CompileStatic
final class MetricKey {

    private MetricKey() { }

    /** @return the canonical key for a metric name and its tags. */
    static String of(String name, Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) {
            return name
        }
        List<String> keys = new ArrayList<>(tags.keySet())
        Collections.sort(keys)
        StringBuilder sb = new StringBuilder(name).append('{')
        boolean first = true
        for (String k : keys) {
            if (!first) {
                sb.append(',')
            }
            sb.append(k).append('=').append(tags.get(k))
            first = false
        }
        return sb.append('}').toString()
    }
}
