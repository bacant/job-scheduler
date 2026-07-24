package de.example.gron.api

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.Immutable

import java.time.Duration

/**
 * Immutable placement policy describing where a task's runs may execute in a
 * cluster.
 *
 * <ul>
 *   <li>{@link #mode} — {@link RunMode#EXCLUSIVE} (default, claim-based, one
 *       node per occurrence) or {@link RunMode#EVERY_NODE}.</li>
 *   <li>{@link #nodeSelector} — {@code null} = any node; an exact node id; or
 *       {@code 'tag:<tag>'} to require a node tag.</li>
 *   <li>{@link #fallback} — {@link NodeFallback#WAIT} (default) or
 *       {@link NodeFallback#ANY_NODE}.</li>
 *   <li>{@link #fallbackAfter} — only used when {@code fallback == ANY_NODE};
 *       default 5 minutes.</li>
 * </ul>
 */
@CompileStatic
@Immutable(knownImmutableClasses = [Duration])
@EqualsAndHashCode
class Placement implements Serializable {

    private static final long serialVersionUID = 1L

    RunMode mode
    String nodeSelector
    NodeFallback fallback
    Duration fallbackAfter

    /** Default placement: exclusive, any node, wait for a match. */
    static Placement defaults() {
        return new Placement(RunMode.EXCLUSIVE, null, NodeFallback.WAIT, Duration.ofMinutes(5))
    }

    /** @return true if this placement targets a specific node id or tag. */
    boolean hasSelector() {
        return nodeSelector != null && !nodeSelector.trim().isEmpty()
    }

    /** @return the tag required by a {@code 'tag:<tag>'} selector, or null. */
    String selectorTag() {
        if (hasSelector() && nodeSelector.startsWith('tag:')) {
            return nodeSelector.substring('tag:'.length())
        }
        return null
    }

    /** @return the exact node id required by the selector, or null. */
    String selectorNodeId() {
        if (hasSelector() && !nodeSelector.startsWith('tag:')) {
            return nodeSelector
        }
        return null
    }

    @Override
    String toString() {
        return "Placement(mode=${mode}, selector=${nodeSelector}, " +
                "fallback=${fallback}, fallbackAfter=${fallbackAfter})"
    }
}
