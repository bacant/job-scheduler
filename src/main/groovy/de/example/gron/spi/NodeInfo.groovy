package de.example.gron.spi

import groovy.transform.CompileStatic

import java.time.Instant

/**
 * Describes a cluster node: its id, its tags, and when it was last seen.
 */
@CompileStatic
class NodeInfo implements Serializable {

    private static final long serialVersionUID = 1L

    /** Unique node id. */
    final String id

    /** Node tags used by {@code tag:<tag>} placement selectors. */
    final Set<String> tags

    /** Last heartbeat instant, or {@code null} if unknown. */
    final Instant lastSeen

    NodeInfo(String id, Set<String> tags, Instant lastSeen = null) {
        this.id = id
        this.tags = tags == null ? Collections.<String> emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<String>(tags))
        this.lastSeen = lastSeen
    }

    /** @return true if this node carries the given tag. */
    boolean hasTag(String tag) {
        return tags.contains(tag)
    }

    @Override
    String toString() {
        return "NodeInfo(id=${id}, tags=${tags}, lastSeen=${lastSeen})"
    }
}
