package de.example.gron.store.memory

import de.example.gron.store.AbstractInMemoryStore
import groovy.transform.CompileStatic

/**
 * In-memory {@link de.example.gron.spi.TaskStore}: the default store and the
 * semantic reference. Inherits the full overlap / catch-up / placement claim
 * logic from {@link AbstractInMemoryStore} and adds no persistence.
 *
 * <p>Not persistent and not shared.</p>
 */
@CompileStatic
class MemoryTaskStore extends AbstractInMemoryStore {

    @Override
    boolean isPersistent() { return false }

    @Override
    boolean isShared() { return false }
}
