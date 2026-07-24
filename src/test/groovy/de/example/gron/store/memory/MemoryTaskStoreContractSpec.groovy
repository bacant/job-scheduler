package de.example.gron.store.memory

import de.example.gron.spi.TaskStore
import de.example.gron.store.TaskStoreContract

/** Runs the shared store contract against {@link MemoryTaskStore}. */
class MemoryTaskStoreContractSpec extends TaskStoreContract {

    @Override
    protected TaskStore createStore() {
        return new MemoryTaskStore()
    }

    @Override
    protected boolean expectedPersistent() { return false }

    @Override
    protected boolean expectedShared() { return false }
}
