package de.example.gron.store.file

import de.example.gron.spi.TaskStore
import de.example.gron.store.TaskStoreContract
import spock.lang.TempDir

import java.nio.file.Path

/** Runs the shared store contract against {@link FileTaskStore}. */
class FileTaskStoreContractSpec extends TaskStoreContract {

    @TempDir
    Path tempDir

    @Override
    protected TaskStore createStore() {
        return new FileTaskStore(tempDir)
    }

    @Override
    protected boolean expectedPersistent() { return true }

    @Override
    protected boolean expectedShared() { return false }
}
