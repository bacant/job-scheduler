package de.example.gron.history

import spock.lang.TempDir

import java.nio.file.Path

class FileHistoryStoreContractSpec extends HistoryStoreContract {

    @TempDir
    Path tempDir

    @Override
    protected HistoryStore createStore() {
        return new FileHistoryStore(tempDir)
    }
}
