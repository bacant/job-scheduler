package de.example.gron.history

class MemoryHistoryStoreContractSpec extends HistoryStoreContract {

    @Override
    protected HistoryStore createStore() {
        return new MemoryHistoryStore(1000)
    }
}
