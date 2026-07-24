package de.example.gron.store.jdbc

import de.example.gron.spi.TaskStore
import de.example.gron.store.TaskStoreContract
import org.h2.jdbcx.JdbcDataSource

import javax.sql.DataSource
import java.util.concurrent.atomic.AtomicInteger

/** Runs the shared store contract against {@link JdbcTaskStore} on H2. */
class JdbcTaskStoreContractSpec extends TaskStoreContract {

    private static final AtomicInteger DB_SEQ = new AtomicInteger(0)

    @Override
    protected TaskStore createStore() {
        JdbcDataSource ds = new JdbcDataSource()
        // Unique in-memory database per test; kept alive while the test runs.
        ds.setURL("jdbc:h2:mem:contract_${DB_SEQ.incrementAndGet()};DB_CLOSE_DELAY=-1")
        ds.setUser('sa')
        ds.setPassword('')
        return new JdbcTaskStore((DataSource) ds, true)
    }

    @Override
    protected boolean expectedPersistent() { return true }

    @Override
    protected boolean expectedShared() { return true }
}
