package de.example.gron.history

import org.h2.jdbcx.JdbcDataSource

import javax.sql.DataSource
import java.util.concurrent.atomic.AtomicInteger

class JdbcHistoryStoreContractSpec extends HistoryStoreContract {

    private static final AtomicInteger DB_SEQ = new AtomicInteger(0)

    @Override
    protected HistoryStore createStore() {
        JdbcDataSource ds = new JdbcDataSource()
        ds.setURL("jdbc:h2:mem:history_${DB_SEQ.incrementAndGet()};DB_CLOSE_DELAY=-1")
        ds.setUser('sa'); ds.setPassword('')
        return new JdbcHistoryStore((DataSource) ds, true)
    }
}
