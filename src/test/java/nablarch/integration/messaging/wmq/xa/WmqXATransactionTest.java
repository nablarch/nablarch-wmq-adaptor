package nablarch.integration.messaging.wmq.xa;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;

import nablarch.core.db.connection.BasicDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.transaction.Transaction;
import nablarch.fw.messaging.MessagingContext;
import nablarch.integration.messaging.wmq.MockWmqMessagingContextSupport;
import nablarch.integration.messaging.wmq.OnMemoryLogWriter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * {@link WmqXATransaction}のテスト。
 * @author Kiyohito Itoh
 */
public class WmqXATransactionTest {

    @Before
    public void setUp() {
        DbConnectionContext.removeConnection("testConn");
        MessagingContext.detach();
    }

    @After
    public void tearDown() {
        DbConnectionContext.removeConnection("testConn");
        MessagingContext.detach();
    }

    /**
     * 開始、コミット、ロールバックのタイミングでログが出力されること。
     */
    @Test
    public void testLog() throws Exception {

        MockWmqMessagingContext mockContext = new MockWmqMessagingContext();
        MessagingContext.attach(mockContext);

        MockBasicDbConnection connection = new MockBasicDbConnection(null);
        DbConnectionContext.setConnection("testConn", connection);

        WmqXATransactionFactory factory = new WmqXATransactionFactory();
        Transaction transaction = factory.getTransaction("testConn");

        OnMemoryLogWriter.LOGS.clear();

        transaction.begin();
        assertThat(OnMemoryLogWriter.LOGS.get("writer.memory").get(0).trim(),
                   is("SQL nablarch.integration.messaging.wmq.xa.WmqXATransaction#begin()"));

        transaction.commit();
        assertThat(OnMemoryLogWriter.LOGS.get("writer.memory").get(1).trim(),
                   is("SQL nablarch.integration.messaging.wmq.xa.WmqXATransaction#commit()"));

        transaction.rollback();
        assertThat(OnMemoryLogWriter.LOGS.get("writer.memory").get(2).trim(),
                   is("SQL nablarch.integration.messaging.wmq.xa.WmqXATransaction#rollback()"));
    }

    private static final class MockBasicDbConnection extends BasicDbConnection {
        public MockBasicDbConnection(Connection con) {
            super(con);
        }
        @Override
        public void setIsolationLevel(int level) {
        }
    }

    private static final class MockWmqMessagingContext extends MockWmqMessagingContextSupport {
        @Override
        public void begin() {
        }
        @Override
        public void commit() {
        }
        @Override
        public void backout() {
        }
    }
}
