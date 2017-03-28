package nablarch.integration.messaging.wmq.xa;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import nablarch.core.db.connection.BasicDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.transaction.Transaction;
import nablarch.fw.messaging.MessagingContext;
import nablarch.integration.messaging.wmq.MockWmqMessagingContextSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * {@link WmqXATransactionFactory}のテスト。
 * @author Kiyohito Itoh
 */
public class WmqXATransactionFactoryTest {

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
     * {@link WmqXATransaction}が生成され、指定したアイソレーションレベルが設定されること。
     */
    @Test
    @SuppressWarnings("serial")
    public void testGetTransaction() throws Exception {

        Map<String, Integer> isolationLevels = new HashMap<String, Integer>() {
            {
                put("READ_COMMITTED", Connection.TRANSACTION_READ_COMMITTED);
                put("READ_UNCOMMITTED", Connection.TRANSACTION_READ_UNCOMMITTED);
                put("REPEATABLE_READ", Connection.TRANSACTION_REPEATABLE_READ);
                put("SERIALIZABLE", Connection.TRANSACTION_SERIALIZABLE);
            }
        };

        for (Map.Entry<String, Integer> isolationLevel : isolationLevels.entrySet()) {

            MockWmqMessagingContext mockContext = new MockWmqMessagingContext();
            MessagingContext.attach(mockContext);

            MockBasicDbConnection connection = new MockBasicDbConnection(null);
            DbConnectionContext.setConnection("testConn", connection);

            try {
                WmqXATransactionFactory factory = new WmqXATransactionFactory();
                factory.setIsolationLevel(isolationLevel.getKey());
                Transaction transaction = factory.getTransaction("testConn");
                assertThat(transaction, instanceOf(WmqXATransaction.class));
                transaction.begin();
                assertThat(connection.isolationLevel, is(isolationLevel.getValue()));
            } finally {
                DbConnectionContext.removeConnection("testConn");
            }
        }

        // 不正なアイソレーションレベルを指定した場合
        try {
            WmqXATransactionFactory factory = new WmqXATransactionFactory();
            factory.setIsolationLevel("UNKNOWN");
            fail("IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("invalid isolation level. isolation level:UNKNOWN"));
        }
    }

    private static final class MockBasicDbConnection extends BasicDbConnection {
        public int isolationLevel;
        public MockBasicDbConnection(Connection con) {
            super(con);
        }
        @Override
        public void setIsolationLevel(int level) {
            isolationLevel = level;
        }
    }

    private static final class MockWmqMessagingContext extends MockWmqMessagingContextSupport {
        @Override
        public void begin() {
        } 
    }
}
