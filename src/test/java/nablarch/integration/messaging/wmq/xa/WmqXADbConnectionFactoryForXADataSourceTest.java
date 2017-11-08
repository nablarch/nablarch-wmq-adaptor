package nablarch.integration.messaging.wmq.xa;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;

import nablarch.core.db.DbAccessException;
import nablarch.core.db.connection.DbAccessExceptionFactory;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.connection.exception.DbConnectionException;
import nablarch.fw.messaging.MessagingContext;
import nablarch.integration.messaging.wmq.MockWmqMessagingContextSupport;
import nablarch.integration.messaging.wmq.provider.WmqMessagingContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link WmqXADbConnectionFactoryForXADataSource}のテスト。
 * @author Kiyohito Itoh
 */
@RunWith(DatabaseTestRunner.class)
@TargetDb(exclude = TargetDb.Db.DB2)
public class WmqXADbConnectionFactoryForXADataSourceTest {
    
    @Rule
    public final SystemRepositoryResource systemRepositoryResource = new SystemRepositoryResource("db-default.xml");

    /**
     * {@link WmqMessagingContext#getJdbcConnection(XADataSource)}を使用して
     * {@link java.sql.Connection}を取得していること。
     */
    @Test
    public void testGetConnection() throws Exception {

        final XADataSource xaDataSource = systemRepositoryResource.getComponent("xaDataSource");
        MockWmqMessagingContext mockContext = new MockWmqMessagingContext();
        MessagingContext.attach(mockContext);

        WmqXADbConnectionFactoryForXADataSource factory = new WmqXADbConnectionFactoryForXADataSource();
        factory.setXaDataSource(xaDataSource);

        try {
            TransactionManagerConnection connection = factory.getConnection("test");
            assertNotNull(mockContext.xaConnection);
            assertThat(connection, instanceOf(WmqXADbConnection.class));
        } finally {
            MessagingContext.detach();
            if (mockContext != null && mockContext.xaConnection != null) {
                mockContext.xaConnection.close();
            }
        }
    }

    private static final class MockWmqMessagingContext extends WmqMessagingContext {
        private XAConnection xaConnection;
        public MockWmqMessagingContext() {
            super(null, null, null, null, null);
        }
        public Connection getJdbcConnection(XADataSource xaDataSource) throws SQLException {
            xaConnection = xaDataSource.getXAConnection();
            return xaConnection.getConnection();
        }
    }

    /**
     * {@link SQLException}を捕捉した場合に、
     * {@link DbAccessExceptionFactory#createDbAccessException(String, SQLException, nablarch.core.db.connection.AppDbConnection)}
     * に処理を委譲していること。
     */
    @Test
    public void testCatchSQLException() {

        MockWmqMessagingContextThrowSQLException mockContext = new MockWmqMessagingContextThrowSQLException(999);
        MessagingContext.attach(mockContext);

        WmqXADbConnectionFactoryForXADataSource factory = new WmqXADbConnectionFactoryForXADataSource();
        factory.setDbAccessExceptionFactory(new MockDbAccessExceptionFactory());

        try {
            factory.getConnection("test");
            fail("DbConnectionException");
        } catch (DbConnectionException e) {
            assertThat(e.getMessage(), is("db connection error occurred."));
            assertThat(e.getCause(), instanceOf(SQLException.class));
            assertThat(((SQLException) e.getCause()).getErrorCode(), is(999));
        } finally {
            MessagingContext.detach();
        }

        mockContext = new MockWmqMessagingContextThrowSQLException(100);
        MessagingContext.attach(mockContext);

        factory = new WmqXADbConnectionFactoryForXADataSource();
        factory.setDbAccessExceptionFactory(new MockDbAccessExceptionFactory());

        try {
            factory.getConnection("test");
            fail("DbAccessException");
        } catch (DbAccessException e) {
            assertThat(e.getMessage(), is("db access error occurred."));
            assertThat(e, not(instanceOf(DbConnectionException.class)));
            assertThat(e.getCause(), instanceOf(SQLException.class));
            assertThat(((SQLException) e.getCause()).getErrorCode(), is(100));
        } finally {
            MessagingContext.detach();
        }
    }

    /**
     * {@link XAException}を捕捉した場合に、{@link RuntimeException}でラップして再送出していること。
     */
    @Test
    public void testCatchXAException() {

        MockWmqMessagingContextThrowXAException mockContext = new MockWmqMessagingContextThrowXAException();
        MessagingContext.attach(mockContext);

        WmqXADbConnectionFactoryForXADataSource factory = new WmqXADbConnectionFactoryForXADataSource();
        factory.setDbAccessExceptionFactory(new MockDbAccessExceptionFactory());

        try {
            factory.getConnection("test");
            fail("RuntimeException");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("failed to get database connection."));
            assertThat(e.getCause(), instanceOf(XAException.class));
            assertThat(((XAException) e.getCause()).getMessage(), is("testXa"));
        } finally {
            MessagingContext.detach();
        }
    }

    private static final class MockDbAccessExceptionFactory implements DbAccessExceptionFactory {
        @Override
        public DbAccessException createDbAccessException(String message, SQLException cause, TransactionManagerConnection connection) {
            if (cause.getErrorCode() == 999) {
                throw new DbConnectionException("db connection error occurred.", cause);
            }
            return new DbAccessException("db access error occurred.", cause);
        }
    }

    private static final class MockWmqMessagingContextThrowSQLException extends MockWmqMessagingContextSupport {
        private int errorCode;
        public MockWmqMessagingContextThrowSQLException(int errorCode) {
            this.errorCode = errorCode;
        }
        public Connection getJdbcConnection(XADataSource xaDataSource) throws SQLException {
            throw new SQLException("testReason", "testSqlState", errorCode);
        }
    }

    private static final class MockWmqMessagingContextThrowXAException extends MockWmqMessagingContextSupport {
        public Connection getJdbcConnection(XADataSource xaDataSource) throws XAException {
            throw new XAException("testXa");
        }
    }
}
