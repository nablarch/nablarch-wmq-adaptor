package nablarch.integration.messaging.wmq.xa;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.SQLException;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;

import org.junit.Rule;
import org.junit.Test;

/**
 * {@link WmqXADbConnection}のテスト。
 * @author Kiyohito Itoh
 */
public class WmqXADbConnectionTest {

    @Rule
    public final SystemRepositoryResource systemRepositoryResource = new SystemRepositoryResource("db-default.xml");

    /**
     * 分散トランザクション対応でコミット、ロールバック、クローズがされないこと。
     */
    @Test
    public void doNothingForXA() throws SQLException {

        final ConnectionFactory connectionFactory = systemRepositoryResource.getComponentByType(ConnectionFactory.class);
        Connection jdbcConnection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY).getConnection();

        try {

            WmqXADbConnection connection = new WmqXADbConnection(jdbcConnection);
            connection.terminate();
            assertThat(jdbcConnection.isClosed(), is(false));

            jdbcConnection.close();
            assertThat(jdbcConnection.isClosed(), is(true));

            connection.commit();
            connection.rollback();

        } finally {
            if (!jdbcConnection.isClosed()) {
                jdbcConnection.close();
            }
        }
    }
}
