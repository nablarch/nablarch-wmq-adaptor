package nablarch.integration.messaging.wmq.xa;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ResourceBundle;

import oracle.jdbc.pool.OracleDataSource;

import org.junit.Test;

/**
 * {@link WmqXADbConnection}のテスト。
 * @author Kiyohito Itoh
 */
public class WmqXADbConnectionTest {

    /**
     * 分散トランザクション対応でコミット、ロールバック、クローズがされないこと。
     */
    @Test
    public void doNothingForXA() throws SQLException {

        ResourceBundle rb = ResourceBundle.getBundle("db");
        OracleDataSource ds = new OracleDataSource();
        ds.setURL(rb.getString("db.url"));
        ds.setUser(rb.getString("db.user"));
        ds.setPassword(rb.getString("db.password"));

        Connection jdbcConnection = ds.getConnection();

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
