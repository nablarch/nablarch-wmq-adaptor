package nablarch.integration.messaging.wmq.usage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ResourceBundle;

import javax.sql.XADataSource;

import oracle.jdbc.pool.OracleDataSource;
import oracle.jdbc.xa.client.OracleXADataSource;

public class UsageOfWebSphereMQTestForOracle extends UsageOfWebSphereMQTestSupport {

    protected XADataSource createXADataSource() throws SQLException {
        OracleXADataSource xads = new OracleXADataSource();
        xads.setURL("jdbc:oracle:thin:@//localhost:1521/XE");
        xads.setUser("nablarch");
        xads.setPassword("nablarch");
        return xads;
    }

    protected void initDb() throws SQLException {
        ResourceBundle rb = ResourceBundle.getBundle("db");
        OracleDataSource ds = new OracleDataSource();
        ds.setURL(rb.getString("db.url"));
        ds.setUser(rb.getString("db.user"));
        ds.setPassword(rb.getString("db.password"));
        Connection con = ds.getConnection();
        try {
            PreparedStatement statement = con.prepareStatement("DROP TABLE WMQ_TEST");
            statement.execute();
            statement.close();
        } catch (Exception e) {
            // nop
        }
        try {
            PreparedStatement statement = con.prepareStatement("CREATE TABLE WMQ_TEST (ID CHAR(10), MESSAGE VARCHAR(100), PRIMARY KEY(ID))");
            statement.execute();
            statement.close();
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

}
