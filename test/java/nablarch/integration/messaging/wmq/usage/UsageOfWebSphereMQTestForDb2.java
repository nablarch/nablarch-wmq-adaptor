package nablarch.integration.messaging.wmq.usage;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XADataSource;

import nablarch.test.support.tool.HereisDb;

import com.ibm.db2.jcc.DB2SimpleDataSource;
import com.ibm.db2.jcc.DB2XADataSource;
import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;

public class UsageOfWebSphereMQTestForDb2 extends UsageOfWebSphereMQTestSupport {

    protected XADataSource createXADataSource() throws SQLException {
        DB2XADataSource xads = new DB2XADataSource();
        xads.setServerName("localhost");
        xads.setPortNumber(50000);
        xads.setDatabaseName("tutorial");
        xads.setUser("nablarch");
        xads.setPassword("nablarch");
        return xads;
    }

    protected void initDb() throws SQLException {
        DB2SimpleDataSource ds = new DB2SimpleDataSource();
        ds.setServerName("localhost");
        ds.setPortNumber(50000);
        ds.setDatabaseName("tutorial");
        ds.setUser("nablarch");
        ds.setPassword("nablarch");
        Connection con = ds.getConnection();
        con.setAutoCommit(true);
        try {
            HereisDb.sql(con);
            /*
            DROP TABLE WMQ_TEST;
             */
        } catch (Exception e) {
            // nop
        }
        try {
            HereisDb.sql(con);
            /*
            CREATE TABLE WMQ_TEST (ID CHAR(10) NOT NULL, MESSAGE VARCHAR(100));
            ALTER TABLE WMQ_TEST ADD PRIMARY KEY (ID);
             */
        } catch (RuntimeException e) {
            throw e;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected MQQueueManager createMQQueueManager() throws MQException {
        MQEnvironment.properties.put(CMQC.TRANSPORT_PROPERTY, CMQC.TRANSPORT_MQSERIES_BINDINGS); // バインディングモード接続
        MQEnvironment.properties.put(CMQC.THREAD_AFFINITY_PROPERTY, true); // スレッド類縁性
        return new MQQueueManager("TEST_DB2");
    }
}
