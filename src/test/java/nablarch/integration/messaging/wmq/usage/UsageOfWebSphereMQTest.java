package nablarch.integration.messaging.wmq.usage;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.XADataSource;

import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.util.BinaryUtil;
import nablarch.core.util.StringUtil;
import nablarch.test.support.SystemRepositoryResource;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/**
 * WebSphere MQの使用方法のテスト。
 * @author Kiyohito Itoh
 */
public class UsageOfWebSphereMQTest {
    
    @Rule
    public final SystemRepositoryResource systemRepositoryResource = new SystemRepositoryResource("db-default.xml");

    private static final int LOOP = 10;
    private static final int TRANSACTION = 10;

    @Before
    public void setUp() throws Exception {
        initDb();
    }

    @Test
    @Ignore
    public void single() throws Exception {
        MQQueueManager qm = null;
        Connection con = null;
        try {
            qm = createMQQueueManager();
            MQQueue q = getMQQueue(qm);
            XADataSource xads = systemRepositoryResource.getComponent("xaDataSource");
            con = qm.getJDBCConnection(xads);
            con.setAutoCommit(false);
            qm.begin();
            String msg = "single_test1";
            putMessage(q, msg);
            insertMessage(con, msg);
            qm.commit();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (qm != null) {
                qm.disconnect();
            }
        }
    }

    @Test
    @Ignore
    public void multiple() throws Exception {

        MQQueueManager qm = null;
        Connection con = null;

        XADataSource xads = systemRepositoryResource.getComponent("xaDataSource");

        long start = System.currentTimeMillis();

        for (int i = 0; i < LOOP; i++) {
            try {

                qm = createMQQueueManager();
                MQQueue q = getMQQueue(qm);
                con = qm.getJDBCConnection(xads);
                con.setAutoCommit(false);

                for (int j = 0; j < TRANSACTION; j++) {

                    qm.begin();

                    String msg = "multiple_test_" + i + "_" + j;
                    String mid = putMessage(q, msg);
                    System.out.println("mid: " + mid);
                    insertMessage(con, msg);
                    if (j % 2 != 0) {
                        qm.backout();
                        System.out.println("backout: " + msg);
                    } else {
                        qm.commit();
                        System.out.println("commit: " + msg);
                    }
                    qm.close();
                }
    
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            } finally {
                if (qm != null) {
                    try {
                        qm.disconnect();
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            }
        }

        long end = System.currentTimeMillis();

        System.out.println("TIME(msec): " + (end - start));
    }

    private static final Charset CHARSET = Charset.forName("UTF-8");

    protected String putMessage(MQQueue q, String message) throws IOException, MQException {
        MQMessage m = new MQMessage();
        m.write(StringUtil.getBytes(message, CHARSET));
        MQPutMessageOptions opts = new MQPutMessageOptions();
        opts.options = CMQC.MQPMO_SYNCPOINT | CMQC.MQPMO_NEW_MSG_ID;
        q.put(m, opts);
        return BinaryUtil.convertToHexString(m.messageId);
    }

    @SuppressWarnings("unchecked")
    protected MQQueueManager createMQQueueManager() throws MQException {
        MQEnvironment.properties.put(CMQC.TRANSPORT_PROPERTY, CMQC.TRANSPORT_MQSERIES_BINDINGS); // バインディングモード接続
        MQEnvironment.properties.put(CMQC.THREAD_AFFINITY_PROPERTY, true); // スレッド類縁性
        return new MQQueueManager("TEST");
    }

    protected MQQueue getMQQueue(MQQueueManager qm) throws MQException {
        return qm.accessQueue("SEND_TEST", CMQC.MQOO_OUTPUT | CMQC.MQOO_FAIL_IF_QUIESCING);
    }

    protected void insertMessage(Connection con, String message) throws SQLException {

        PreparedStatement stmt = con.prepareStatement("SELECT COUNT(*) AS RS_COUNT FROM WMQ_TEST");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        int count = rs.getInt("RS_COUNT");
        stmt.close();

        stmt = con.prepareStatement("INSERT INTO WMQ_TEST VALUES (?, ?)");
        stmt.setInt(1, ++count);
        stmt.setString(2, message);
        stmt.executeUpdate();
        stmt.close();
    }

    protected void initDb() throws SQLException {
        final ConnectionFactory connectionFactory = systemRepositoryResource.getComponentByType(ConnectionFactory.class);
        final Connection con = connectionFactory.getConnection(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY)
                                                       .getConnection();

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
