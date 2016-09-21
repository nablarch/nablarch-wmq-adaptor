package nablarch.integration.messaging.wmq.tool;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;

import nablarch.core.util.BinaryUtil;
import nablarch.core.util.StringUtil;
import oracle.jdbc.pool.OracleDataSource;

import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;

/**
 * ツール用のユーティリティ。
 * @author Kiyohito Itoh
 */
public class ToolUtil {

    private static final String DB_URL = "jdbc:oracle:thin:@localhost:1521/xe";
    private static final String DB_USER = "nablarch";
    private static final String DB_PASSWORD = "nablarch";

    public static Connection getConnection() throws SQLException {
        OracleDataSource ds = new OracleDataSource();
        ds.setURL(DB_URL);
        ds.setUser(DB_USER);
        ds.setPassword(DB_PASSWORD);
        return ds.getConnection();
    }

    public static void close(Connection con) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static final Charset MESSAGE_ID_CHARSET = Charset.forName("ISO-8859-1");

    public static final Charset MESSAGE_CHARSET = Charset.forName("MS932");

    public static byte[] putResponseMessage(String queueManagerName, String queueName,
                                              String message, byte[] messageId)
            throws MQException, IOException {
        return putMessage(queueManagerName, queueName, message, messageId, null);
    }

    public static byte[] putRequestMessage(String queueManagerName, String queueName,
                                             String message, String replyTo)
            throws MQException, IOException {
        return putMessage(queueManagerName, queueName, message, CMQC.MQMI_NONE, replyTo);
    }

    public static byte[] rePutRequestMessage(String queueManagerName, String queueName,
                                               String message, String replyTo, byte[] messageId)
            throws MQException, IOException {
        return putMessage(queueManagerName, queueName, message, messageId, replyTo);
    }

    /**
     * キューにメッセージを書き込む。
     * @param queueManagerName キューマネージャ名称
     * @param queueName キュー名
     * @param message メッセージ
     * @param messageId 相関IDに指定するメッセージID。指定しない場合は{@link CMQC#MQMI_NONE}
     * @param useXa 分散トランザクションを使用する場合はtrue。
     * @return メッセージID
     * @throws MQException MQ例外
     * @throws IOException IO例外
     */
    private static byte[] putMessage(String queueManagerName, String queueName,
                                       String message, byte[] messageId, String replyTo)
            throws MQException, IOException {
        MQQueueManager qm = null;
        try {
            qm = createMQQueueManager(queueManagerName);
            MQQueue q = qm.accessQueue(queueName, CMQC.MQOO_OUTPUT | CMQC.MQOO_FAIL_IF_QUIESCING);

            MQMessage m = new MQMessage();


            m.write(StringUtil.getBytes(message, MESSAGE_CHARSET));

            MQPutMessageOptions opts = new MQPutMessageOptions();
            opts.options = CMQC.MQPMO_NO_SYNCPOINT | CMQC.MQPMO_NEW_MSG_ID;

            if (messageId != CMQC.MQMI_NONE) {
                m.messageType = CMQC.MQMT_REPLY;
                m.correlationId = messageId;
            }

            if (StringUtil.hasValue(replyTo)) {
                m.messageType = CMQC.MQMT_REQUEST;
                m.replyToQueueName = replyTo;
            }

            m.expiry = CMQC.MQEI_UNLIMITED;
            q.put(m, opts);
            close(q);

            System.out.println("messageId\n[" + BinaryUtil.convertToHexString(m.messageId) + "]");
            System.out.println("correlationId\n[" + BinaryUtil.convertToHexString(m.correlationId) + "]");
            System.out.println("put message\n[" + message + "]");

            return m.messageId;

        } catch (MQException e) {
            e.printStackTrace();
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } finally {
            disconnect(qm);
        }
    }

    /**
     * キューの先頭メッセージのメッセージIDを取得する。
     * @param queueManagerName キューマネージャ名称
     * @param queueName キュー名
     * @return キューの先頭メッセージのメッセージID。取得できない場合は{@link CMQC#MQMI_NONE}
     * @throws MQException MQ例外
     */
    public static byte[] getFirstMessageId(String queueManagerName, String queueName)
            throws MQException {
        MQQueueManager qm = null;
        try {
            qm = createMQQueueManager(queueManagerName);
            MQQueue q = qm.accessQueue(queueName, CMQC.MQOO_BROWSE | CMQC.MQOO_INPUT_SHARED | CMQC.MQOO_FAIL_IF_QUIESCING);
            MQMessage m = new MQMessage();
            MQGetMessageOptions opts = new MQGetMessageOptions();
            opts.options = CMQC.MQGMO_BROWSE_FIRST + CMQC.MQGMO_NO_WAIT;
            opts.matchOptions = CMQC.MQMO_NONE;
            q.get(m, opts);
            close(q);
            return m.messageId;
        } catch (MQException e) {
            if (e.reasonCode == CMQC.MQRC_NO_MSG_AVAILABLE) {
                return CMQC.MQMI_NONE;
            }
            e.printStackTrace();
            throw e;
        } finally {
            disconnect(qm);
        }
    }

    private static void close(MQQueue q) {
        try {
            q.close();
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    private static void disconnect(MQQueueManager qm) {
        try {
            qm.disconnect();
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    public static MQQueueManager createMQQueueManager(String queueManagerName) throws MQException {
        MQEnvironment.properties.put(CMQC.TRANSPORT_PROPERTY, CMQC.TRANSPORT_MQSERIES_BINDINGS); // バインディングモード接続
        MQEnvironment.properties.put(CMQC.THREAD_AFFINITY_PROPERTY, true); // スレッド類縁性
        return new MQQueueManager(queueManagerName);
    }
}
