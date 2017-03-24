package nablarch.integration.messaging.wmq.provider.exception;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import java.io.OutputStreamWriter;

import nablarch.fw.messaging.MessagingException;
import nablarch.fw.messaging.provider.exception.MomConnectionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.CMQC;

public class BasicWmqMessagingExceptionFactoryTest {

    private OutputStreamWriter tempMqLog;

    @Before
    public void setUp() {
        tempMqLog = MQException.log;
        MQException.log = null;
    }

    @After
    public void tearDown() {
        MQException.log = tempMqLog;
    }

    /**
     * 発生した例外がMOM接続に関する問題である場合に、{@link MomConnectionException}が生成されること。
     */
    @Test
    public void testCatchReasonCodeForMomConnectionError() {

        int[] reasonCodes = new int[] {
                CMQC.MQRC_CONNECTION_BROKEN,
                CMQC.MQRC_CONNECTION_QUIESCING,
                CMQC.MQRC_CONNECTION_STOPPING,
                CMQC.MQRC_OPEN_FAILED,
                CMQC.MQRC_Q_MGR_NOT_AVAILABLE,
                CMQC.MQRC_Q_MGR_QUIESCING,
                CMQC.MQRC_Q_MGR_STOPPING,
                CMQC.MQRC_NO_EXTERNAL_PARTICIPANTS,
                CMQC.MQRC_UNEXPECTED_ERROR
        };

        BasicWmqMessagingExceptionFactory factory = new BasicWmqMessagingExceptionFactory();
        for (int reasonCode : reasonCodes) {
            Throwable cause = new MQException(CMQC.MQCC_FAILED, reasonCode, null);
            MessagingException e = factory.createMessagingException("dummy", cause);
            assertThat(e, instanceOf(MomConnectionException.class));
            assertSame(e.getCause(), cause);
        }

        // causeにMQExceptionが設定されている場合
        Throwable cause = new RuntimeException("fuga",
                new IllegalArgumentException("hoge",
                        new MQException(CMQC.MQCC_FAILED, CMQC.MQRC_OPEN_FAILED, null)));
        MessagingException e = factory.createMessagingException("dummy", cause);
        assertThat(e, instanceOf(MomConnectionException.class));
        assertSame(e.getCause(), cause);
    }

    /**
     * 発生した例外がMOM接続に関する問題でない場合に、{@link MessagingException}が生成されること。
     */
    @Test
    public void testCatchReasonCodeForNonMomConnectionError() {

        BasicWmqMessagingExceptionFactory factory = new BasicWmqMessagingExceptionFactory();
        Throwable cause = new MQException(CMQC.MQCC_FAILED, CMQC.MQRC_BUFFER_ERROR, null);
        MessagingException e = factory.createMessagingException("dummy", cause);
        assertThat(e, instanceOf(MessagingException.class));
        assertSame(e.getCause(), cause);

        // causeにMQExceptionが設定されている場合
        cause = new RuntimeException("fuga",
                new IllegalArgumentException("hoge",
                        new MQException(CMQC.MQCC_FAILED, CMQC.MQRC_SECURITY_ERROR, null)));
        e = factory.createMessagingException("dummy", cause);
        assertThat(e, instanceOf(MessagingException.class));
        assertSame(e.getCause(), cause);
    }
}
