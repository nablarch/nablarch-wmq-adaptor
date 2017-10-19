package nablarch.integration.messaging.wmq.provider;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;

import nablarch.fw.launcher.ProcessAbnormalEnd;
import nablarch.fw.messaging.MessagingException;
import nablarch.fw.messaging.ReceivedMessage;
import nablarch.fw.messaging.SendingMessage;
import nablarch.fw.messaging.provider.MessagingExceptionFactory;
import nablarch.fw.messaging.provider.exception.BasicMessagingExceptionFactory;
import nablarch.fw.messaging.provider.exception.MomConnectionException;

import org.junit.Test;

public class WmqMessagingProviderTest {

    /**
     * 設定に従い初期化されること。
     */
    @Test
    public void testInitialize() {

        OutputStreamWriter tempMqLog = MQException.log;

        try {

            // デフォルト設定の場合

            WmqMessagingProvider provider = new WmqMessagingProvider();
            provider.initialize();
            assertThat(MQEnvironment.properties.get(CMQC.TRANSPORT_PROPERTY).toString(),
                       is(CMQC.TRANSPORT_MQSERIES_BINDINGS));
            assertThat(MQEnvironment.properties.get(CMQC.THREAD_AFFINITY_PROPERTY).toString(),
                       is(Boolean.TRUE.toString()));
            assertNull(MQException.log);

            MQException.log = tempMqLog;

            // 設定した場合

            provider = new WmqMessagingProvider();
            provider.setUseXa(false);
            provider.setUseProductSystemErrorOutput(true);

            provider.initialize();
            assertThat(MQEnvironment.properties.get(CMQC.TRANSPORT_PROPERTY).toString(),
                       is(CMQC.TRANSPORT_MQSERIES_BINDINGS));
            assertThat(MQEnvironment.properties.get(CMQC.THREAD_AFFINITY_PROPERTY).toString(),
                       is(Boolean.FALSE.toString()));
            assertNotNull(MQException.log);

        } finally {
            MQException.log = tempMqLog;
        }

        // 退避キュー名の設定
        final Set<String> queueNames = new HashSet<String>();
        WmqMessagingProvider provider = new WmqMessagingProvider() {
            @Override
            protected MQQueueManager createMQQueueManager() {
                return null;
            }
            @Override
            protected Map<String, MQQueue> getMQQueues(MQQueueManager mqQueueManager, Collection<String> queueNames, int openOptions) {
                return null;
            }
            @Override
            protected MQQueue getMQQueue(MQQueueManager mqQueueManager, String queueName, int openOptions) throws MQException {
                queueNames.add(queueName);
                return null;
            }
        };
        provider.setReceivedQueueName("FOO");
        provider.setPoisonQueueName("POI.FOO");
        provider.initialize();
        provider.createContext();
        assertThat(queueNames.toString(), containsString("POI.FOO"));

        // 退避キュー名パターンの設定
        queueNames.clear();
        provider = new WmqMessagingProvider() {
            @Override
            protected MQQueueManager createMQQueueManager() {
                return null;
            }
            @Override
            protected Map<String, MQQueue> getMQQueues(MQQueueManager mqQueueManager, Collection<String> queueNames, int openOptions) {
                return null;
            }
            @Override
            protected MQQueue getMQQueue(MQQueueManager mqQueueManager, String queueName, int openOptions) throws MQException {
                queueNames.add(queueName);
                return null;
            }
            
        };
        provider.setReceivedQueueName("HOGE");
        provider.setPoisonQueueNamePattern("%s-POISON");
        provider.initialize();
        provider.createContext();
        assertThat(queueNames.toString(), containsString("HOGE-POISON"));

        // 退避キューの設定不備
        provider = new WmqMessagingProvider();
        provider.setQueueManagerName("testQmgr");
        provider.setPoisonQueueName("invalidPqn");
        try {
            provider.initialize();
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(),
                       is("poison queue setting was invalid. if receivedQueueName was not set, must not be set poisonQueueNamePattern and poisonQueueName. "
                        + "queueManagerName = [testQmgr], poisonQueueNamePattern = [null], poisonQueueName = [invalidPqn]"));
        }
        provider = new WmqMessagingProvider();
        provider.setQueueManagerName("testQmgr");
        provider.setPoisonQueueNamePattern("invalidPqnp");
        try {
            provider.initialize();
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(),
                       is("poison queue setting was invalid. if receivedQueueName was not set, must not be set poisonQueueNamePattern and poisonQueueName. "
                        + "queueManagerName = [testQmgr], poisonQueueNamePattern = [invalidPqnp], poisonQueueName = [null]"));
        }
        provider = new WmqMessagingProvider();
        provider.setQueueManagerName("testQmgr");
        provider.setReceivedQueueName("testRecv");
        provider.setPoisonQueueName("invalidPqn");
        provider.setPoisonQueueNamePattern("invalidPqnp");
        try {
            provider.initialize();
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(),
                       is("poison queue setting was invalid. if receivedQueueName was set, must be set either poisonQueueNamePattern or poisonQueueName "
                        + "or must not be set both poisonQueueNamePattern and poisonQueueName. "
                        + "queueManagerName = [testQmgr], poisonQueueNamePattern = [invalidPqnp], poisonQueueName = [invalidPqn]"));
        }
    }

    /**
     * キュー名の指定がない場合に例外が送出されないこと。
     */
    @Test
    public void testGetMQQueue() throws MQException {

        WmqMessagingProvider provider = new WmqMessagingProvider();
        Map<String, MQQueue> qs = provider.getMQQueues(null, Collections.<String>emptyList(), 999);
        assertThat(qs.size(), is(0));

        String queueName = null;
        MQQueue q = provider.getMQQueue(null, queueName, 999);
        assertNull(q);
    }

    /**
     * ポイズンメッセージの処理が正常に動作すること。
     */
    @Test
    public void testPoison() {

        WmqMessagingProvider provider = new WmqMessagingProvider();
        MQMessage m = new MQMessage();

        // バックアウト回数の上限値がデフォルトの場合
        m.backoutCount = 0;
        assertThat(false, is(provider.isPoisonMessage(m)));
        m.backoutCount = 1;
        assertThat(true, is(provider.isPoisonMessage(m)));

        // バックアウト回数の上限値がデフォルトの場合
        provider = new WmqMessagingProvider();
        provider.setBackoutLimit(3);
        m.backoutCount = 2;
        assertThat(false, is(provider.isPoisonMessage(m)));
        m.backoutCount = 3;
        assertThat(false, is(provider.isPoisonMessage(m)));
        m.backoutCount = 4;
        assertThat(true, is(provider.isPoisonMessage(m)));

        // 退避キューが指定されていない場合(順序保障あり)
        provider = new WmqMessagingProvider();
        try {
            provider.setBackoutLimitExceededFailureCode("dummy");
            provider.processPoisonMessage(null, m);
            fail();
        } catch (ProcessAbnormalEnd e) {
            assertThat(e.getStatusCode(), is(190));
            assertNotNull(e.getMessageId());
        }
        provider = new WmqMessagingProvider();
        provider.setBackoutLimitExceededExitCode(199);
        provider.setBackoutLimitExceededFailureCode("HOGEHOGE");
        try {
            provider.processPoisonMessage(null, m);
            fail();
        } catch (ProcessAbnormalEnd e) {
            assertThat(e.getStatusCode(), is(199));
            assertThat(e.getMessageId(), is("HOGEHOGE"));
        }
    }

    /**
     * {@link WmqMessagingContext}の生成時に{@link MQException}を捕捉した場合に、
     * {@link MessagingExceptionFactory#createMessagingException(String, Throwable)}
     * に処理を委譲していること。
     */
    @Test
    public void testCreateContextCatchMQException() {

        // MQQueueManagerの生成時にMQExceptionが送出された場合

        WmqMessagingProvider provider = new WmqMessagingProvider() {
            @Override
            protected MQQueueManager createMQQueueManager() throws MQException {
                throw new MQException(CMQC.MQCC_FAILED, CMQC.MQRC_Q_MGR_NOT_AVAILABLE, null);
            }
        };
        try {
            provider.createContext();
            fail("MomConnectionException");
        } catch (MomConnectionException e) {
            assertThat(((MQException) e.getCause()).reasonCode, is(CMQC.MQRC_Q_MGR_NOT_AVAILABLE));
        }

        // MQQueueへのアクセス時にMQExceptionが送出された場合

        provider = new WmqMessagingProvider() {
            @Override
            protected MQQueueManager createMQQueueManager() throws MQException {
                return null;
            }
            @Override
            protected MQQueue getMQQueue(MQQueueManager mqQueueManager, String queueName, int openOptions)
                    throws MQException {
                throw new MQException(CMQC.MQCC_FAILED, CMQC.MQRC_OPEN_FAILED, null);
            }
        };
        try {
            provider.createContext();
            fail("MomConnectionException");
        } catch (MomConnectionException e) {
            assertThat(((MQException) e.getCause()).reasonCode, is(CMQC.MQRC_OPEN_FAILED));
        }

        // MessagingExceptionFactoryを変更した場合
        provider = new WmqMessagingProvider() {
            @Override
            protected MQQueueManager createMQQueueManager() throws MQException {
                throw new MQException(CMQC.MQCC_FAILED, CMQC.MQRC_CONNECTION_BROKEN, null);
            }
        };
        provider.setMessagingExceptionFactory(new BasicMessagingExceptionFactory());
        try {
            provider.createContext();
            fail("MessagingException");
        } catch (MessagingException e) {
            assertThat(((MQException) e.getCause()).reasonCode, is(CMQC.MQRC_CONNECTION_BROKEN));
        }
    }

    /**
     * オプションのデフォルト値を取得できること。
     */
    @Test
    public void testOptions() {

        // 送信先MQQueueのオープンを制御するオプション

        WmqMessagingProvider provider = new WmqMessagingProvider();
        assertThat(provider.getSendingQueueOpenOptions(),
                   is(CMQC.MQOO_OUTPUT | CMQC.MQOO_FAIL_IF_QUIESCING));

        // ポイズンメッセージ送信先MQQueueのオープンを制御するオプション

        provider = new WmqMessagingProvider();
        assertThat(provider.getPoisonQueueOpenOptions(),
                   is(CMQC.MQOO_OUTPUT | CMQC.MQOO_FAIL_IF_QUIESCING));

        // 受信先MQQueueのオープンを制御するオプション

        provider = new WmqMessagingProvider();
        assertThat(provider.getReceivedQueueOpenOptions(),
                   is(CMQC.MQOO_INPUT_SHARED | CMQC.MQOO_FAIL_IF_QUIESCING));

        // MQQueueへのメッセージ書き込みを制御するオプション

        /* XA使用する */
        provider = new WmqMessagingProvider();
        MQPutMessageOptions putOpts = provider.getPutMessageOptions();
        assertThat(putOpts.options, is(CMQC.MQPMO_SYNCPOINT | CMQC.MQPMO_NEW_MSG_ID));

        provider = new WmqMessagingProvider();
        putOpts = provider.getPutPoisonMessageOptions();
        assertThat(putOpts.options, is(CMQC.MQPMO_SYNCPOINT | CMQC.MQPMO_NEW_MSG_ID));

        /* XA使用しない */
        provider = new WmqMessagingProvider();
        provider.setUseXa(false);
        putOpts = provider.getPutMessageOptions();
        assertThat(putOpts.options, is(CMQC.MQPMO_NO_SYNCPOINT | CMQC.MQPMO_NEW_MSG_ID));

        provider = new WmqMessagingProvider();
        provider.setUseXa(false);
        putOpts = provider.getPutPoisonMessageOptions();
        assertThat(putOpts.options, is(CMQC.MQPMO_NO_SYNCPOINT | CMQC.MQPMO_NEW_MSG_ID));

        // MQQueueからのメッセージ読み取りを制御するオプション

        /* XA使用する、メッセージID指定あり、タイムアウト1以上 */
        provider = new WmqMessagingProvider();
        MQGetMessageOptions getOpts = provider.getGetMessageOptions("test", 100);
        assertThat(getOpts.options, is(CMQC.MQGMO_SYNCPOINT | CMQC.MQGMO_WAIT));
        assertThat(getOpts.matchOptions, is(CMQC.MQMO_MATCH_CORREL_ID));
        assertThat(getOpts.waitInterval, is(100));

        /* XA使用しない、メッセージID指定なし、タイムアウト0以下 */
        provider = new WmqMessagingProvider();
        provider.setUseXa(false);
        getOpts = provider.getGetMessageOptions("", 0);
        assertThat(getOpts.options, is(CMQC.MQGMO_NO_SYNCPOINT | CMQC.MQGMO_WAIT));
        assertThat(getOpts.matchOptions, is(CMQC.MQMO_NONE));
        assertThat(getOpts.waitInterval, is(300 * 1000)); // WmqMessagingProviderのデフォルト値
    }

    /**
     * MQQueuManagerの接続遮断とMQQueueのクローズで例外が送出されないこと。
     */
    @Test
    public void testDisconnectAndClose() {

        WmqMessagingProvider provider = new WmqMessagingProvider();

        provider.disconnect(null);
        MQQueue q = null;
        provider.close(q);
        Map<String, MQQueue> map = null;
        provider.close(map);
    }

    /**
     * XA設定に不備がある場合に例外が送出されること。
     */
    @Test
    public void testIncorrectXAConfiguration() throws Exception {

        WmqMessagingProvider provider = new WmqMessagingProvider();

        provider.setUseXa(false);

        String expectedMsg = "XA configuration was incorrect. "
                           + "must be set true to WmqMessagingProvider's useXa property";

        try {
            provider.getJdbcConnection(null, null);
            fail("IllegalStateException");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is(expectedMsg));
        }
        try {
            provider.begin(null);
            fail("IllegalStateException");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is(expectedMsg));
        }
        try {
            provider.commit(null);
            fail("IllegalStateException");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is(expectedMsg));
        }
        try {
            provider.backout(null);
            fail("IllegalStateException");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is(expectedMsg));
        }
    }

    /**
     * キューへのメッセージ書き込み時に例外を捕捉した場合に正しく処理されること。
     */
    @Test
    public void testPutMessageCatchException() {

        // MQExceptionを捕捉した場合
        WmqMessagingProvider provider = new WmqMessagingProvider();
        provider.setMqmdFieldsOperator(new BasicWmqMqmdFieldsOperator() {
            @Override
            public void setFieldsBeforeSend(
                    SendingMessage sendingMessage, MQMessage mqMessage, long defaultTimeToLive)
                    throws MQException {
                throw new MQException(CMQC.MQCC_FAILED, CMQC.MQRC_Q_MGR_NOT_AVAILABLE, null);
            }
        });
        try {
            provider.putMessage(null, null);
            fail("MomConnectionException");
        } catch (MomConnectionException e) {
            assertThat(((MQException) e.getCause()).reasonCode, is(CMQC.MQRC_Q_MGR_NOT_AVAILABLE));
        }
    }

    /**
     * キューからメッセージ取得時に例外を捕捉した場合に正しく処理されること。
     */
    @Test
    public void testGetMessageCatchException() {

        // MQExceptionを捕捉した場合(受信メッセージなし)
        WmqMessagingProvider provider = new WmqMessagingProvider();
        provider.setMqmdFieldsOperator(new BasicWmqMqmdFieldsOperator() {
            @Override
            public void setFieldsBeforeReceive(
                    String messageId, MQMessage mqMessage) throws MQException {
                throw new MQException(CMQC.MQCC_FAILED, CMQC.MQRC_NO_MSG_AVAILABLE, null);
            }
        });
        ReceivedMessage receivedMessage = provider.getMessage(null, null, 999, null);
        assertNull(receivedMessage);

        // MQExceptionを捕捉した場合(接続エラー)
        provider = new WmqMessagingProvider();
        provider.setMqmdFieldsOperator(new BasicWmqMqmdFieldsOperator() {
            @Override
            public void setFieldsBeforeReceive(
                    String messageId, MQMessage mqMessage) throws MQException {
                throw new MQException(CMQC.MQCC_FAILED, CMQC.MQRC_Q_MGR_QUIESCING, null);
            }
        });
        try {
            provider.getMessage(null, null, 999, null);
            fail("MomConnectionException");
        } catch (MomConnectionException e) {
            assertThat(((MQException) e.getCause()).reasonCode, is(CMQC.MQRC_Q_MGR_QUIESCING));
        }
    }
}
