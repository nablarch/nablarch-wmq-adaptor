package nablarch.integration.messaging.wmq.provider;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.nio.charset.Charset;

import nablarch.core.repository.SystemRepository;
import nablarch.core.repository.di.DiContainer;
import nablarch.core.repository.di.config.xml.XmlComponentDefinitionLoader;
import nablarch.core.util.BinaryUtil;
import nablarch.core.util.StringUtil;
import nablarch.fw.messaging.FwHeader;
import nablarch.fw.messaging.ReceivedMessage;
import nablarch.fw.messaging.RequestMessage;
import nablarch.fw.messaging.ResponseMessage;
import nablarch.fw.messaging.SendingMessage;
import nablarch.integration.messaging.wmq.FixedSystemTimeProvider;
import nablarch.test.support.SystemRepositoryResource;

import org.hamcrest.CoreMatchers;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.constants.CMQC;

/**
 * {@link BasicWmqMqmdFieldsOperator}のテスト
 * @author Kiyohito Itoh
 */
public class BasicWmqMqmdFieldsOperatorTest {
    
    @Rule
    public SystemRepositoryResource systemRepositoryResource = new SystemRepositoryResource(
            "nablarch/integration/messaging/wmq/provider/BasicWmqMqmdFieldsOperatorTest.xml");

    /**
     * メッセージ送信前のMQMDフィールドが正しく設定されること。
     */
    @Test
    public void testSetFieldsBeforeSend() throws MQException {

        long defaultTimeToLive = 15000;
        BasicWmqMqmdFieldsOperator mqmdOperator = new BasicWmqMqmdFieldsOperator();

        // 応答不要メッセージ送信処理の場合

        SendingMessage sendingMessage = new SendingMessage();

        MQMessage mqMessage = new MQMessage();

        mqmdOperator.setFieldsBeforeSend(sendingMessage, mqMessage, defaultTimeToLive);

        assertThat(mqMessage.getVersion(), is(CMQC.MQMD_VERSION_2));
        assertThat(mqMessage.encoding, is(CMQC.MQENC_INTEGER_NORMAL | CMQC.MQENC_DECIMAL_NORMAL | CMQC.MQENC_FLOAT_IEEE_NORMAL));
        assertThat(mqMessage.format, is(CMQC.MQFMT_NONE));
        assertThat(mqMessage.messageSequenceNumber, is(1));
        assertThat(mqMessage.messageId, is(CMQC.MQMI_NONE));
        assertThat(mqMessage.messageType, is(CMQC.MQMT_DATAGRAM));
        assertThat(mqMessage.putDateTime.getTimeInMillis(), is(FixedSystemTimeProvider.FIXED_DATE.getTime()));
        assertThat(mqMessage.characterSet, is(CMQC.MQCCSI_Q_MGR));
        assertThat(mqMessage.persistence, is(CMQC.MQPER_PERSISTENT));
        assertThat(mqMessage.correlationId, is(CMQC.MQCI_NONE));
        assertThat(mqMessage.replyToQueueName, is(""));
        assertThat(mqMessage.expiry, is(150));

        // 同期応答メッセージ受信処理の場合

        RequestMessage requestMessage = new RequestMessage(new FwHeader(), new ReceivedMessage("test".getBytes()));
        requestMessage.setMessageId("414D51205445535420202020202020205A24D84E20000F02");
        requestMessage.setReplyTo("TEST.RESPONSE");
        sendingMessage = new ResponseMessage(requestMessage);

        mqMessage = new MQMessage();

        mqmdOperator.setFieldsBeforeSend(sendingMessage, mqMessage, defaultTimeToLive);

        assertThat(mqMessage.getVersion(), is(CMQC.MQMD_VERSION_2));
        assertThat(mqMessage.encoding, is(CMQC.MQENC_INTEGER_NORMAL | CMQC.MQENC_DECIMAL_NORMAL | CMQC.MQENC_FLOAT_IEEE_NORMAL));
        assertThat(mqMessage.format, is(CMQC.MQFMT_NONE));
        assertThat(mqMessage.messageSequenceNumber, is(1));
        assertThat(mqMessage.messageId, is(CMQC.MQMI_NONE));
        assertThat(mqMessage.messageType, is(CMQC.MQMT_REPLY));
        assertThat(mqMessage.putDateTime.getTimeInMillis(), is(FixedSystemTimeProvider.FIXED_DATE.getTime()));
        assertThat(mqMessage.characterSet, is(CMQC.MQCCSI_Q_MGR));
        assertThat(mqMessage.persistence, is(CMQC.MQPER_PERSISTENT));
        assertThat(mqMessage.correlationId, is(BinaryUtil.convertHexToBytes("414D51205445535420202020202020205A24D84E20000F02")));
        assertThat(mqMessage.replyToQueueName, is(""));
        assertThat(mqMessage.expiry, is(150));

        // 同期応答メッセージ送信処理の場合

        sendingMessage = new SendingMessage();
        sendingMessage.setReplyTo("TEST.RESPONSE");

        mqMessage = new MQMessage();

        mqmdOperator.setFieldsBeforeSend(sendingMessage, mqMessage, defaultTimeToLive);

        assertThat(mqMessage.getVersion(), is(CMQC.MQMD_VERSION_2));
        assertThat(mqMessage.encoding, is(CMQC.MQENC_INTEGER_NORMAL | CMQC.MQENC_DECIMAL_NORMAL | CMQC.MQENC_FLOAT_IEEE_NORMAL));
        assertThat(mqMessage.format, is(CMQC.MQFMT_NONE));
        assertThat(mqMessage.messageSequenceNumber, is(1));
        assertThat(mqMessage.messageId, is(CMQC.MQMI_NONE));
        assertThat(mqMessage.messageType, is(CMQC.MQMT_REQUEST));
        assertThat(mqMessage.putDateTime.getTimeInMillis(), is(FixedSystemTimeProvider.FIXED_DATE.getTime()));
        assertThat(mqMessage.characterSet, is(CMQC.MQCCSI_Q_MGR));
        assertThat(mqMessage.persistence, is(CMQC.MQPER_PERSISTENT));
        assertThat(mqMessage.correlationId, is(CMQC.MQCI_NONE));
        assertThat(mqMessage.replyToQueueName, is("TEST.RESPONSE"));
        assertThat(mqMessage.expiry, is(150));

        // SendingMessageのTIME_TO_LIVEヘッダが指定された場合(0以上)
        sendingMessage = new SendingMessage();
        sendingMessage.setTimeToLive(2000);
        mqMessage = new MQMessage();
        mqmdOperator.setFieldsBeforeSend(sendingMessage, mqMessage, defaultTimeToLive);
        assertThat(mqMessage.expiry, is(20));

        // SendingMessageのTIME_TO_LIVEヘッダが指定された場合(0)
        sendingMessage = new SendingMessage();
        sendingMessage.setTimeToLive(0);
        mqMessage = new MQMessage();
        mqmdOperator.setFieldsBeforeSend(sendingMessage, mqMessage, defaultTimeToLive);
        assertThat(mqMessage.expiry, is(CMQC.MQEI_UNLIMITED));

        // characterSetIdプロパティが指定された場合
        mqmdOperator.setCharacterSetId(1208);
        sendingMessage = new SendingMessage();
        mqMessage = new MQMessage();
        mqmdOperator.setFieldsBeforeSend(sendingMessage, mqMessage, defaultTimeToLive);
        assertThat(mqMessage.characterSet, is(1208));

        // messageIdCharsetプロパティが指定された場合
        sendingMessage = new SendingMessage();
        sendingMessage.setCorrelationId("414D51205445535420202020202020205A24D84E20000F02");
        mqMessage = new MQMessage();
        mqmdOperator.setFieldsBeforeSend(sendingMessage, mqMessage, defaultTimeToLive);
        assertThat(mqMessage.correlationId, is(BinaryUtil.convertHexToBytes("414D51205445535420202020202020205A24D84E20000F02")));

        // persistenceプロパティが指定された場合
        mqmdOperator.setPersistence(false);
        sendingMessage = new SendingMessage();
        mqMessage = new MQMessage();
        mqmdOperator.setFieldsBeforeSend(sendingMessage, mqMessage, defaultTimeToLive);
        assertThat(mqMessage.persistence, is(CMQC.MQPER_NOT_PERSISTENT));
    }

    /**
     * メッセージ送信後のMQMDフィールドが正しく取得されること。
     */
    @Test
    public void testGetFieldsAfterSend() throws MQException {

        BasicWmqMqmdFieldsOperator mqmdOperator = new BasicWmqMqmdFieldsOperator();

        SendingMessage sendingMessage = new SendingMessage();

        MQMessage mqMessage = new MQMessage();
        mqMessage.messageId = BinaryUtil.convertHexToBytes("414D51205445535420202020202020205A24D84E20000F02");

        mqmdOperator.getFieldsAfterSend(mqMessage, sendingMessage);

        assertThat(sendingMessage.getMessageId(), is("414D51205445535420202020202020205A24D84E20000F02"));
    }

    /**
     * メッセージ受信前のMQMDフィールドが正しく設定されること。
     */
    @Test
    public void testSetFieldsBeforeReceive() throws MQException {

        BasicWmqMqmdFieldsOperator mqmdOperator = new BasicWmqMqmdFieldsOperator();

        // messageId引数が指定された場合

        MQMessage mqMessage = new MQMessage();

        mqmdOperator.setFieldsBeforeReceive("414D51205445535420202020202020205A24D84E20000F02", mqMessage);
        assertThat(mqMessage.messageId, is(CMQC.MQMI_NONE));
        assertThat(mqMessage.correlationId, is(BinaryUtil.convertHexToBytes("414D51205445535420202020202020205A24D84E20000F02")));

        // messageId引数が指定されなかった場合

        mqMessage = new MQMessage();

        mqmdOperator.setFieldsBeforeReceive(null, mqMessage);
        assertThat(mqMessage.messageId, is(CMQC.MQMI_NONE));
        assertThat(mqMessage.correlationId, is(CMQC.MQCI_NONE));

        mqMessage = new MQMessage();

        mqmdOperator.setFieldsBeforeReceive("", mqMessage);
        assertThat(mqMessage.messageId, is(CMQC.MQMI_NONE));
        assertThat(mqMessage.correlationId, is(CMQC.MQCI_NONE));

        mqMessage = new MQMessage();

        mqmdOperator.setFieldsBeforeReceive(BinaryUtil.convertToHexString(CMQC.MQMI_NONE), mqMessage);
        assertThat(mqMessage.messageId, is(CMQC.MQMI_NONE));
        assertThat(mqMessage.correlationId, is(CMQC.MQCI_NONE));
    }

    /**
     * メッセージ受信後のMQMDフィールドが正しく取得されること。
     */
    @Test
    public void testGetFieldsAfterReceive() throws MQException {

        BasicWmqMqmdFieldsOperator mqmdOperator = new BasicWmqMqmdFieldsOperator();

        // メッセージID、相関ID、応答先キューの名前が設定されている場合

        MQMessage mqMessage = new MQMessage();
        mqMessage.messageId = BinaryUtil.convertHexToBytes("414D51205445535420202020202020205A24D84E20000F02");
        mqMessage.correlationId = BinaryUtil.convertHexToBytes("414D51205445535420202020202020205A24D84E2FFFFF04");
        mqMessage.replyToQueueName = "TEST.RESPONSE";

        ReceivedMessage receivedMessage = new ReceivedMessage("dummy".getBytes());

        mqmdOperator.getFieldsAfterReceive(mqMessage, receivedMessage);

        assertThat(receivedMessage.getMessageId(), is("414D51205445535420202020202020205A24D84E20000F02"));
        assertThat(receivedMessage.getCorrelationId(), is("414D51205445535420202020202020205A24D84E2FFFFF04"));
        assertThat(receivedMessage.getReplyTo(), is("TEST.RESPONSE"));

        // メッセージIDのみ設定されている場合

        mqMessage = new MQMessage();
        mqMessage.messageId = BinaryUtil.convertHexToBytes("414D51205445535420202020202020205A24D84E20000F02");

        receivedMessage = new ReceivedMessage("dummy".getBytes());

        mqmdOperator.getFieldsAfterReceive(mqMessage, receivedMessage);

        assertThat(receivedMessage.getMessageId(), is("414D51205445535420202020202020205A24D84E20000F02"));
        assertNull(receivedMessage.getCorrelationId());
        assertNull(receivedMessage.getReplyTo());

        // 応答先キューの名前が空白の場合

        mqMessage = new MQMessage();
        mqMessage.messageId = BinaryUtil.convertHexToBytes("414D51205445535420202020202020205A24D84E20000F02");
        StringUtil.getBytes(" ", Charset.forName("MS932"));
        mqMessage.replyToQueueName = " ";

        receivedMessage = new ReceivedMessage("dummy".getBytes());

        mqmdOperator.getFieldsAfterReceive(mqMessage, receivedMessage);

        assertThat(receivedMessage.getMessageId(), is("414D51205445535420202020202020205A24D84E20000F02"));
        assertNull(receivedMessage.getCorrelationId());
        assertNull(receivedMessage.getReplyTo());
    }
}
