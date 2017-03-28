package nablarch.integration.messaging.wmq.provider;

import nablarch.fw.messaging.ReceivedMessage;
import nablarch.fw.messaging.SendingMessage;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;

/**
 * メッセージの送信時と受信時に{@link MQMessage}のMQMDフィールドを操作するクラス。
 * <pre>
 * 本クラスは下記の処理を行う。
 * 
 *     メッセージ送信前に{@link MQMessage}のMQMDフィールドに値を設定する処理
 *     メッセージ送信後に{@link MQMessage}のMQMDフィールドから値を取得する処理
 *     メッセージ受信前に{@link MQMessage}のMQMDフィールドに値を設定する処理
 *     メッセージ受信後に{@link MQMessage}のMQMDフィールドから値を取得する処理
 * 
 * </pre>
 * @author Kiyohito Itoh
 */
public interface WmqMqmdFieldsOperator {

    /**
     * メッセージ送信前に{@link MQMessage}のMQMDフィールドに値を設定する。
     * @param sendingMessage 送信メッセージ
     * @param mqMessage {@link MQMessage}
     * @param defaultTimeToLive 送信電文の有効期間(単位:msec)
     * @throws MQException MQMDフィールドに対して不正な操作が行われた場合
     */
    void setFieldsBeforeSend(SendingMessage sendingMessage, MQMessage mqMessage, long defaultTimeToLive) throws MQException;

    /**
     * メッセージ送信後に{@link MQMessage}のMQMDフィールドから値を取得する。
     * @param sendingMessage 送信メッセージ
     * @param mqMessage {@link MQMessage}
     * @throws MQException MQMDフィールドに対して不正な操作が行われた場合
     */
    void getFieldsAfterSend(MQMessage mqMessage, SendingMessage sendingMessage) throws MQException;

    /**
     * メッセージ受信前に{@link MQMessage}のMQMDフィールドに値を設定する。
     * @param messageId 相関IDに指定するメッセージID
     * @param mqMessage {@link MQMessage}
     * @throws MQException MQMDフィールドに対して不正な操作が行われた場合
     */
    void setFieldsBeforeReceive(String messageId, MQMessage mqMessage) throws MQException;

    /**
     * メッセージ受信後に{@link MQMessage}のMQMDフィールドから値を取得する。
     * @param mqMessage {@link MQMessage}
     * @param receivedMessage 受信メッセージ
     * @throws MQException MQMDフィールドに対して不正な操作が行われた場合
     */
    void getFieldsAfterReceive(MQMessage mqMessage, ReceivedMessage receivedMessage) throws MQException;
}
