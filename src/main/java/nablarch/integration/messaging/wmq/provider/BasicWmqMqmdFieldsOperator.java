package nablarch.integration.messaging.wmq.provider;

import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.Map;

import nablarch.core.date.SystemTimeUtil;
import nablarch.core.util.BinaryUtil;
import nablarch.core.util.StringUtil;
import nablarch.fw.messaging.InterSystemMessage.HeaderName;
import nablarch.fw.messaging.ReceivedMessage;
import nablarch.fw.messaging.ResponseMessage;
import nablarch.fw.messaging.SendingMessage;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.constants.CMQC;

/**
 * {@link WmqMqmdFieldsOperator}の基本実装クラス。
 * @author Kiyohito Itoh
 */
public class BasicWmqMqmdFieldsOperator implements WmqMqmdFieldsOperator {

    /** メッセージデータの文字セットID(MQMDフィールド)の値 */
    private Integer characterSetId = null;

    /** メッセージの持続性(MQMDフィールド)の値 */
    private boolean persistence = true;

    /**
     * {@inheritDoc}
     * <pre>
     * 設定内容は下記のとおり。
     * 
     * 構造体のバージョン番号
     *     {@link CMQC#MQMD_VERSION_2}
     * メッセージデータの数値エンコード
     *     {@link CMQC#MQENC_INTEGER_NORMAL}
     *     {@link CMQC#MQENC_DECIMAL_NORMAL}
     *     {@link CMQC#MQENC_FLOAT_IEEE_NORMAL}
     * メッセージデータの形式名
     *     {@link CMQC#MQFMT_NONE}
     * グループ内での論理メッセージの順序番号
     *     1
     * メッセージID
     *     {@link CMQC#MQMI_NONE}
     * メッセージタイプ
     *     {@link #getMessageType(SendingMessage)}の戻り値
     * メッセージが書き込まれた日付
     *     {@link SystemTimeUtil#getDate()}の戻り値
     * メッセージデータの文字セットID
     *     {@link #characterSetId}プロパティの値
     *     設定しない場合は{@link CMQC#MQCCSI_Q_MGR}が使用される。
     * メッセージの持続性
     *     {@link #persistence}がtrueの場合は{@link CMQC#MQPER_PERSISTENT}
     *     {@link #persistence}がfalseの場合は{@link CMQC#MQPER_NOT_PERSISTENT}
     * 相関ID
     *     {@link SendingMessage}のヘッダ({@link HeaderName#CORRELATION_ID})に指定された値
     *     指定がない場合は設定しない。
     * 応答先キューの名前
     *     {@link SendingMessage}のヘッダ({@link HeaderName#REPLY_TO})に指定された値
     *     指定がない場合は設定しない。
     * メッセージ存続時間
     *     {@link SendingMessage}のヘッダ({@link HeaderName#TIME_TO_LIVE})に指定された値
     *     指定がない場合はdefaultTimeToLive引数に指定された値
     *     値が0以下の場合は{@link CMQC#MQEI_UNLIMITED}
     * </pre>
     */
    public void setFieldsBeforeSend(SendingMessage sendingMessage, MQMessage mqMessage, long defaultTimeToLive)
            throws MQException {

        /**********************************************
           基本実装の設定値(固定値)
         **********************************************/

        // 構造体のバージョン番号
        mqMessage.setVersion(CMQC.MQMD_VERSION_2);

        // メッセージデータの数値エンコード
        mqMessage.encoding = CMQC.MQENC_INTEGER_NORMAL | CMQC.MQENC_DECIMAL_NORMAL | CMQC.MQENC_FLOAT_IEEE_NORMAL;

        // メッセージデータの形式名
        mqMessage.format = CMQC.MQFMT_NONE;

        // グループ内での論理メッセージの順序番号
        mqMessage.messageSequenceNumber = 1;

        // メッセージID
        mqMessage.messageId = CMQC.MQMI_NONE;

        // メッセージタイプ
        mqMessage.messageType = getMessageType(sendingMessage);

        // メッセージが書き込まれた日付
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(SystemTimeUtil.getDate());
        mqMessage.putDateTime = calendar;

        /**********************************************
           本クラスのプロパティ
         **********************************************/

        // メッセージデータの文字セットID
        if (characterSetId != null) {
            mqMessage.characterSet = characterSetId;
        }

        // メッセージの持続性
        mqMessage.persistence = persistence ? CMQC.MQPER_PERSISTENT : CMQC.MQPER_NOT_PERSISTENT;

        /**********************************************
           SendingMessageヘッダの設定値
         **********************************************/

        Map<String, Object> headerMap = sendingMessage.getHeaderMap();

        // 相関ID
        String correlationId = getHeaderValue(headerMap, HeaderName.CORRELATION_ID, null);
        if (StringUtil.hasValue(correlationId)) {
            mqMessage.correlationId = convertIdToByte(correlationId);
        }

        // 応答先キューの名前
        String replyTo = getHeaderValue(headerMap, HeaderName.REPLY_TO, null);
        if (StringUtil.hasValue(replyTo)) {
            mqMessage.replyToQueueName = replyTo;
        }

        // メッセージ存続時間
        // timeToLive: ミリ秒単位(Nablarch)
        // expiry    : 1/10秒単位(WebSphere MQ)
        Long timeToLive = getHeaderValue(headerMap, HeaderName.TIME_TO_LIVE, defaultTimeToLive);
        sendingMessage.setTimeToLive(timeToLive);
        int expiry = (int) (timeToLive / 100); // ミリ秒単位 -> 1/10秒単位
        mqMessage.expiry = expiry <= 0 ? CMQC.MQEI_UNLIMITED : expiry;
    }

    /**
     * 送信メッセージから判定したメッセージタイプを取得する。
     * <pre>
     * 判定内容は下記のとおり。
     * 
     * 送信メッセージが{@link ResponseMessage}である場合
     *     {@link CMQC#MQMT_REPLY}を返す。
     * 送信メッセージが{@link ResponseMessage}でなく、応答宛先キューが設定されている場合
     *     {@link CMQC#MQMT_REQUEST}を返す。
     * 送信メッセージが{@link ResponseMessage}でなく、応答宛先キューが設定されていない場合
     *     {@link CMQC#MQMT_DATAGRAM}を返す。
     * </pre>
     * @param sendingMessage 送信メッセージ
     * @return メッセージタイプ
     */
    protected int getMessageType(SendingMessage sendingMessage) {

        if (sendingMessage instanceof ResponseMessage) {
            // 同期応答メッセージ受信の場合
            return CMQC.MQMT_REPLY;
        }

        if (StringUtil.hasValue(sendingMessage.getReplyTo())) {
            // 同期応答メッセージ送信の場合
            return CMQC.MQMT_REQUEST;
        } else {
            // 応答不要メッセージ送信の場合
            return CMQC.MQMT_DATAGRAM;
        }
    }

    /**
     * ヘッダマップから指定されたヘッダ名を使用して値を取得する。
     * <p/>
     * 指定されたヘッダ名がヘッダマップに存在しない場合は指定されたデフォルト値を返す。
     * 
     * @param <T> 値の型
     * @param headerMap ヘッダマップ(キーはヘッダ名)
     * @param headerName ヘッダ名
     * @param defaultValue デフォルト値
     * @return ヘッダ名に対応する値
     */
    @SuppressWarnings("unchecked")
    protected <T> T getHeaderValue(Map<String, Object> headerMap, String headerName, T defaultValue) {
        return headerMap.containsKey(headerName) ? (T) headerMap.get(headerName) : defaultValue;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * {@link MQMessage}に設定されたメッセージIDを
     * {@link SendingMessage}のヘッダ({@link HeaderName#MESSAGE_ID})に設定する。
     */
    public void getFieldsAfterSend(MQMessage mqMessage, SendingMessage sendingMessage) throws MQException {
        String messageId = convertIdToString(mqMessage.messageId);
        sendingMessage.setMessageId(messageId);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * メッセージIDが指定された場合は、{@link MQMessage}の相関IDに指定されたメッセージIDを設定する。
     */
    public void setFieldsBeforeReceive(String messageId, MQMessage mqMessage) throws MQException {
        if (StringUtil.hasValue(messageId)) {
            byte[] correlationId = convertIdToByte(messageId);
            if (!Arrays.equals(correlationId, CMQC.MQMI_NONE)) {
                mqMessage.correlationId = correlationId;
            }
        }
    }

    /**
     * {@inheritDoc}
     * <pre>
     * 設定内容は下記のとおり。
     * 
     * メッセージID
     *     {@link MQMessage}に設定されたメッセージIDを
     *     {@link ReceivedMessage}のヘッダ({@link HeaderName#MESSAGE_ID})に設定する。
     * 相関ID
     *     {@link MQMessage}に設定された相関IDを
     *     {@link ReceivedMessage}のヘッダ({@link HeaderName#CORRELATION_ID})に設定する。
     *     指定がない場合は設定しない。
     * 応答先キューの名前
     *     {@link SendingMessage}のヘッダ({@link HeaderName#REPLY_TO})に指定された値
     *     指定がない場合は設定しない。
     * </pre>
     * @param mqMessage {@link MQMessage}
     * @param receivedMessage 受信メッセージ
     */
    public void getFieldsAfterReceive(MQMessage mqMessage, ReceivedMessage receivedMessage) throws MQException {

        // メッセージID
        receivedMessage.setMessageId(convertIdToString(mqMessage.messageId));

        // 相関ID
        if (!Arrays.equals(mqMessage.correlationId, CMQC.MQCI_NONE)) {
            String correlationId = convertIdToString(mqMessage.correlationId);
            receivedMessage.setCorrelationId(correlationId);
        }

        // 応答先キューの名前
        String replyTo = mqMessage.replyToQueueName.trim();
        if (StringUtil.hasValue(replyTo)) {
            receivedMessage.setReplyTo(replyTo);
        }
    }

    /**
     * バイト配列で表現されたメッセージID(または相関ID)を文字列に変換する。
     * <p/>
     * メッセージID(または相関ID)の型が{@link MQMessage}(バイト配列)と
     * {@link nablarch.fw.messaging.InterSystemMessage}(文字列)で異なるため、本メソッドにより変換を行う。
     * <p/>
     * 基本実装では、メッセージID(または相関ID)の文字列表現に16進数表記を使用する。
     * 
     * @param id バイト配列で表現されたメッセージID(または相関ID)
     * @return 変換後の文字列
     */
    protected String convertIdToString(byte[] id) {
        return BinaryUtil.convertToHexString(id);
    }

    /**
     * 文字列で表現されたメッセージID(または相関ID)をバイト配列に変換する。
     * <p/>
     * メッセージID(または相関ID)の型が{@link MQMessage}(バイト配列)と
     * {@link nablarch.fw.messaging.InterSystemMessage}(文字列)で異なるため、本メソッドにより変換を行う。
     * <p/>
     * 基本実装では、メッセージID(または相関ID)の文字列表現に16進数表記を使用する。
     * 
     * @param id 文字列で表現されたメッセージID(または相関ID)
     * @return 変換後のバイト配列
     */
    protected byte[] convertIdToByte(String id) {
        return BinaryUtil.convertHexToBytes(id);
    }

    /**
     * メッセージデータの文字セットID(MQMDフィールド)の値を設定する。
     * <pre>
     * 下記のいずれかの値を設定する。
     * 
     * 850
     *     commonly used ASCII codeset
     * 819
     *     the ISO standard ASCII codeset
     * 37
     *     the American EBCDIC codeset
     * 1200
     *     Unicode
     * 1208
     *     UTF-8
     * 
     * 設定しない場合は{@link CMQC#MQCCSI_Q_MGR}が使用される。
     * </pre>
     * @param characterSetId メッセージデータの文字セットID(MQMDフィールド)
     * @return このオブジェクト自体
     */
    public BasicWmqMqmdFieldsOperator setCharacterSetId(Integer characterSetId) {
        this.characterSetId = characterSetId;
        return this;
    }

    /**
     * メッセージの持続性(MQMDフィールド)の値を設定する。
     * <pre>
     * trueが指定された場合はMQMDフィールドに{@link CMQC#MQPER_PERSISTENT}を使用する。
     * falseが指定された場合はMQMDフィールドに{@link CMQC#MQPER_NOT_PERSISTENT}を使用する。
     * 
     * デフォルトはtrue。
     * </pre>
     * @param persistence MQMDフィールドに{@link CMQC#MQPER_PERSISTENT}を使用する場合はtrue、
     *                     {@link CMQC#MQPER_NOT_PERSISTENT}を使用する場合はfalse
     * @return このオブジェクト自体
     */
    public BasicWmqMqmdFieldsOperator setPersistence(boolean persistence) {
        this.persistence = persistence;
        return this;
    }
}
