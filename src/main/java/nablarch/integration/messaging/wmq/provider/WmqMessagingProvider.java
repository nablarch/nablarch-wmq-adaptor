package nablarch.integration.messaging.wmq.provider;

import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.log.app.FailureLogUtil;
import nablarch.core.repository.IgnoreProperty;
import nablarch.core.repository.initialization.Initializable;
import nablarch.core.util.BinaryUtil;
import nablarch.core.util.StringUtil;
import nablarch.fw.handler.retry.Retryable;
import nablarch.fw.launcher.ProcessAbnormalEnd;
import nablarch.fw.messaging.MessagingContext;
import nablarch.fw.messaging.MessagingException;
import nablarch.fw.messaging.MessagingProvider;
import nablarch.fw.messaging.ReceivedMessage;
import nablarch.fw.messaging.SendingMessage;
import nablarch.fw.messaging.provider.MessagingExceptionFactory;
import nablarch.integration.messaging.wmq.provider.exception.BasicWmqMessagingExceptionFactory;

import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * IBM MQを使用した{@link MessagingProvider}の実装クラス。
 * @author Kiyohito Itoh
 */
public class WmqMessagingProvider implements MessagingProvider, Initializable {

    /** メッセージングログを出力するロガー */
    private static final Logger LOGGER = LoggerManager.get("MESSAGING");

    /** キューマネージャ名称 */
    private String queueManagerName;

    /** 送信キュー名(論理名)リスト */
    private List<String> sendingQueueNames = new ArrayList<String>();

    /** 受信キュー名(論理名) */
    private String receivedQueueName;

    /** 退避キュー名パターン(論理名) */
    private String poisonQueueNamePattern;

    /** 退避キュー名(論理名) */
    private String poisonQueueName;

    /** バックアウト回数の上限値  */
    private int backoutLimit = 0; 

    /** バックアウト上限を超えた場合に使用する終了コード(プロセスを終了({@link System#exit(int)})する際に設定する値) */
    private int backoutLimitExceededExitCode = 190;
    
    /** バックアウト上限を超えた場合に使用する障害コード */
    private String backoutLimitExceededFailureCode;

    /** ポイズンメッセージの送信に失敗した場合に使用する終了コード(プロセスを終了({@link System#exit(int)})する際に設定する値) */
    private int putPoisonFailedExitCode = 191;
    
    /** ポイズンメッセージの送信に失敗した場合に使用する障害コード */
    private String putPoisonFailedFailureCode;

    /** デフォルトタイムアウト値(単位:msec) */
    private long defaultResponseTimeout = 300 * 1000;

    /** 送信電文の有効期間(単位:msec) */
    private long defaultTimeToLive = 60 * 1000;

    /** MQMDフィールドのオペレータ */
    private WmqMqmdFieldsOperator mqmdFieldsOperator = new BasicWmqMqmdFieldsOperator();

    /** {@link MessagingException}ファクトリオブジェクト */
    private MessagingExceptionFactory messagingExceptionFactory = new BasicWmqMessagingExceptionFactory();

    /**
     * 分散トランザクションを使用するか否か。
     * 分散トランザクションを使用する場合はtrue。
     */
    private boolean useXa = true;

    /**
     * IBM MQによる{@link MQException}発生時の標準エラー出力を使用するか否か。
     * IBM MQによる{@link MQException}発生時の標準エラー出力を使用する場合はtrue。
     */
    private boolean useProductSystemErrorOutput = false;

    /**
     * IBM MQの初期化処理を行う。
     * <p/>
     * 下記の処理を行う。
     * <ul>
     * <li>{@link #checkPoisonSetting()}メソッドを呼び出し退避キューの設定不備がないことをチェックする。</li>
     * <li>接続モード({@link CMQC#TRANSPORT_PROPERTY})をバインディングモードに設定する。</li>
     * <li>スレッド類縁性({@link CMQC#THREAD_AFFINITY_PROPERTY})に{@link #useXa}プロパティの値を設定する。</li>
     * <li>{@link #useProductSystemErrorOutput}プロパティがfalseの場合はIBM MQによる標準エラー出力を無効化する。</li>
     * <li>接続モード({@link CMQC#TRANSPORT_PROPERTY})をバインディングモードに設定する。</li>
     * <li>{@link #poisonQueueNamePattern}が指定された場合は{@link #receivedQueueName}を使用してフォーマットした退避キュー名を設定する。</li>
     * </ul>
     */
    @SuppressWarnings("unchecked")
    public void initialize() {

        // 退避キューの設定不備チェック
        checkPoisonSetting();

        // バインディングモード接続
        MQEnvironment.properties.put(CMQC.TRANSPORT_PROPERTY, CMQC.TRANSPORT_MQSERIES_BINDINGS);

        // スレッド類縁性
        MQEnvironment.properties.put(CMQC.THREAD_AFFINITY_PROPERTY, useXa);

        // IBM MQによるMQException発生時の標準エラー出力
        if (!useProductSystemErrorOutput) {
            MQException.log = null;
        }

        // 退避キュー名
        if (StringUtil.hasValue(receivedQueueName) && StringUtil.hasValue(poisonQueueNamePattern)) {
            poisonQueueName = String.format(poisonQueueNamePattern, receivedQueueName);
        }
    }

    /**
     * 退避キューの設定不備がないことをチェックする。
     * <p/>
     * チェック内容は下記のとおり。
     * <ul>
     * <li>
     * {@link #receivedQueueName}が指定されない場合は、
     * {@link #poisonQueueNamePattern}と{@link #poisonQueueName}も指定されないこと。
     * </li>
     * <li>
     * {@link #receivedQueueName}が指定された場合は、
     * {@link #poisonQueueNamePattern}と{@link #poisonQueueName}の両方が指定されないか、
     * いずれか一方のみ指定されること。
     * </li>
     * </ul>
     * 設定不備が見つかった場合は実行例例外を送出する。
     */
    protected void checkPoisonSetting() {

        if (StringUtil.isNullOrEmpty(receivedQueueName)
                && (StringUtil.hasValue(poisonQueueNamePattern, poisonQueueName))) {
            throw new IllegalArgumentException(
                String.format("poison queue setting was invalid. "
                            + "if receivedQueueName was not set, "
                            + "must not be set poisonQueueNamePattern and poisonQueueName. "
                            + "queueManagerName = [%s], poisonQueueNamePattern = [%s], poisonQueueName = [%s]",
                            queueManagerName, poisonQueueNamePattern, poisonQueueName));
        }

        if (StringUtil.hasValue(receivedQueueName)
                && (StringUtil.hasValue(poisonQueueNamePattern) && StringUtil.hasValue(poisonQueueName))) {
            throw new IllegalArgumentException(
                String.format("poison queue setting was invalid. "
                            + "if receivedQueueName was set, "
                            + "must be set either poisonQueueNamePattern or poisonQueueName "
                            + "or must not be set both poisonQueueNamePattern and poisonQueueName. "
                            + "queueManagerName = [%s], poisonQueueNamePattern = [%s], poisonQueueName = [%s]",
                            queueManagerName, poisonQueueNamePattern, poisonQueueName));
        }
    }

    /**
     * {@link WmqMessagingContext}を生成する。
     * <pre>
     * 指定された設定情報から{@link MQQueueManager}を生成し、
     * {@link MQQueue}マップ(送信先と受信先とポイズンメッセージ送信先)の初期化を行う。
     * 
     * {@link MQQueueManager}の生成は{@link #createMQQueueManager()}メソッドに委譲する。
     * 
     * 送信先{@link MQQueue}マップの初期化は{@link #getMQQueues(MQQueueManager, Collection, int)}メソッド、
     * 受信先{@link MQQueue}とポイズンメッセージ送信先{@link MQQueue}の初期化は{@link #getPoisonQueueOpenOptions()}メソッドに委譲する。
     * 
     * 送信先{@link MQQueue}のオープンを制御するオプションは{@link #getSendingQueueOpenOptions()}メソッド、
     * 受信先{@link MQQueue}のオープンを制御するオプションは{@link #getReceivedQueueOpenOptions()}メソッド、
     * ポイズンメッセージ送信先{@link MQQueue}のオープンを制御するオプションは{@link #getPoisonQueueOpenOptions()}メソッド、
     * から取得する。
     * 
     * MQExceptionを捕捉した場合は、
     * {@link MessagingExceptionFactory#createMessagingException(String, Throwable)} メソッド
     * に例外処理を委譲する。
     * </pre>
     * 
     * @return {@link WmqMessagingContext}
     */
    public MessagingContext createContext() {
        try {
            MQQueueManager mqQueueManager = createMQQueueManager();
            return new WmqMessagingContext(
                this, mqQueueManager,
                getMQQueues(mqQueueManager, sendingQueueNames, getSendingQueueOpenOptions()),
                getMQQueue(mqQueueManager, receivedQueueName, getReceivedQueueOpenOptions()),
                getMQQueue(mqQueueManager, poisonQueueName, getPoisonQueueOpenOptions()));
        } catch (MQException e) {
            throw messagingExceptionFactory.createMessagingException(
                String.format("could not initialize IBM MQ MQQueueManager/MQQueue. "
                            + "queueManagerName = [%s]", queueManagerName), e);
        }
    }

    /**
     * キューマネージャ名称を指定して{@link MQQueueManager}を生成する。
     * @return {@link MQQueueManager}
     * @throws MQException {@link MQQueueManager}の生成に失敗した場合
     */
    protected MQQueueManager createMQQueueManager() throws MQException {
        return new MQQueueManager(queueManagerName);
    }

    /**
     * 送信先{@link MQQueue}のオープンを制御するオプションを取得する。
     * <p/>
     * 下記オプションを指定した値を返す。
     * <ul>
     * <li>{@link CMQC#MQOO_OUTPUT}</li>
     * <li>{@link CMQC#MQOO_FAIL_IF_QUIESCING}</li>
     * </ul>
     * @return 送信先{@link MQQueue}のオープンを制御するオプション
     */
    protected int getSendingQueueOpenOptions() {
        return CMQC.MQOO_OUTPUT | CMQC.MQOO_FAIL_IF_QUIESCING;
    }

    /**
     * 受信先{@link MQQueue}のオープンを制御するオプションを取得する。
     * <p/>
     * 下記オプションを指定した値を返す。
     * <ul>
     * <li>{@link CMQC#MQOO_INPUT_SHARED}</li>
     * <li>{@link CMQC#MQOO_FAIL_IF_QUIESCING}</li>
     * </ul>
     * @return 受信先{@link MQQueue}のオープンを制御するオプション
     */
    protected int getReceivedQueueOpenOptions() {
        return CMQC.MQOO_INPUT_SHARED | CMQC.MQOO_FAIL_IF_QUIESCING;
    }

    /**
     * ポイズンメッセージの退避先{@link MQQueue}のオープンを制御するオプションを取得する。
     * <p/>
     * {@link #getSendingQueueOpenOptions()}メソッドに処理を委譲する。
     * 
     * @return ポイズンメッセージの退避先{@link MQQueue}のオープンを制御するオプション
     */
    protected int getPoisonQueueOpenOptions() {
        return getSendingQueueOpenOptions();
    }

    /**
     * 指定されたキュー名とオプションを使用して、正常にオープンされた{@link MQQueue}を取得する。
     * @param mqQueueManager {@link MQQueueManager}
     * @param queueNames キュー名リスト
     * @param openOptions {@link MQQueue}のオープンを制御するオプション
     * @return 正常にオープンされた{@link MQQueue}マップ(キーはキュー名)
     * @throws MQException {@link MQQueue}のオープンに失敗した場合
     */
    protected Map<String, MQQueue> getMQQueues(
            MQQueueManager mqQueueManager, Collection<String> queueNames, int openOptions)
            throws MQException {
        if (queueNames.isEmpty()) {
            return Collections.emptyMap();
        }
        HashMap<String, MQQueue> mqQueues = new HashMap<String, MQQueue>();
        for (String queueName : queueNames) {
            mqQueues.put(queueName, getMQQueue(mqQueueManager, queueName, openOptions));
        }
        return mqQueues;
    }

    /**
     * 指定されたキュー名とオプションを使用して、正常にオープンされた{@link MQQueue}を取得する。
     * @param mqQueueManager {@link MQQueueManager}
     * @param queueName キュー名
     * @param openOptions {@link MQQueue}のオープンを制御するオプション
     * @return 正常にオープンされた{@link MQQueue}。キュー名の指定がない場合はnull
     * @throws MQException {@link MQQueue}のオープンに失敗した場合
     */
    protected MQQueue getMQQueue(MQQueueManager mqQueueManager, String queueName, int openOptions)
            throws MQException {
        if (StringUtil.isNullOrEmpty(queueName)) {
            return null;
        }
        return mqQueueManager.accessQueue(queueName, openOptions);
    }

    /**
     * 送信メッセージを指定された{@link MQQueue}に書き込む。
     * </p>
     * 送信メッセージから{@link MQMessage}を作成し{@link MQQueue}に書き込む。
     * <br/>
     * メッセージ送信前のMQMDフィールドの設定は、
     * {@link WmqMqmdFieldsOperator#setFieldsBeforeSend(SendingMessage, MQMessage, long)}メソッドに委譲する。
     * <br/>
     * {@link MQQueue}への書き込みを制御するオプションは{@link #getGetMessageOptions(String, long)}メソッドから取得する。
     * <br/>
     * メッセージ送信後のMQMDフィールドの取得は、
     * {@link WmqMqmdFieldsOperator#getFieldsAfterSend(MQMessage, SendingMessage)}メソッドに委譲する。
     * </p>
     * MQExceptionを捕捉した場合は、
     * {@link MessagingExceptionFactory#createMessagingException(String, Throwable)}メソッドに例外処理を委譲する。
     * 
     * @param mqQueue {@link MQQueue}
     * @param sendingMessage 送信メッセージ
     * @return メッセージID
     */
    protected String putMessage(MQQueue mqQueue, SendingMessage sendingMessage) {
        MQMessage mqMessage = new MQMessage();
        try {
            mqmdFieldsOperator.setFieldsBeforeSend(sendingMessage, mqMessage, defaultTimeToLive);
            mqMessage.write(sendingMessage.getBodyBytes());
            mqQueue.put(mqMessage, getPutMessageOptions());
            mqmdFieldsOperator.getFieldsAfterSend(mqMessage, sendingMessage);
        } catch (MQException e) {
            throw messagingExceptionFactory.createMessagingException(
                    "an error occurred while sending the message.", e);
        } catch (IOException e) {
            throw new MessagingException(e);
        }
        return sendingMessage.getMessageId();
    }

    /**
     * {@link MQQueue}へのメッセージ書き込みを制御するオプションを取得する。
     * <p/>
     * 下記オプションを指定した値を返す。
     * <p/>
     * <ul>
     * <li>{@link #getPutSyncpointOption()}メソッドが返す同期点オプション</li>
     * <li>{@link CMQC#MQPMO_NEW_MSG_ID}</li>
     * </ul>
     * @return {@link MQQueue}へのメッセージ書き込みを制御するオプション
     */
    protected MQPutMessageOptions getPutMessageOptions() {
        MQPutMessageOptions mqPutMessageOptions = new MQPutMessageOptions();
        int syncpointOption = getPutSyncpointOption();
        mqPutMessageOptions.options = syncpointOption | CMQC.MQPMO_NEW_MSG_ID;
        return mqPutMessageOptions;
    }

    /**
     * {@link MQQueue}へのポイズンメッセージ書き込みを制御するオプションを取得する。
     * <p/>
     * {@link #getPutMessageOptions()}に処理を委譲する。
     * 
     * @return {@link MQQueue}へのメッセージ書き込みを制御するオプション
     */
    protected MQPutMessageOptions getPutPoisonMessageOptions() {
        return getPutMessageOptions();
    }

    /**
     * メッセージ書き込み時に使用する同期点オプションを取得する。
     * <p/>
     * {@link #useXa}プロパティがtrueの場合は{@link CMQC#MQPMO_SYNCPOINT}、
     * falseの場合は{@link CMQC#MQPMO_NO_SYNCPOINT}を返す。
     * 
     * @return メッセージ書き込み時に使用する同期点オプション
     */
    protected int getPutSyncpointOption() {
        return useXa ? CMQC.MQPMO_SYNCPOINT : CMQC.MQPMO_NO_SYNCPOINT;
    }

    /**
     * 指定された{@link MQQueue}から受信メッセージを読み込む。
     * <p/>
     * {@link MQQueue}から読み込んだ{@link MQMessage}から受信メッセージを作成する。
     * <br/>
     * 読み込んだ{@link MQMessage}がポイズンメッセージの場合は
     * {@link #processPoisonMessage(MQQueue, MQMessage)}メソッドに処理を委譲する。
     * {@link MQMessage}がポイズンメッセージであるか否かの判定は
     * {@link #isPoisonMessage(MQMessage)}メソッドが行う。
     * <p/>
     * メッセージ受信前のMQMDフィールドの設定は
     * {@link WmqMqmdFieldsOperator#setFieldsBeforeReceive(String, MQMessage)}メソッドに委譲する。
     * <br/>
     * {@link MQQueue}への書き込みを制御するオプションは{@link #getGetMessageOptions(String, long)}メソッドから取得する。
     * <br/>
     * メッセージ受信後のMQMDフィールドの取得は
     * {@link WmqMqmdFieldsOperator#getFieldsAfterReceive(MQMessage, ReceivedMessage)}メソッドに委譲する。
     * <p/>
     * {@link MQException}が送出され、理由コードが{@link CMQC#MQRC_NO_MSG_AVAILABLE}の場合はnullを返す。
     * <p/>
     * MQExceptionを捕捉した場合は、
     * {@link MessagingExceptionFactory#createMessagingException(String, Throwable)}メソッド
     * に例外処理を委譲する。
     * 
     * @param receivedMqQueue {@link MQQueue}
     * @param messageId 相関IDに指定するメッセージID
     * @param timeout タイムアウト値(単位:msec)
     * @param poisonMqQueue ポイズンメッセージ送信先{@link MQQueue}。指定がない場合はnull
     * @return 受信メッセージ。受信できなかった場合はnull
     */
    protected ReceivedMessage getMessage(MQQueue receivedMqQueue, String messageId, long timeout, MQQueue poisonMqQueue) {
        MQMessage mqMessage = new MQMessage();
        try {
            mqmdFieldsOperator.setFieldsBeforeReceive(messageId, mqMessage);
            receivedMqQueue.get(mqMessage, getGetMessageOptions(messageId, timeout));

            if (isPoisonMessage(mqMessage)) {
                // バックアウト回数の上限値を超えている場合
                processPoisonMessage(poisonMqQueue, mqMessage);
                return null;
            }

            byte[] body = new byte[mqMessage.getDataLength()];
            mqMessage.readFully(body);
            ReceivedMessage receivedMessage = new ReceivedMessage(body);
            mqmdFieldsOperator.getFieldsAfterReceive(mqMessage, receivedMessage);
            return receivedMessage;
        } catch (MQException e) {
            if (e.reasonCode == CMQC.MQRC_NO_MSG_AVAILABLE) {
                return null;
            }
            throw messagingExceptionFactory.createMessagingException(
                    "an error occurred while receiving the message.", e);
        } catch (IOException e) {
            throw new MessagingException(e);
        }
    }

    /**
     * 指定された{@link MQMessage}がポイズンメッセージであるか否かを判定する。
     * <p/>
     * {@link MQMessage}のバックアウト回数が{@link #backoutLimit}プロパティより大きい場合にtrueを返す。
     * 
     * @param mqMessage {@link MQMessage}
     * @return ポイズンメッセージである場合はtrue
     */
    protected boolean isPoisonMessage(MQMessage mqMessage) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.logTrace(String.format("backoutLimit = [%s], backoutCount = [%s]",
                                          backoutLimit, mqMessage.backoutCount));
        }
        return backoutLimit < mqMessage.backoutCount;
    }

    /**
     * ポイズンメッセージの処理を行う。
     * <pre>
     * ポイズンメッセージ送信先{@link MQQueue}の指定有無により処理が分かれる。
     * 
     * ポイズンメッセージ送信先{@link MQQueue}の指定がない場合
     * 
     *   メッセージ受信処理が無限ループになるのを防止するために、
     *   {@link ProcessAbnormalEnd}を送出しプロセスを異常終了させる。
     *   {@link ProcessAbnormalEnd}の終了コードと障害コードには、
     *   {@link #backoutLimitExceededExitCode}プロパティと{@link #backoutLimitExceededFailureCode}プロパティを指定する。
     * 
     * ポイズンメッセージ送信先{@link MQQueue}が指定された場合
     * 
     *   FATALレベルで障害ログを出力し、指定された{@link MQQueue}にポイズンメッセージを送信する。
     *   送信時は、メッセージ存続時間を無制限、元々設定されていたメッセージIDを相関IDに設定する。
     *   ポイズンメッセージの送信で{@link MQException}が送出された場合は、
     *   送出された{@link MQException}がリトライ可能な例外である場合は再送出し、
     *   リトライ不可の場合は、メッセージ受信処理が無限ループになるのを防止するために、
     *   {@link ProcessAbnormalEnd}を送出しプロセスを異常終了させる。
     *   {@link ProcessAbnormalEnd}の終了コードと障害コードには、
     *   {@link #backoutLimitExceededExitCode}プロパティと{@link #backoutLimitExceededFailureCode}プロパティを指定する。
     * 
     * </pre>
     * @param poisonMqQueue ポイズンメッセージ送信先{@link MQQueue}。指定がない場合はnull
     * @param mqMessage ポイズンメッセージ
     * @throws ProcessAbnormalEnd ポイズンメッセージ送信先{@link MQQueue}の指定がない場合。
     *                             ポイズンメッセージの送信に失敗し、かつリトライ不可な場合
     * @throws MessagingException ポイズンメッセージの送信に失敗し、かつリトライ可能な場合
     */
    protected void processPoisonMessage(MQQueue poisonMqQueue, MQMessage mqMessage)
            throws ProcessAbnormalEnd, MessagingException {

        MessagingException backoutLimitExceededException
            = new MessagingException(String.format("backout limit was exceeded. messageId = [%s], backoutCount = [%s]",
                                     BinaryUtil.convertToHexString(mqMessage.messageId), mqMessage.backoutCount));

        if (poisonMqQueue == null) {
            // 退避キューが指定されていない場合(順序保障あり)
            throw new ProcessAbnormalEnd(backoutLimitExceededExitCode, backoutLimitExceededException, backoutLimitExceededFailureCode);
        }

        // 退避キューが指定されている場合(順序保障なし)
        try {
            FailureLogUtil.logFatal(backoutLimitExceededException, mqMessage, backoutLimitExceededFailureCode);
            mqMessage.expiry = CMQC.MQEI_UNLIMITED;
            mqMessage.correlationId = mqMessage.messageId;
            poisonMqQueue.put(mqMessage, getPutPoisonMessageOptions());
        } catch (MQException e) {
            // ポイズンメッセージ送信エラーの場合
            MessagingException sendingPoisonFailedException
                = messagingExceptionFactory.createMessagingException(
                        "an error occurred while sending the poison message.", e);
            if (sendingPoisonFailedException instanceof Retryable) {
                // リトライ可能な例外の場合
                throw sendingPoisonFailedException;
            } else {
                // 無限ループになるためプロセス異常終了
                throw new ProcessAbnormalEnd(putPoisonFailedExitCode, sendingPoisonFailedException, putPoisonFailedFailureCode);
            }
        }
    }

    /**
     * {@link MQQueue}からのメッセージ読み取りを制御するオプションを取得する。
     * <p/>
     * 下記オプションを指定した値を返す。
     * <ul>
     * <li>{@link #getGetSyncpointOption()}メソッドが返す同期点オプション</li>
     * <li>{@link CMQC#MQPMO_NEW_MSG_ID}</li>
     * <li>{@link CMQC#MQGMO_WAIT}</li>
     * <li>
     * 相関IDに指定するメッセージIDが指定された場合は{@link CMQC#MQMO_MATCH_CORREL_ID}、
     * 指定がない場合は{@link CMQC#MQMO_NONE}
     * </li>
     * <li>
     * タイムアウト値。タイムアウト値が0以下の場合は{@link #defaultResponseTimeout}プロパティの値を使用する。
     * </li>
     * </ul>
     * @param messageId 相関IDに指定するメッセージID
     * @param timeout タイムアウト値(単位:msec)
     * @return {@link MQQueue}からのメッセージ読み取りを制御するオプション
     */
    protected MQGetMessageOptions getGetMessageOptions(String messageId, long timeout) {
        MQGetMessageOptions mqGetMessageOptions = new MQGetMessageOptions();
        int syncpointOption = getGetSyncpointOption();
        mqGetMessageOptions.options = syncpointOption | CMQC.MQGMO_WAIT;
        mqGetMessageOptions.matchOptions = StringUtil.hasValue(messageId)
                                            ? CMQC.MQMO_MATCH_CORREL_ID : CMQC.MQMO_NONE;
        mqGetMessageOptions.waitInterval = (int) (timeout <= 0 ? defaultResponseTimeout : timeout);
        return mqGetMessageOptions;
    }

    /**
     * メッセージ読み取り時に使用する同期点オプションを取得する。
     * <p/>
     * {@link #useXa}プロパティがtrueの場合は{@link CMQC#MQPMO_SYNCPOINT}、
     * falseの場合は{@link CMQC#MQPMO_NO_SYNCPOINT}を返す。
     * 
     * @return メッセージ読み取り時に使用する同期点オプション
     */
    protected int getGetSyncpointOption() {
        return useXa ? CMQC.MQGMO_SYNCPOINT : CMQC.MQGMO_NO_SYNCPOINT;
    }

    /**
     * {@link MQQueueManager}の接続を切断する。
     * <p/>
     * MQExceptionが送出された場合はTRACEレベルのログを出力し、MQExceptionを送出しない。
     * 
     * @param mqQueueManager {@link MQQueueManager}
     */
    protected void disconnect(MQQueueManager mqQueueManager) {
        if (mqQueueManager == null) {
            return;
        }
        try {
            mqQueueManager.disconnect();
        } catch (MQException e) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.logTrace("could not close IBM MQ MQQueueManager.", e);
            }
        }
    }

    /**
     * 指定された全ての{@link MQQueue}をクローズする。
     * <p/>
     * 個々の{@link MQQueue}を指定して{@link #close(MQQueue)}を呼び出す。
     * 
     * @param mqQueues {@link MQQueue}マップ
     */
    protected void close(Map<String, MQQueue> mqQueues) {
        if (mqQueues == null) {
            return;
        }
        for (MQQueue mqQueue : mqQueues.values()) {
            close(mqQueue);
        }
    }

    /**
     * 指定された{@link MQQueue}をクローズする。
     * <p/>
     * MQExceptionが送出された場合はTRACEレベルのログを出力し、MQExceptionを送出しない。
     * <p/>
     * 
     * @param mqQueue {@link MQQueue}
     */
    protected void close(MQQueue mqQueue) {
        if (mqQueue == null) {
            return;
        }
        try {
            mqQueue.close();
        } catch (MQException e) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.logTrace("could not close IBM MQ MQQueue.", e);
            }
        }
    }

    /**
     * {@link MQQueueManager}から{@link java.sql.Connection}を取得する。
     * <p/>
     * MQExceptionを捕捉した場合は、
     * {@link MessagingExceptionFactory#createMessagingException(String, Throwable)}メソッド
     * に例外処理を委譲する。
     * 
     * @param mqQueueManager {@link MQQueueManager}
     * @param xaDataSource XAデータソース
     * @return {@link java.sql.Connection}
     * @throws SQLException {@link java.sql.Connection}の取得に失敗した場合
     * @throws XAException {@link java.sql.Connection}の取得に失敗した場合
     */
    protected Connection getJdbcConnection(MQQueueManager mqQueueManager, XADataSource xaDataSource)
            throws SQLException, XAException {
        checkXa();
        try {
            return mqQueueManager.getJDBCConnection(xaDataSource);
        } catch (MQException e) {
            throw messagingExceptionFactory.createMessagingException(
                                "failed to get database connection.", e);
        }
    }

    /**
     * 分散トランザクションを使用するに設定されていることをチェックする。
     * <p/>
     * {@link #useXa}プロパティがfalseの場合は{@link IllegalStateException}を送出する。
     */
    protected void checkXa() {
        if (!useXa) {
            throw new IllegalStateException(
                    "XA configuration was incorrect. "
                  + "must be set true to WmqMessagingProvider's useXa property");
        }
    }

    /**
     * 分散トランザクションを開始する。
     * <p/>
     * MQExceptionを捕捉した場合は、
     * {@link MessagingExceptionFactory#createMessagingException(String, Throwable)}メソッド
     * に例外処理を委譲する。
     *
     * @param mqQueueManager {@link MQQueueManager}
     */
    protected void begin(MQQueueManager mqQueueManager) {
        checkXa();
        try {
            mqQueueManager.begin();
        } catch (MQException e) {
            throw messagingExceptionFactory.createMessagingException(
                                        "failed to begin transaction", e);
        }
    }

    /**
     * 分散トランザクションをコミットする。
     * <p/>
     * MQExceptionを捕捉した場合は、
     * {@link MessagingExceptionFactory#createMessagingException(String, Throwable)}メソッド
     * に例外処理を委譲する。
     *
     * @param mqQueueManager {@link MQQueueManager}
     */
    protected void commit(MQQueueManager mqQueueManager) {
        checkXa();
        try {
            mqQueueManager.commit();
        } catch (MQException e) {
            throw messagingExceptionFactory.createMessagingException(
                                        "failed to commit transaction", e);
        }
    }

    /**
     * 分散トランザクションをバックアウトする。
     * <p/>
     * MQExceptionを捕捉した場合は、
     * {@link MessagingExceptionFactory#createMessagingException(String, Throwable)}メソッド
     * に例外処理を委譲する。
     *
     * @param mqQueueManager {@link MQQueueManager}
     */
    protected void backout(MQQueueManager mqQueueManager) {
        checkXa();
        try {
            mqQueueManager.backout();
        } catch (MQException e) {
            throw messagingExceptionFactory.createMessagingException(
                                        "failed to backout transaction", e);
        }
    }

    /**
     * キューマネージャ名称を設定する。
     * @param queueManagerName キューマネージャ名称
     * @return このオブジェクト自体
     */
    public WmqMessagingProvider setQueueManagerName(String queueManagerName) {
        this.queueManagerName = queueManagerName;
        return this;
    }

    /**
     * 送信キュー名(論理名)リストを設定する。
     * @param sendingQueueNames 送信キュー名(論理名)リスト
     * @return このオブジェクト自体
     */
    public WmqMessagingProvider setSendingQueueNames(List<String> sendingQueueNames) {
        this.sendingQueueNames = sendingQueueNames;
        return this;
    }

    /**
     * 受信キュー名(論理名)を設定する。
     * @param receivedQueueName 受信キュー名(論理名)
     * @return このオブジェクト自体
     */
    public WmqMessagingProvider setReceivedQueueName(String receivedQueueName) {
        this.receivedQueueName = receivedQueueName;
        return this;
    }

    /**
     * 退避キュー名パターン(論理名)を設定する。
     * <pre>
     * 受信先キュー名を使用した命名規約により退避キュー名が規定されている場合は、
     * 本プロパティを指定する。
     * 本プロパティの設定例を示す。
     * 
     *     設定値: "%s.POISON"
     *     受信先キュー名が"TEST"の場合は、"TEST.POISON"となる。
     * 
     * </pre>
     * @param poisonQueueNamePattern 退避キュー名パターン(論理名)
     */
    public void setPoisonQueueNamePattern(String poisonQueueNamePattern) {
        this.poisonQueueNamePattern = poisonQueueNamePattern;
    }

    /**
     * 退避キュー名(論理名)を設定する。
     * <pre>
     * 退避キュー名が受信先キュー名と命名規約により規定されていない場合は、
     * 本プロパティを指定する。
     * </pre>
     * @param poisonQueueName 退避キュー名(論理名)
     */
    public void setPoisonQueueName(String poisonQueueName) {
        this.poisonQueueName = poisonQueueName;
    }

    /**
     * バックアウト回数の上限値を設定する。
     * <p/>
     * デフォルトは0。
     * @param backoutLimit バックアウト回数の上限値
     * @return このオブジェクト自体
     */
    public WmqMessagingProvider setBackoutLimit(int backoutLimit) {
        this.backoutLimit = backoutLimit;
        return this;
    }

    /**
     * バックアウト上限を超えた場合に使用する終了コード(プロセスを終了({@link System#exit(int)})する際に設定する値)を設定する。
     * <p/>
     * デフォルトは190。
     * @param backoutLimitExceededExitCode バックアウト上限を超えた場合に使用する終了コード(プロセスを終了({@link System#exit(int)})する際に設定する値)
     * @return このオブジェクト自体 
     */
    public WmqMessagingProvider setBackoutLimitExceededExitCode(int backoutLimitExceededExitCode) {
        this.backoutLimitExceededExitCode = backoutLimitExceededExitCode;
        return this;
    }

    /**
     * バックアウト上限を超えた場合に使用する障害コードを設定する。
     * @param backoutLimitExceededFailureCode バックアウト上限を超えた場合に使用する障害コード
     * @return このオブジェクト自体
     */
    public WmqMessagingProvider setBackoutLimitExceededFailureCode(String backoutLimitExceededFailureCode) {
        this.backoutLimitExceededFailureCode = backoutLimitExceededFailureCode;
        return this;
    }

    /**
     * ポイズンメッセージの送信に失敗した場合に使用する終了コード(プロセスを終了({@link System#exit(int)})する際に設定する値)を設定する。
     * <p/>
     * デフォルトは191。
     * @param putPoisonFailedExitCode ポイズンメッセージの送信に失敗した場合に使用する終了コード(プロセスを終了({@link System#exit(int)})する際に設定する値)
     */
    public void setPutPoisonFailedExitCode(int putPoisonFailedExitCode) {
        this.putPoisonFailedExitCode = putPoisonFailedExitCode;
    }

    /**
     * ポイズンメッセージの送信に失敗した場合に使用する障害コードを設定する。
     * @param putPoisonFailedFailureCode ポイズンメッセージの送信に失敗した場合に使用する障害コード
     */
    public void setPutPoisonFailedFailureCode(String putPoisonFailedFailureCode) {
        this.putPoisonFailedFailureCode = putPoisonFailedFailureCode;
    }

    /**
     * デフォルトタイムアウト値(単位:msec)を設定する。
     * <p/>
     * デフォルトは300秒。
     * 
     * @param defaultResponseTimeout デフォルトタイムアウト値(単位:msec)
     * @return このオブジェクト自体
     */
    public MessagingProvider setDefaultResponseTimeout(long defaultResponseTimeout) {
        this.defaultResponseTimeout = defaultResponseTimeout;
        return this;
    }

    /**
     * 送信電文の有効期間(単位:msec)を設定する。
     * <p/>
     * デフォルトは60秒。
     * 
     * @param defaultTimeToLive 送信電文の有効期間(単位:msec)
     * @return このオブジェクト自体
     */
    public MessagingProvider setDefaultTimeToLive(long defaultTimeToLive) {
        this.defaultTimeToLive = defaultTimeToLive;
        return this;
    }

    /**
     * {@link WmqMqmdFieldsOperator}オブジェクトを設定する。
     * <p/>
     * デフォルトは{@link BasicWmqMqmdFieldsOperator}。
     * 
     * @param mqmdFieldsOperator {@link WmqMqmdFieldsOperator}オブジェクト
     * @return このオブジェクト自体
     */
    public WmqMessagingProvider setMqmdFieldsOperator(WmqMqmdFieldsOperator mqmdFieldsOperator) {
        this.mqmdFieldsOperator = mqmdFieldsOperator;
        return this;
    }

    /**
     * {@link MessagingException}ファクトリオブジェクトを設定する。
     * <p/>
     * デフォルトは{@link BasicWmqMessagingExceptionFactory}。
     * 
     * @param messagingExceptionFactory {@link MessagingException}ファクトリオブジェクト
     * @return このオブジェクト自体
     */
    public WmqMessagingProvider setMessagingExceptionFactory(
            MessagingExceptionFactory messagingExceptionFactory) {
        this.messagingExceptionFactory = messagingExceptionFactory;
        return this;
    }

    /**
     * 分散トランザクションを使用するか否かを設定する。
     * <p/>
     * デフォルトはtrue。
     * 
     * @param useXa 分散トランザクションを使用する場合はtrue
     */
    public void setUseXa(boolean useXa) {
        this.useXa = useXa;
    }

    /**
     * IBM MQによる{@link MQException}発生時の標準エラー出力を使用するか否かを設定する。
     *
     * <p><b>IBM MQ8.0系以降でロギング機能が削除されたため、本プロパティは廃止しました。(値を設定しても意味がありません)</b>
     * 
     * @param useProductSystemErrorOutput
     *     IBM MQによる{@link MQException}発生時の標準エラー出力を使用する場合はtrue
     */
    @IgnoreProperty("IBM MQ8.0系以降でロギング機能が削除されたため、本プロパティは廃止しました。(値を設定しても意味がありません)")
    public void setUseProductSystemErrorOutput(boolean useProductSystemErrorOutput) {
        this.useProductSystemErrorOutput = useProductSystemErrorOutput;
    }
}
