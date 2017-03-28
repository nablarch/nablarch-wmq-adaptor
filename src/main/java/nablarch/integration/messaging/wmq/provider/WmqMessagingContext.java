package nablarch.integration.messaging.wmq.provider;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.XADataSource;
import javax.transaction.xa.XAException;

import nablarch.fw.messaging.MessagingContext;
import nablarch.fw.messaging.MessagingException;
import nablarch.fw.messaging.ReceivedMessage;
import nablarch.fw.messaging.SendingMessage;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;

/**
 * WebSphere MQを使用した{@link MessagingContext}の実装クラス。
 * @author Kiyohito Itoh
 */
public class WmqMessagingContext extends MessagingContext {

    /**
     * カレントスレッドに紐づけられている{@link WmqMessagingContext}を取得する。
     * @return カレントスレッドに紐づけられている{@link WmqMessagingContext}
     */
    public static WmqMessagingContext getInstance() {
        return (WmqMessagingContext) MessagingContext.getInstance();
    }

    /** {@link WmqMessagingProvider} */
    private final WmqMessagingProvider provider;

    /** {@link MQQueueManager} */
    private final MQQueueManager mqQueueManager;

    /** 送信先{@link MQQueue}マップ(キーはキュー名) */
    private final Map<String, MQQueue> sendingMqQueues;

    /** 受信先{@link MQQueue} */
    private final MQQueue receivedMqQueue;

    /** ポイズンメッセージ送信先{@link MQQueue} */
    private final MQQueue poisonMqQueue;

    /**
     * コンストラクタ。
     * @param provider {@link WmqMessagingProvider}
     * @param mqQueueManager {@link MQQueueManager}
     * @param sendingMqQueues 送信先{@link MQQueue}マップ(キーはキュー名)
     * @param receivedMqQueue 受信先{@link MQQueue}
     * @param poisonMqQueue ポイズンメッセージ送信先{@link MQQueue}
     */
    public WmqMessagingContext(WmqMessagingProvider provider,
                                MQQueueManager mqQueueManager,
                                Map<String, MQQueue> sendingMqQueues,
                                MQQueue receivedMqQueue,
                                MQQueue poisonMqQueue) {
        this.provider = provider;
        this.mqQueueManager = mqQueueManager;
        this.sendingMqQueues = sendingMqQueues;
        this.receivedMqQueue = receivedMqQueue;
        this.poisonMqQueue = poisonMqQueue;
    }

    /**
     * {@inheritDoc}
     * </p>
     * {@link WmqMessagingProvider#putMessage(MQQueue, SendingMessage)}メソッドに処理を委譲する。
     */
    @Override
    public String sendMessage(SendingMessage sendingMessage) {
        MQQueue mqQueue = getMQQueue(sendingMqQueues, sendingMessage.getDestination());
        return provider.putMessage(mqQueue, sendingMessage);
    }

    /**
     * {@inheritDoc}
     * </p>
     * {@link WmqMessagingProvider#getMessage(MQQueue, String, long, MQQueue)}メソッドに処理を委譲する。
     */
    @Override
    public ReceivedMessage receiveMessage(String receiveQueue, String messageId, long timeout) {
        checkReceivedQueueName(receiveQueue);
        return provider.getMessage(receivedMqQueue, messageId, timeout, poisonMqQueue);
    }

    /**
     * 指定された受信キュー名に対応するキューが登録されているか否かをチェックする。
     * @param receiveQueue 受信キュー名
     * @throws MessagingException 指定されたキュー名に対する{@link MQQueue}が登録されていなかった場合
     */
    protected void checkReceivedQueueName(String receiveQueue)
            throws MessagingException {
        try {
            if (receivedMqQueue == null || !receivedMqQueue.getName().trim().equals(receiveQueue)) {
                throw new MessagingException("queue was not found. queueName = [" + receiveQueue + "]");
            }
        } catch (MQException e) {
            throw new MessagingException("failed to get queue name.", e);
        }
    }

    /**
     * {@link MQQueue}マップから指定されたキュー名の{@link MQQueue}を取得する。
     * @param mqQueues {@link MQQueue}マップ
     * @param queueName キュー名
     * @return {@link MQQueue}
     * @throws MessagingException 指定されたキュー名に対する{@link MQQueue}が登録されていなかった場合
     */
    protected MQQueue getMQQueue(Map<String, MQQueue> mqQueues, String queueName)
            throws MessagingException {
        if (!mqQueues.containsKey(queueName)) {
            throw new MessagingException("queue was not found. queueName = [" + queueName + "]");
        }
        return mqQueues.get(queueName);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * {@link WmqMessagingProvider#close(Map)}メソッド、
     * {@link WmqMessagingProvider#close(MQQueue)}メソッド、
     * {@link WmqMessagingProvider#disconnect(MQQueueManager)}メソッド
     * を順に呼び出し、処理を委譲する。
     */
    @Override
    public void close() {
        provider.close(sendingMqQueues);
        provider.close(receivedMqQueue);
        provider.close(poisonMqQueue);
        provider.disconnect(mqQueueManager);
    }

    /**
     * {@link MQQueueManager}から{@link java.sql.Connection}を取得する。
     * <p/>
     * {@link WmqMessagingProvider#getJdbcConnection(MQQueueManager, XADataSource)}メソッドに処理を委譲する。
     * 
     * @param xaDataSource XAデータソース
     * @return {@link java.sql.Connection}
     * @throws SQLException {@link java.sql.Connection}の取得に失敗した場合
     * @throws XAException {@link java.sql.Connection}の取得に失敗した場合
     */
    public Connection getJdbcConnection(XADataSource xaDataSource) throws SQLException, XAException {
        return provider.getJdbcConnection(mqQueueManager, xaDataSource);
    }

    /**
     * 分散トランザクションを開始する。
     * <p/>
     * {@link WmqMessagingProvider#begin(MQQueueManager)}メソッドに処理を委譲する。
     */
    public void begin() {
        provider.begin(mqQueueManager);
    }

    /**
     * 分散トランザクションをコミットする。
     * <p/>
     * {@link WmqMessagingProvider#commit(MQQueueManager)}メソッドに処理を委譲する。
     */
    public void commit() {
        provider.commit(mqQueueManager);
    }

    /**
     * 分散トランザクションをバックアウトする。
     * <p/>
     * {@link WmqMessagingProvider#backout(MQQueueManager)}メソッドに処理を委譲する。
     */
    public void backout() {
        provider.backout(mqQueueManager);
    }
}
