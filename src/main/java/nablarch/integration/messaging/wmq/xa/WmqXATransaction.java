package nablarch.integration.messaging.wmq.xa;

import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.transaction.Transaction;
import nablarch.integration.messaging.wmq.provider.WmqMessagingContext;

/**
 * WebSphere MQを使用した分散トランザクションの制御を行うクラス。
 * @author Kiyohito Itoh
 */
public class WmqXATransaction implements Transaction {

    /** SQLログを出力するロガー */
    private static final Logger SQL_LOGGER = LoggerManager.get("SQL");

    /** インスタンスクラス名 */
    private static final String CLASS_NAME = WmqXATransaction.class.getName();

    /** コネクション名 */
    private final String connectionName;

    /** アイソレーションレベル */
    private int isolationLevel;

    /**
     * 指定されたコネクション名に対するトランザクションオブジェクトを生成する。
     * 
     * @param connectionName コネクション名
     */
    public WmqXATransaction(String connectionName) {
        this.connectionName = connectionName;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * トランザクション開始後にアイソレーションレベルを設定する。
     */
    public void begin() {
        WmqMessagingContext.getInstance().begin();
        if (SQL_LOGGER.isDebugEnabled()) {
            SQL_LOGGER.logDebug(CLASS_NAME + "#begin()");
        }
        TransactionManagerConnection con
            = (TransactionManagerConnection) DbConnectionContext.getConnection(connectionName);
        con.setIsolationLevel(isolationLevel);
    }

    /**
     * {@inheritDoc}
     */
    public void commit() {
        WmqMessagingContext.getInstance().commit();
        if (SQL_LOGGER.isDebugEnabled()) {
            SQL_LOGGER.logDebug(CLASS_NAME + "#commit()");
        }
    }

    /**
     * {@inheritDoc}
     */
    public void rollback() {
        WmqMessagingContext.getInstance().backout();
        if (SQL_LOGGER.isDebugEnabled()) {
            SQL_LOGGER.logDebug(CLASS_NAME + "#rollback()");
        }
    }

    /**
     * アイソレーションレベルを設定する。
     * @param isolationLevel アイソレーションレベル
     */
    void setIsolationLevel(int isolationLevel) {
        this.isolationLevel = isolationLevel;
    }
}
