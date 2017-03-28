package nablarch.integration.messaging.wmq.xa;

import java.sql.Connection;

import nablarch.core.transaction.Transaction;
import nablarch.core.transaction.TransactionFactory;

/**
 * {@link WmqXATransaction}を生成するクラス。
 * @author Kiyohito Itoh
 */
public class WmqXATransactionFactory implements TransactionFactory {

    /** アイソレーションレベル */
    private int isolationLevel = Connection.TRANSACTION_READ_COMMITTED;

    /**
     * {@link WmqXATransaction}を生成する。
     *
     * @param connectionName コネクション名
     * @return トランザクションオブジェクト
     */
    public Transaction getTransaction(String connectionName) {
        WmqXATransaction transaction = new WmqXATransaction(connectionName);
        transaction.setIsolationLevel(isolationLevel);
        return transaction;
    }

    /**
     * アイソレーションレベルを設定する。<br>
     * 設定できる値は、下記のとおり。<br>
     * READ_COMMITTED:{@link java.sql.Connection#TRANSACTION_READ_COMMITTED}<br>
     * READ_UNCOMMITTED:{@link java.sql.Connection#TRANSACTION_READ_UNCOMMITTED}<br>
     * REPEATABLE_READ:{@link java.sql.Connection#TRANSACTION_REPEATABLE_READ}<br>
     * SERIALIZABLE:{@link java.sql.Connection#TRANSACTION_SERIALIZABLE}<br>
     * アイソレーションレベルが設定されない場合は、
     * デフォルトで{@link java.sql.Connection#TRANSACTION_READ_COMMITTED}が使用される。
     *
     * @param isolationLevel アイソレーションレベルを表す文字列。
     */
    public void setIsolationLevel(String isolationLevel) {
        if ("READ_COMMITTED".equals(isolationLevel)) {
            this.isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
        } else if ("READ_UNCOMMITTED".equals(isolationLevel)) {
            this.isolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
        } else if ("REPEATABLE_READ".equals(isolationLevel)) {
            this.isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
        } else if ("SERIALIZABLE".equals(isolationLevel)) {
            this.isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
        } else {
            throw new IllegalArgumentException(
                    "invalid isolation level. isolation level:" + isolationLevel);
        }
    }
}
