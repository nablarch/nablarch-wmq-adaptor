package nablarch.integration.messaging.wmq.xa;

import java.sql.Connection;

import nablarch.core.db.connection.BasicDbConnection;

/**
 * WebSphere MQを使用した分散トランザクションに対応した
 * {@link nablarch.core.db.connection.TransactionManagerConnection}の実装クラス。
 * <p/>
 * WebSphere MQの実装に合わせて下記メソッドをオーバーライドする。
 * <ul>
 * <li>{@link #commit()}</li>
 * <li>{@link #rollback()}</li>
 * <li>{@link #closeConnection()}</li>
 * </ul>
 * その他の振る舞いは、{@link BasicDbConnection}と同じ。
 * 
 * @author Kiyohito Itoh
 */
public class WmqXADbConnection extends BasicDbConnection {

    /**
     * 指定されたデータ接続を保持するオブジェクトを生成する。
     * @param con データベース接続オブジェクト
     */
    public WmqXADbConnection(Connection con) {
        super(con);
    }

    /**
     * 分散トランザクションのため何もしない。
     */
    @Override
    public void commit() {
    }

    /**
     * 分散トランザクションのため何もしない。
     */
    @Override
    public void rollback() {
    }

    /**
     * 分散トランザクションのため何もしない。
     * <p/>
     * {@link com.ibm.mq.MQQueueManager#disconnect()}にてコネクションがクローズされる。
     */
    @Override
    protected void closeConnection() {
    }
}
