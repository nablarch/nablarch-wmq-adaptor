package nablarch.integration.messaging.wmq.xa;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XADataSource;
import javax.transaction.xa.XAException;

import nablarch.core.db.connection.BasicDbConnection;
import nablarch.core.db.connection.ConnectionFactorySupport;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.integration.messaging.wmq.provider.WmqMessagingContext;

/**
 * IBM MQを使用した分散トランザクションに対応したデータベース接続を生成するクラス。
 * @author Kiyohito Itoh
 */
public class WmqXADbConnectionFactoryForXADataSource extends ConnectionFactorySupport {

    /** XAデータソース */
    private XADataSource xaDataSource;

    /**
     * {@inheritDoc}
     * <p/>
     * {@link WmqMessagingContext#getJdbcConnection(XADataSource)}メソッドを呼び出し、
     * 取得した{@link java.sql.Connection}を使用して{@link BasicDbConnection}を生成する。
     */
    @Override
    public TransactionManagerConnection getConnection(String name) {

        Connection connection;
        try {
            connection = WmqMessagingContext.getInstance().getJdbcConnection(xaDataSource);
        } catch (SQLException e) {
            throw dbAccessExceptionFactory.createDbAccessException(
                            "failed to get database connection.", e, null);
        } catch (XAException e) {
            throw new RuntimeException("failed to get database connection.", e);
        }

        BasicDbConnection dbConnection = new WmqXADbConnection(connection);
        initConnection(dbConnection, name);

        return dbConnection;
    }

    /**
     * XAデータソースを設定する。
     * @param xaDataSource XAデータソース
     */
    public void setXaDataSource(XADataSource xaDataSource) {
        this.xaDataSource = xaDataSource;
    }
}
