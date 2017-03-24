package nablarch.integration.messaging.wmq.provider.exception;

import java.util.HashSet;
import java.util.Set;

import nablarch.fw.messaging.MessagingException;
import nablarch.fw.messaging.provider.MessagingExceptionFactory;
import nablarch.fw.messaging.provider.exception.MomConnectionException;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.CMQC;

/**
 * WebSphere MQを使用したメッセージング機能向けの{@link MessagingExceptionFactory}の基本実装クラス。
 * @author Kiyohito Itoh
 */
public class BasicWmqMessagingExceptionFactory implements MessagingExceptionFactory {

    /** 発生した例外がMOM接続に関する問題である場合の理由コード */
    @SuppressWarnings("serial")
    private static final Set<Integer> REASON_CODES_FOR_MOM_CONNECTION_ERROR
        = new HashSet<Integer>() {
            {
                add(CMQC.MQRC_CONNECTION_BROKEN);
                add(CMQC.MQRC_CONNECTION_QUIESCING);
                add(CMQC.MQRC_CONNECTION_STOPPING);
                add(CMQC.MQRC_OPEN_FAILED);
                add(CMQC.MQRC_Q_MGR_NOT_AVAILABLE);
                add(CMQC.MQRC_Q_MGR_QUIESCING);
                add(CMQC.MQRC_Q_MGR_STOPPING);
                add(CMQC.MQRC_NO_EXTERNAL_PARTICIPANTS);
                add(CMQC.MQRC_UNEXPECTED_ERROR);
            }
        };

    /**
     * {@inheritDoc}
     * <p/>
     * 発生した例外がMOM接続に関する問題である場合は、{@link MomConnectionException}を生成する。
     * MOM接続に関する問題でない場合は、{@link MessagingException}を生成する。
     * </p>
     * 発生した例外がMOM接続に関する問題であるか否かの判定は、
     * {@link #isMomConnectionError(Throwable)}メソッドに委譲する。
     */
    public MessagingException createMessagingException(String message, Throwable cause) {
        if (isMomConnectionError(cause)) {
            return new MomConnectionException(message, cause);
        }
        return new MessagingException(message, cause);
    }

    /**
     * 発生した例外がMOM接続に関する問題であるか否かを判定する。
     * <p/>
     * 基本実装では、発生した例外がMQExceptionである、かつ下記理由コードの場合にtrueを返す。
     * <ul>
     * <li>{@link CMQC#MQRC_CONNECTION_BROKEN}</li>
     * <li>{@link CMQC#MQRC_CONNECTION_QUIESCING}</li>
     * <li>{@link CMQC#MQRC_CONNECTION_STOPPING}</li>
     * <li>{@link CMQC#MQRC_OPEN_FAILED}</li>
     * <li>{@link CMQC#MQRC_Q_MGR_NOT_AVAILABLE}</li>
     * <li>{@link CMQC#MQRC_Q_MGR_QUIESCING}</li>
     * <li>{@link CMQC#MQRC_Q_MGR_STOPPING}</li>
     * <li>{@link CMQC#MQRC_NO_EXTERNAL_PARTICIPANTS}</li>
     * <li>{@link CMQC#MQRC_UNEXPECTED_ERROR}</li>
     * </ul>
     * @param t 発生した例外
     * @return 発生した例外がMOM接続に関する問題である場合はtrue
     */
    protected boolean isMomConnectionError(Throwable t) {
        if (t instanceof MQException) {
            MQException e = (MQException) t;
            if (REASON_CODES_FOR_MOM_CONNECTION_ERROR.contains(e.reasonCode)) {
                return true;
            }
        }
        Throwable cause = t.getCause();
        return cause != null && isMomConnectionError(cause);
    }
}
