package nablarch.integration.messaging.wmq.tool;

import java.text.SimpleDateFormat;
import java.util.Date;

import nablarch.core.util.StringUtil;

import com.ibm.mq.constants.CMQC;

/**
 * 同期応答メッセージ送信サンプルの打鍵テスト用にメッセージを書き込む。
 * <pre>
 * 同期応答メッセージ送信サンプルの起動方法
 * 
 * 画面オンライン処理のユーザ登録確認画面からメッセージ送信ボタンを選択する。
 * 
 * </pre>
 * @author Kiyohito Itoh
 */
public class PutMessageForSyncMessageSend {

    private static final String QUEUE_MANAGER_NAME = "TEST";
    private static final String RECEIVE_QUEUE_NAME = "TEST.REQUEST";
    private static final String SEND_QUEUE_NAME = "TEST.RESPONSE";

    public static void main(String[] args) throws Exception {

        byte[] firstMessageId = CMQC.MQMI_NONE;
        while (true) {
            firstMessageId = ToolUtil.getFirstMessageId(QUEUE_MANAGER_NAME, RECEIVE_QUEUE_NAME);
            if (firstMessageId != CMQC.MQMI_NONE) {
                break;
            }
            Thread.sleep(2000);
        }

        SimpleDateFormat df = new SimpleDateFormat("HHmmssSSS");
        char xChar = ' ';

        String date = df.format(new Date());

        StringBuilder message = new StringBuilder()

        // ヘッダ部
        .append(StringUtil.rpad("RM11AC0201", 20, xChar))       // リクエストID
        .append(StringUtil.rpad("0", 1, xChar))                 // 再送要求フラグ (0: 初回送信 1: 再送要求)
        .append(StringUtil.rpad("200", 8, xChar))               // ステータスコード
        .append(StringUtil.rpad("", 21, xChar))                 // 予備領域
        .append("\r\n")

        // データ部
        .append(StringUtil.rpad("0", 1, xChar))                 // データ区分(固定)
        .append(StringUtil.lpad(date, 10, '0'))                 // 採番したユーザID
        .append(StringUtil.rpad("", 20, xChar))                 // 障害事由コード
        .append(StringUtil.rpad("", 20, xChar))                 // 問い合わせID
        .append(StringUtil.rpad("", 369, xChar))                // 空白領域
        .append("\r\n");

        ToolUtil.putResponseMessage(QUEUE_MANAGER_NAME, SEND_QUEUE_NAME, message.toString(), firstMessageId);
    }
}
