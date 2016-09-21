package nablarch.integration.messaging.wmq.tool;

import java.text.SimpleDateFormat;
import java.util.Date;

import nablarch.core.util.StringUtil;

/**
 * 同期応答メッセージ受信サンプルの打鍵テスト用にメッセージを書き込む。
 * <pre>
 * 再送メッセージのテストは、{@link RePutMessageForSyncMessageReceive}を使用する。
 * 
 * 同期応答メッセージ受信サンプルの起動方法
 * 
 * 起動クラス
 *     {@link nablarch.fw.launcher.Main}
 * JVM引数
 *     -diConfig classpath:messaging-component-configuration.xml -userId M_SVR_USER -requestPath ss11AC.MessagingServer/RB11AC0160
 * 
 * </pre>
 * @author Kiyohito Itoh
 */
public class PutMessageForSyncMessageReceive {

    private static final String QUEUE_MANAGER_NAME = "TEST";
    private static final String SEND_QUEUE_NAME = "TEST.REQUEST";
    private static final String REPLY_TO_QUEUE_NAME = "TEST.RESPONSE";
    private static final int SIZE = 50;
    private static final int LOOP = 1;
    private static final int WAIT = 5000;

    public static void main(String[] args) throws Exception {

        int sum = 0;
        for (int loop = 0; loop < LOOP; loop++) {
            for (int i = 0; i < SIZE; i++) {
    
                String message = createBodyDataForSyncMessageReceive();
    
                ToolUtil.putRequestMessage(QUEUE_MANAGER_NAME, SEND_QUEUE_NAME,
                                          message, REPLY_TO_QUEUE_NAME);
            }

            sum += SIZE;
            System.out.println("**************************************** sum[" + sum + "]");

            Thread.sleep(WAIT);
        }
    }

    public static String createBodyDataForSyncMessageReceive() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd_HH.mm.ss_SSS");
        char xChar = ' ';
        char nChar = '　';
        String date = df.format(new Date());

        StringBuilder message = new StringBuilder()

            // ヘッダ部
            .append(StringUtil.rpad("RM11AC0101", 20, xChar))   // リクエストID
            .append(StringUtil.rpad("0", 1, xChar))             // 再送要求フラグ (0: 初回送信 1: 再送要求)
            .append(StringUtil.rpad("", 8, xChar))              // ステータスコード
            .append(StringUtil.rpad("", 21, xChar))             // 予備領域
            .append("\r\n")

            // データ部
            .append(StringUtil.rpad("1234567890", 20, xChar))   // ログインID
            .append(StringUtil.rpad("漢字氏名", 50, nChar))     // 漢字指名
            .append(StringUtil.rpad("カナメイ", 50, nChar))     // カナ指名
            .append(StringUtil.rpad("", 50, xChar))             // 空白領域
            .append(StringUtil.rpad(date + "@test.co.jp", 100, xChar)) // メールアドレス
            .append(StringUtil.rpad("11", 2, xChar))            // 内線番号（ビル番号）
            .append(StringUtil.rpad("2222", 4, xChar))          // 内線番号（個人番号）
            .append(StringUtil.rpad("333", 3, xChar))           // 携帯番号（市外）
            .append(StringUtil.rpad("4444", 4, xChar))          // 携帯電話番号(市内)
            .append(StringUtil.rpad("5555", 4, xChar))          // 携帯電話番号(加入)
            .append(StringUtil.rpad("", 33, xChar))             // 空白領域
            .append("\r\n");

        return message.toString();
    }
}
