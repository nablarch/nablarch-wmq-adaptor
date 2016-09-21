package nablarch.integration.messaging.wmq.tool;

import java.text.SimpleDateFormat;
import java.util.Date;

import nablarch.core.util.StringUtil;

/**
 * 応答不要メッセージ受信サンプルの打鍵テスト用にメッセージを書き込む。
 * <pre>
 * 応答不要メッセージ受信サンプルの起動方法
 * 
 * 起動クラス
 *     {@link nablarch.fw.launcher.Main}
 * JVM引数
 *     -diConfig classpath:receive-messaging-component-configuration.xml -userId batch_user -requestPath RB99AC0110
 * 
 * </pre>
 * @author Kiyohito Itoh
 */
public class PutMessageForAsyncMessageReceive {

    private static final String QUEUE_MANAGER_NAME = "TEST";
    private static final String SEND_QUEUE_NAME = "TEST.REQUEST";
    private static final int SIZE = 5;
    private static final int LOOP = 1;
    private static final int WAIT = 5000;
    
    public static void main(String[] args) throws Exception {

        SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd_HH.mm.ss_SSS");
        char xChar = ' ';
        char nChar = '　';

        int sum = 0;

        for (int loop = 0; loop < LOOP; loop++) {
            for (int i = 0; i < SIZE; i++) {
    
                String date = df.format(new Date());
    
                StringBuilder message = new StringBuilder()
    
                    // ヘッダ部
                    .append(StringUtil.rpad("RM11AC0201", 20, xChar))   // リクエストID
                    .append(StringUtil.rpad("0", 1, xChar))             // 再送要求フラグ (0: 初回送信 1: 再送要求)
                    .append(StringUtil.rpad("", 8, xChar))              // ステータスコード
                    .append(StringUtil.rpad("batch_user", 10, xChar))   // 送信ユーザID
                    .append(StringUtil.rpad("", 11, xChar))             // 予備領域
                    .append("\r\n")
    
                    // データ部
                    .append(StringUtil.rpad("1234567890", 20, xChar))   // ログインID
                    .append(StringUtil.rpad("漢字氏名", 50, nChar))     // 漢字指名
                    .append(StringUtil.rpad("カナ氏名", 50, nChar))     // カナ指名
                    .append(StringUtil.rpad("", 50, xChar))             // 空白領域
                    .append(StringUtil.rpad(date + "@test.co.jp", 100, xChar)) // メールアドレス
                    .append(StringUtil.rpad("11", 2, xChar))            // 内線番号（ビル番号）
                    .append(StringUtil.rpad("2222", 4, xChar))          // 内線番号（個人番号）
                    .append(StringUtil.rpad("333", 3, xChar))           // 携帯番号（市外）
                    .append(StringUtil.rpad("4444", 4, xChar))          // 携帯電話番号(市内)
                    .append(StringUtil.rpad("5555", 4, xChar))          // 携帯電話番号(加入)
                    .append(StringUtil.rpad("", 33, xChar));            // 空白領域
    
                ToolUtil.putRequestMessage(QUEUE_MANAGER_NAME, SEND_QUEUE_NAME, message.toString(), null);
            }

            sum += SIZE;
            System.out.println("**************************************** sum[" + sum + "]");
            
            Thread.sleep(WAIT);
        }
    }
}
