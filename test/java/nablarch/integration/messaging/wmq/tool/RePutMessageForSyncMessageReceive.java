package nablarch.integration.messaging.wmq.tool;


/**
 * 同期応答メッセージ受信サンプルの打鍵テスト用に再送メッセージを書き込む。
 * <pre>
 * 再送なしメッセージのテストは、{@link PutMessageForSyncMessageReceive}を使用する。
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
public class RePutMessageForSyncMessageReceive {

    private static final String QUEUE_MANAGER_NAME = "TEST";
    private static final String SEND_QUEUE_NAME = "TEST.REQUEST";
    private static final String REPLY_TO_QUEUE_NAME = "TEST.RESPONSE";
    private static final int SIZE = 5;

    public static void main(String[] args) throws Exception {

        byte[][] messageIds = new byte[SIZE][];

        for (int i = 0; i < SIZE; i++) {

            String message = PutMessageForSyncMessageReceive.createBodyDataForSyncMessageReceive();

            messageIds[i] = ToolUtil.putRequestMessage(QUEUE_MANAGER_NAME, SEND_QUEUE_NAME,
                                                       message, REPLY_TO_QUEUE_NAME);
        }

        System.out.println("20秒以内に応答キューの書き込み禁止を解除してください！！！");
        Thread.sleep(20000); // 10sec

        for (byte[] messageId : messageIds) {

            String message = PutMessageForSyncMessageReceive.createBodyDataForSyncMessageReceive();

            ToolUtil.rePutRequestMessage(QUEUE_MANAGER_NAME, SEND_QUEUE_NAME,
                                         message, REPLY_TO_QUEUE_NAME, messageId);
        }
    }
}
