package nablarch.integration.messaging.wmq.tool;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;

import nablarch.core.util.StringUtil;
import nablarch.test.support.tool.HereisDb;

/**
 * 応答不要メッセージ送信サンプルの打鍵テスト用にデータを挿入する。
 * <pre>
 * 応答不要メッセージ送信サンプルの起動方法
 * 
 * 起動クラス
 *     {@link nablarch.fw.launcher.Main}
 * JVM引数
 *     -diConfig classpath:send-messaging-component-configuration.xml -requestPath RB99AC0120 -userId batch_user -messageRequestId RM11AC0301
 * 
 * </pre>
 * @author Kiyohito Itoh
 */
public class InsertDataForAyncMessageSend {

    private static final int SIZE = 5;
    private static final int LOOP = 1;
    private static final int WAIT = 5000;

    public static void main(String[] args) throws Exception {
        int sum = 0;
        for (int loop = 0; loop < LOOP; loop++) {
            Connection con = null;
            try {
    
                con = ToolUtil.getConnection();
    
                int count = Integer.valueOf(HereisDb.query(con).get(0).get("RS_COUNT").toString());
                /*
                SELECT COUNT(*) AS RS_COUNT FROM DELETE_USER_SEND_MESSAGE
                */
    
                SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                SimpleDateFormat exeIdFmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    
                for (int i = 0; i < SIZE; i++) {
    
                    Date now = new Date();
    
                    String sendMessageSequence = StringUtil.lpad(String.valueOf(++count), 10, '0');
                    String userId = StringUtil.lpad(String.valueOf(i + 1), 10, '0');
                    String kanjiName = "漢字氏名" + (i + 1);
                    String kanaName = "カナメイ" + (i + 1);
                    String status = "0";
                    String insertDate = dateFmt.format(now);
                    String insertRequestId = "RB11AC0110";
                    String insertExecutionId = "EXECUTIONID_" + exeIdFmt.format(now);
    
                    HereisDb.sql(con, sendMessageSequence, userId, kanjiName, kanaName, status, insertDate, insertRequestId, insertExecutionId);
                    /*
                    INSERT INTO DELETE_USER_SEND_MESSAGE VALUES ('${sendMessageSequence}','${userId}','${kanjiName}','${kanaName}','${status}','${userId}',to_timestamp('${insertDate}','YYYY-MM-DD HH24:MI:SS.FF'),'${insertRequestId}','${insertExecutionId}','${userId}',to_timestamp('${insertDate}','YYYY-MM-DD HH24:MI:SS.FF'))
                    */
                }
    
                con.commit();

                sum += SIZE;
    
                System.out.println("**************************************** sum[" + sum + "]");
    
            } finally {
                ToolUtil.close(con);
            }
            Thread.sleep(WAIT);
        }
    }
}
