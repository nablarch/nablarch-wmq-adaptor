package nablarch.integration.messaging.wmq.provider;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.XADataSource;
import javax.transaction.xa.XAException;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;

import nablarch.fw.messaging.MessagingException;
import nablarch.fw.messaging.SendingMessage;

import org.junit.Test;

public class WmqMessagingContextTest {

    /**
     * キューが存在しない場合は例外が送出されること。
     */
    @Test
    public void testGetMQQueue() throws MQException {

        WmqMessagingContext context = new WmqMessagingContext(
                null, null, new HashMap<String, MQQueue>(), null, null);

        SendingMessage sendingMessage = new SendingMessage();
        sendingMessage.setDestination("TEST");
        try {
            context.send(sendingMessage);
            fail("MessagingException");
        } catch (MessagingException e) {
            assertThat(e.getMessage(), is("queue was not found. queueName = [TEST]"));
        }

        try {
            context.receiveMessage("TEST", null, 100);
            fail("MessagingException");
        } catch (MessagingException e) {
            assertThat(e.getMessage(), is("queue was not found. queueName = [TEST]"));
        }
    }

    /**
     * デリゲートされること。
     */
    @Test
    public void testDelegate() throws Exception {

        MockWmqMessagingProvider mockProvider = new MockWmqMessagingProvider();
        WmqMessagingContext context = new WmqMessagingContext(
                mockProvider, null, new HashMap<String, MQQueue>(), null, null);

        int expectedCount = 0;

        context.getJdbcConnection(null);
        ++expectedCount;

        context.begin();
        ++expectedCount;

        context.commit();
        ++expectedCount;

        context.backout();
        ++expectedCount;

        context.close();
        expectedCount += 4; // MQQueueManager/Map<String,MQQueue>(sending)/MQQueue(received)/MQQueue(poison)

        assertThat(mockProvider.count, is(expectedCount));
    }

    private static final class MockWmqMessagingProvider extends WmqMessagingProvider {
        protected int count = 0;
        @Override
        protected void disconnect(MQQueueManager mqQueueManager) {
            ++count;
        }
        @Override
        protected void close(Map<String, MQQueue> mqQueues) {
            ++count;
        }
        @Override
        protected void close(MQQueue mqQueue) {
            ++count;
        }
        @Override
        protected Connection getJdbcConnection(MQQueueManager mqQueueManager,
                XADataSource xaDataSource) throws SQLException, XAException {
            ++count;
            return null;
        }
        @Override
        protected void begin(MQQueueManager mqQueueManager) {
            ++count;
        }
        @Override
        protected void commit(MQQueueManager mqQueueManager) {
            ++count;
        }
        @Override
        protected void backout(MQQueueManager mqQueueManager) {
            ++count;
        }
    }
}
