package nablarch.integration.messaging.wmq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nablarch.core.log.basic.LogWriterSupport;

public class OnMemoryLogWriter extends LogWriterSupport {

    public static final Map<String, List<String>> LOGS = new HashMap<String, List<String>>();

    @Override
    protected void onWrite(String formattedMessage) {
        if (!LOGS.containsKey(getName())) {
            LOGS.put(getName(), new ArrayList<String>());
        }
        LOGS.get(getName()).add(formattedMessage);
    }
}
