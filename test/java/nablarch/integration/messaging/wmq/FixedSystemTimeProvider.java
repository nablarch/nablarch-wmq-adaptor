package nablarch.integration.messaging.wmq;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import nablarch.core.date.SystemTimeProvider;

public class FixedSystemTimeProvider implements SystemTimeProvider {
    public static final Date FIXED_DATE;
    static {
        try {
            FIXED_DATE = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS").parse("2011/11/29 16:49:23.578");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
    public Date getDate() {
        return FIXED_DATE;
    }
    public Timestamp getTimestamp() {
        return new Timestamp(FIXED_DATE.getTime());
    }
}
