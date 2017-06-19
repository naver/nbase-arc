package com.navercorp.nbasearc.gcp;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.navercorp.nbasearc.gcp.HashedTimingWheel;
import com.navercorp.nbasearc.gcp.TimerCallback;

public class HashedTimingWheelTest {

    HashedTimingWheel htw = new HashedTimingWheel(500, 1024);
    
    @Test
    public void timeout() {
        TimedRequest tc = new TimedRequest(100);
        htw.add(tc);
        htw.processLines(99);
        assertFalse(tc.timeoutDone);
        htw.processLines(100);
        assertFalse(tc.timeoutDone);
        htw.processLines(101);
        assertFalse(tc.timeoutDone);
        
        htw.processLines(102);
        assertTrue(tc.timeoutDone);
    }
    
    @Test
    public void noTimeout() {
        TimedRequest tc = new TimedRequest(200);
        htw.add(tc);
        htw.processLines(99);
        assertFalse(tc.timeoutDone);
        htw.processLines(102);
        assertFalse(tc.timeoutDone);
        
        htw.processLines(202);
        assertTrue(tc.timeoutDone);
    }

    @Test
    public void noTimeoutDeleted() {
        TimedRequest tc = new TimedRequest(100);
        htw.add(tc);
        htw.del(tc);
        htw.processLines(102);
        assertFalse(tc.timeoutDone);
    }
    
    @Test
    public void del() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException { 
        HashedTimingWheel htw = new HashedTimingWheel(500, 1024);
        
        List<TimedRequest> toDel = new ArrayList<TimedRequest>();
        for (int i = 0; i < 100000; i++) {
            TimedRequest tc = new TimedRequest(i);
            htw.add(tc);
            if (i % 10 == 0) {
                toDel.add(tc);
            }
        }

        for (TimedRequest tc : toDel) {
            htw.del(tc);
        }
        
        for (Set<TimerCallback> s : getWheel(htw)) {
            for (TimerCallback tc : s) {
                TimedRequest r = (TimedRequest) tc;
                
                assertFalse(r.ts % 10 == 0);
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private List<Set<TimerCallback>> getWheel(HashedTimingWheel htw)
            throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field f = htw.getClass().getDeclaredField("wheel");
        f.setAccessible(true);
        return (List<Set<TimerCallback>>) f.get(htw);
    }
    
    class TimedRequest implements TimerCallback{
        long ts;
        boolean timeoutDone = false;
        
        public TimedRequest(long ts) {
            this.ts = ts;
        }
        
        @Override
        public long getTimerTimestamp() {
            return ts;
        }

        @Override
        public void onTimer() {
            timeoutDone = true;
        }
    }
    
}
