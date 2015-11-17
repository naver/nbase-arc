package com.navercorp.nbasearc.confmaster.statistics;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.Test;

import com.navercorp.nbasearc.confmaster.logger.Log;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.statistics.Statistics;

public class StatisticsTest {

    @Test
    public void slowCommandLog() throws SecurityException,
            NoSuchFieldException, IllegalArgumentException,
            IllegalAccessException {
        Statistics.updateElapsedTimeForCommands("192.168.0.10", 10000, "ping", "pong", 2000, 1000);
        
        Field f = Logger.getLogHistory().getClass().getDeclaredField("logs");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<Log> logs = (List<Log>) f.get(Logger.getLogHistory());
        
        boolean find = false;
        for (Log log : logs) {
            int index = log.toString().indexOf(
                    "CC [SLOW_CLIENT] HOST:192.168.0.10:10000, " + 
                    "ELAPSED_REPLY_TIME:2000, CMD:ping, REPLY(LEN:4, MEG:\"pong\")");
            if (index != -1) {
                find = true;
                break;
            }
        }
        assertTrue("Slow command log not found.", find);
    }

}
