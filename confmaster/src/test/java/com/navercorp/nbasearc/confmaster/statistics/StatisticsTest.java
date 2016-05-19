/*
 * Copyright 2015 Naver Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.nbasearc.confmaster.statistics;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Log;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.statistics.Statistics;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-nozk.xml")
public class StatisticsTest {

    @Autowired
    Config config;
    
    @Autowired
    ThreadPool executor;
    
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
                    "CC Slow cmd. 192.168.0.10:10000, elapsed: 2000ms, request: \"ping\", reply: \"pong\"");
            if (index != -1) {
                find = true;
                break;
            }
        }
        assertTrue("Slow command log not found.", find);
    }

}
