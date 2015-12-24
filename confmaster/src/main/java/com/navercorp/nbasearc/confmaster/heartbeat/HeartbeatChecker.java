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

package com.navercorp.nbasearc.confmaster.heartbeat;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.io.EventSelector;
import com.navercorp.nbasearc.confmaster.statistics.Statistics;

@Component
public class HeartbeatChecker {
    
    @Autowired
    private Config config;
    
    private EventSelector eventSelector;
    
    public HeartbeatChecker() {
    }
    
    public void initialize() throws IOException {
        eventSelector = new EventSelector(config.getHeartbeatNioSelectionTimeout());
    }
    
    public void process() {
        EventSelector.ElapsedTime elapsedTime = getEventSelector().process();
        Statistics.updateMaxNioLoopDuration(
                elapsedTime.getIoTime(), elapsedTime.getTotalTime());
    }
    
    public EventSelector getEventSelector() {
        return eventSelector;
    }

}
