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
