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

import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.io.Session;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;

public class Statistics {
    
    private static long lastStatTime = System.currentTimeMillis();

    /* Network IO Loop */
    private static long maxNioLoopDuration = 0L;
    private static long nioRead = 0L;
    private static long nioWrite = 0L;

    /* Heartbeat */    
    // The maximum elapsed time for heartbeats that be done until now.
    // In contrast with maxElapsedTimeForHeartbeat, 
    // maxPingpongDuration is not set with 0 at interval for logging statistic.
    private static long maxPingpongDuration = 0L;   
    private static long totalElapsedTimeForHeartbeat = 0L;
    private static long countOfHeartbeat = 0L;
    private static long maxElapsedTimeForHeartbeat = -1L;
    private static long minElapsedTimeForHeartbeat = -1L;
    
    /* Workflow */
    private static long totalElapsedTimeForWorkflow = -1L;
    private static long countOfWorkflow = 0L;
    private static long minElapsedTimeForWorkflow = -1L;
    private static long maxElapsedTimeForWorkflow = -1L;
    
    /* Command */
    private static long totalTimeOfReplyToClient = 0L;
    private static long countOfCommands = 0L;    
    private static long minElapsedTimeForCommand = -1L;
    private static long maxElapsedTimeForCommand = -1L;
    
    /* Job */
    private static long jobCompleted = 0L;
    private static long jobWaiting = 0L;
    
    private static long SLOW_HEARTBEAT;
    private static long STAT_INTERVAL;
    private static ThreadPool jobExecutor; 

    public static void initialize(Config config, ThreadPool executor) {
        SLOW_HEARTBEAT = config.getHeartbeatNioSlowloop();
        STAT_INTERVAL = config.getStatisticsInterval();
        jobExecutor = executor;
    }
    
    public static long getMaxPingpongDuration() {
        return maxPingpongDuration;
    }
    
    public static void updateMaxPingpongDuration(long pingpongDuration,
            HeartbeatTarget target, Session session, long slowHeartbeat) {
        if (Statistics.maxPingpongDuration < pingpongDuration) {
            Logger.warn("Slow pingpong. latency: {}, target: {}, session: {} (max updated)", 
                    new Object[]{pingpongDuration, target, session});
            Statistics.maxPingpongDuration = pingpongDuration;
        }

        if (slowHeartbeat < pingpongDuration) {
            Logger.warn("Slow pingpong. latency: {}, target: {}, session: {} ", 
                    new Object[]{pingpongDuration, target, session});
        }
        
        addPingCount(pingpongDuration);
    }

    public static long getMaxNioLoopDuration() {
        return maxNioLoopDuration;
    }
    
    public static void updateNioRead(long duration) {
        nioRead += duration;
    }
    
    public static void updateNioWrite(long duration) {
        nioWrite += duration;
    }

    public static void updateMaxNioLoopDuration(long ioDuration, long loopDuration) {
        updateJobStat();
        
        if (Statistics.maxNioLoopDuration < loopDuration) {
            Logger.warn("Slow nio process. loop: {}, io: {}(read: {}, write: {})",
                    new Object[]{loopDuration, ioDuration, nioRead, nioWrite});
            Statistics.maxNioLoopDuration = loopDuration;
        }
        
        if (SLOW_HEARTBEAT < loopDuration) {
            Logger.warn("Slow nio process. loop: {}, io: {}(read: {}, write: {})",
                    new Object[]{loopDuration, ioDuration, nioRead, nioWrite});
        }
        
        nioRead = 0L;
        nioWrite = 0L;

        final long currentTime = System.currentTimeMillis();
        if (STAT_INTERVAL < currentTime - lastStatTime) {
            lastStatTime = currentTime;
            statistics();
        }
    }

    public static void updateElapsedTimeForWorkflows(long elapsedTime) {
        totalElapsedTimeForWorkflow += elapsedTime;
        countOfWorkflow++;
        
        if (minElapsedTimeForWorkflow == -1L) {
            minElapsedTimeForWorkflow = elapsedTime;
        } else if (minElapsedTimeForWorkflow < elapsedTime) {
            minElapsedTimeForWorkflow = elapsedTime;
        }
        
        if (maxElapsedTimeForWorkflow < elapsedTime) {
            maxElapsedTimeForWorkflow = elapsedTime;
        }
    }
    
    public static void updateElapsedTimeForCommands(String ip, int port,
            String request, String reply, long elapsedTime, long timeout) {
        totalTimeOfReplyToClient += elapsedTime;
        countOfCommands++;

        if (minElapsedTimeForCommand == -1L) {
            minElapsedTimeForCommand = elapsedTime;
        } else if (minElapsedTimeForCommand > elapsedTime) {
            minElapsedTimeForCommand = elapsedTime;
        }
        
        if (maxElapsedTimeForCommand < elapsedTime) {
            maxElapsedTimeForCommand = elapsedTime;
        }
        
        if (elapsedTime > timeout) {
            String shortReply;
            if (reply.length() > 200) {
                shortReply = reply.substring(0, 200) + "...";
            } else {
                shortReply = reply; 
            }
            
            Logger.info(
                    "Slow cmd. {}:{}, elapsed: {}ms, request: \"{}\", reply: \"{}\"",
                    new Object[] { ip, port, elapsedTime, request, shortReply });
        }
    }
    
    private static void updateJobStat() {
        jobCompleted = jobExecutor.getCompletedTaskCount();
        jobWaiting = jobExecutor.getQSize();
    }
    
    public static void addPingCount(long elapsedTime) {
        totalElapsedTimeForHeartbeat += elapsedTime;
        countOfHeartbeat ++;
        
        if (minElapsedTimeForHeartbeat == -1L) {
            minElapsedTimeForHeartbeat = elapsedTime;
        } else if (minElapsedTimeForHeartbeat > elapsedTime) {
            minElapsedTimeForHeartbeat = elapsedTime;
        }

        if (maxElapsedTimeForHeartbeat < elapsedTime) {
            maxElapsedTimeForHeartbeat = elapsedTime;
        }
    }
    
    public static void statistics() {        
        Logger.info("HB(CNT:"
                + countOfHeartbeat
                + ", MIN:"
                + minElapsedTimeForHeartbeat
                + ", MAX:"
                + maxElapsedTimeForHeartbeat
                + ", AVG:"
                + (totalElapsedTimeForHeartbeat / (countOfHeartbeat == 0 ? 1 : countOfHeartbeat))
                + "), WF(CNT:"
                + countOfWorkflow
                + ", MIN:"
                + minElapsedTimeForWorkflow
                + ", MAX:" 
                + maxElapsedTimeForWorkflow
                + ", AVG:"
                + (totalElapsedTimeForWorkflow / (countOfWorkflow == 0 ? 1
                        : countOfWorkflow))
                + ", RUN:" 
                + jobCompleted
                + ", WAIT:" 
                + jobWaiting
                + "), CMD(CNT:" 
                + countOfCommands
                + ", MIN:"
                + minElapsedTimeForCommand
                + ", MAX:"
                + maxElapsedTimeForCommand
                + ", AVG:"
                + (totalTimeOfReplyToClient / (countOfCommands == 0 ? 1
                        : countOfCommands)) + ")");
        
        countOfHeartbeat = 0L;
        totalElapsedTimeForHeartbeat = 0L;
        maxElapsedTimeForHeartbeat = -1L;
        minElapsedTimeForHeartbeat = -1L;
        
        countOfWorkflow = 0L;
        totalElapsedTimeForWorkflow = 0L;
        minElapsedTimeForWorkflow = -1L;
        maxElapsedTimeForWorkflow = -1L;
        
        totalTimeOfReplyToClient = 0L;
        countOfCommands = 0L;
        minElapsedTimeForCommand = -1L;
        maxElapsedTimeForCommand = -1L;
    }

}
