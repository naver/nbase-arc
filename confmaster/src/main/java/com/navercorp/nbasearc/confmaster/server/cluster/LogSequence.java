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

package com.navercorp.nbasearc.confmaster.server.cluster;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.navercorp.nbasearc.confmaster.logger.Logger;

public class LogSequence {
    
    long min;
    long logCommit;
    long max;
    long beCommit;
    final PartitionGroupServer pgs;

    public LogSequence(PartitionGroupServer pgs) {
        this.pgs = pgs;
    }
    
    public void initialize() throws IOException {
        try {
            String reply = pgs.executeQuery("getseq log");
            Map<String, String> map = new LinkedHashMap<String, String>();
            
            for (String pair : reply.split(" ")) {
                String kv[] = pair.split(":");
                if (kv.length != 2) {
                    continue;
                }
                map.put(kv[0], kv[1]);
            }

            String be_sent = map.get("be_sent");
            if (be_sent == null) {
                be_sent = "0";
            }

            this.min = Long.parseLong(map.get("min"));
            this.logCommit = Long.parseLong(map.get("commit"));
            this.max = Long.parseLong(map.get("max"));
            this.beCommit = Long.parseLong(be_sent);

            Logger.info(
                    "Get log sequence success. {}, role: {}, reply: \"{}\"",
                    new Object[]{pgs, pgs.getRole(), reply});
        } catch (IOException e) {
            Logger.info(
                    "Get log sequence fail. {}, role: {}",
                    new Object[]{pgs, pgs.getRole()});
            throw e;
        }
    }

    public long getMin() {
        return min;
    }
    
    public long getLogCommit() {
        return logCommit;
    }

    public long getBeCommit() {
        return beCommit;
    }
    
    public long getMax() {
        return max;
    }
    
    @Override
    public String toString() {
        return "min:" + getMin() + " commit:" + getLogCommit() + " max:"
                + getMax() + " be_sent:" + getBeCommit();
    }

    public PartitionGroupServer getPgs() {
        return pgs;
    }

}
