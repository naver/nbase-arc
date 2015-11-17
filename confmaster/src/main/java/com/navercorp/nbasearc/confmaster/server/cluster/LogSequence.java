package com.navercorp.nbasearc.confmaster.server.cluster;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.navercorp.nbasearc.confmaster.logger.Logger;

public class LogSequence {
    
    private long min;
    private long logCommit;
    private long max;
    private long beCommit;

    public LogSequence() {}
    
    public void initialize(PartitionGroupServer pgs) throws IOException {
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
                    new Object[]{pgs, pgs.getData().getRole(), reply});
        } catch (IOException e) {
            Logger.info(
                    "Get log sequence fail. {}, role: {}",
                    new Object[]{pgs, pgs.getData().getRole()});
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

}
