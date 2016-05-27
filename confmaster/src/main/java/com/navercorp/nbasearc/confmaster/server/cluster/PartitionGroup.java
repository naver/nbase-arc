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

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jackson.type.TypeReference;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;

public class PartitionGroup extends ZNode<PartitionGroupData> {
    
    private String clusterName;
    private AtomicLong wfEpoch;
    
    public PartitionGroup(ApplicationContext context, String path, String name,
            String clusterName, byte[] data) {
        super(context);
        
        setTypeRef(new TypeReference<PartitionGroupData>(){});

        setPath(path);
        setName(name);
        setClusterName(clusterName);
        setNodeType(NodeType.PG);
        setData(data);
        
        wfEpoch = new AtomicLong();
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
    
    public List<PartitionGroupServer> getSlaves(List<PartitionGroupServer> pgsList) {
        List<PartitionGroupServer> slaveList = new ArrayList<PartitionGroupServer>(); 
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getData().getRole().equals(PGS_ROLE_SLAVE)
                    && pgs.getData().getColor() == GREEN) {
                slaveList.add(pgs);
            }
        }
        return slaveList;
    }
    
    public PartitionGroupServer getMaster(List<PartitionGroupServer> pgsList) {
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getData().getRole().equals(PGS_ROLE_MASTER)
                    && pgs.getData().getColor() == GREEN) {
                return pgs;
            }
        }
        return null;
    }

    public List<PartitionGroupServer> getJoinedPgsList(List<PartitionGroupServer> pgsList) {
        List<PartitionGroupServer> joinedPgsList = new ArrayList<PartitionGroupServer>();
        
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getData().getHb().equals(HB_MONITOR_YES)) {
                joinedPgsList.add(pgs);
            }
        }
        
        return joinedPgsList;
    }
    
    public List<PartitionGroupServer> getAlivePgsList(List<PartitionGroupServer> pgsList) {
        List<PartitionGroupServer> availablePgsList = new ArrayList<PartitionGroupServer>();
        
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getData().getHb().equals(HB_MONITOR_YES)
                    && (pgs.getData().getState().equals(SERVER_STATE_LCONN) 
                            || pgs.getData().getState().equals(SERVER_STATE_NORMAL))) {
                availablePgsList.add(pgs);
            }
        }
        
        return availablePgsList;
    } 

    public List<PartitionGroupServer> getAvailablePgsList(List<PartitionGroupServer> pgsList) {
        List<PartitionGroupServer> availablePgsList = new ArrayList<PartitionGroupServer>();
        
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getData().getHb().equals(HB_MONITOR_YES) && 
                    pgs.getData().getState().equals(SERVER_STATE_NORMAL)) {
                availablePgsList.add(pgs);
            }
        }
        
        return availablePgsList;
    }
    
    public List<String> getClosePgsIds(PartitionGroupServer master, 
            Map<String, LogSequence> logSeqMap) {
        /* 
         * The number of SMRs which are live and 
         * log sequences of them are not less than master`s log sequence - max-Logger-seq.
         */
        List<String> closePgsList = new ArrayList<String>();
        LogSequence masterLogSeq = logSeqMap.get(master.getName());
        Set<String> keys = logSeqMap.keySet();
        Iterator<String> iter = keys.iterator();
        
        while (iter.hasNext()) {
            String pgsid = iter.next();
            if (pgsid.equals(master.getName())) {
                continue;
            }
            
            LogSequence logSeq = logSeqMap.get(pgsid);
            if (logSeq == null) {
                continue;
            }
            
            long diff = masterLogSeq.getMax() - logSeq.getMax();
            
            if (diff < SETQUORUM_ADMITTABLE_RANGE) {
                closePgsList.add(pgsid);
            }
        }
        
        return closePgsList;
    }

    public int getD(List<PartitionGroupServer> pgsList) {
        int d = 0;
        for (PartitionGroupServer pgs : pgsList) {
            Color c = pgs.getData().getColor();
            if (c != GREEN && c != BLUE) {
                d++;
            }
        }
        
        return d;
    }
    
    public SortedLogSeqSet getLogSeq(List<PartitionGroupServer> pgsList)
            throws IOException {
        SortedLogSeqSet logSeqSet = new SortedLogSeqSet();

        for (PartitionGroupServer pgs : pgsList) {
            LogSequence logSeq = new LogSequence(pgs);
            logSeq.initialize();
            logSeqSet.add(logSeq);
        }

        return logSeqSet;
    }
    
    public boolean isMasterCandidate(PartitionGroupServer pgs,
            SortedLogSeqSet logSeqMap, List<PartitionGroupServer> pgsList) {
        final int index = logSeqMap.index(pgs);
        if (index == -1) {
            return true;
        }
        return index > getData().getQuorum() - getD(pgsList);
    }

    /**
     * @brief choose a master among candidates
     * @return return a master
     */
    public PartitionGroupServer chooseMasterRandomly(
            final SortedLogSeqSet logSeqMap,
            final List<PartitionGroupServer> joinedPgsList) {
        final int d = getD(joinedPgsList);
        final int r = new Random(System.currentTimeMillis()).nextInt(
                getData().getQuorum() - d + 1);
        return logSeqMap.get(r);
    }
    
    @Override
    public String toString() {
        return fullName(getClusterName(), getName());
    }
    
    public static String fullName(String clusterName, String pgId) {
        return clusterName + "/pg:" + pgId;
    }
    
    public long nextWfEpoch() {
        return wfEpoch.incrementAndGet();
    }
    
    public long getLastWfEpoch() {
        return wfEpoch.get();
    }
    
}
