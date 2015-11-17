package com.navercorp.nbasearc.confmaster.server.cluster;

import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_YES;
import static com.navercorp.nbasearc.confmaster.Constant.LOG_TYPE_WORKFLOW;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_LCONN;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_NORMAL;
import static com.navercorp.nbasearc.confmaster.Constant.SETQUORUM_ADMITTABLE_RANGE;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_CRITICAL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.codehaus.jackson.type.TypeReference;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;

public class PartitionGroup extends ZNode<PartitionGroupData> {
    
    private String clusterName;
    
    public PartitionGroup(ApplicationContext context, String path, String name,
            String clusterName, byte[] data) {
        super(context);
        
        setTypeRef(new TypeReference<PartitionGroupData>(){});

        setPath(path);
        setName(name);
        setClusterName(clusterName);
        setNodeType(NodeType.PG);
        setData(data);
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
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
    
    public boolean checkJoinConstraintMaster(PartitionGroupServer pgs,
            LogSequence targetLogSeq, long jobID, WorkflowLogDao workflowLogDao) {
        long currentGenCommit = getData().currentSeq();
        return targetLogSeq.getLogCommit() >= currentGenCommit;
    }
    
    public class SlaveJoinInfo {
        private final boolean success;
        private final Long joinSeq;
        
        public SlaveJoinInfo(boolean success, Long joinSeq) {
            this.success = success;
            this.joinSeq = joinSeq;
        }

        public boolean isSuccess() {
            return success;
        }

        public Long getJoinSeq() {
            return joinSeq;
        }
    }
    
    public SlaveJoinInfo checkJoinConstraintSlave(PartitionGroupServer pgs,
            LogSequence targetLogSeq, long jobID, WorkflowLogDao workflowLogDao) {
        int pgsMasterGen = pgs.getData().getMasterGen();
        int currentGen = getData().currentGen();
        long currentGenCommit = getData().currentSeq();
        
        String worklogArgs = "pgsID=" + pgs.getName() + 
                        ", pgsMasterGen=" + pgsMasterGen + 
                        ", currentGen=" + currentGen + 
                        ", pgsCommit=" + targetLogSeq.getLogCommit() +
                        ", currentGenCommit=" + currentGenCommit;
         
        if (pgsMasterGen > currentGen) {
            workflowLogDao.log(jobID,
                    SEVERITY_CRITICAL, "RoleSlave",
                    LOG_TYPE_WORKFLOW, pgs.getClusterName(),
                    "PGS has an invalid masterGen.", worklogArgs);
            
            Logger.error(
                    "PGS has an invalid masterGen. {}, mgen: pg({}) pgs({}), cseq: pg({}) pgs({})",
                    new Object[] { pgs, currentGen, pgsMasterGen,
                            currentGenCommit, targetLogSeq.getLogCommit() });
            return new SlaveJoinInfo(false, 0L);
        } else if (pgsMasterGen == currentGen) {
            Logger.debug(
                    "PGS has a valid masterGen. {}, mgen: pg({}) pgs({}), cseq: pg({}) pgs({})",
                    new Object[] { pgs, currentGen, pgsMasterGen,
                            currentGenCommit, targetLogSeq.getLogCommit() });
            return new SlaveJoinInfo(true, targetLogSeq.getMax());
        } else {
            long commitSeqOfPg = getData().commitSeq(pgsMasterGen); 
            if (targetLogSeq.getLogCommit() <= commitSeqOfPg) {
                Logger.debug(
                        "PGS has a valid masterGen. {}, mgen: pg({}) pgs({}), cseq: cur({}) pg({}) pgs({})",
                        new Object[] { pgs, currentGen, pgsMasterGen,
                                currentGenCommit, commitSeqOfPg, targetLogSeq.getLogCommit() });
                return new SlaveJoinInfo(true, 
                        Math.min(targetLogSeq.getMax(), commitSeqOfPg));
            } else {                
                worklogArgs = "{\"PGS_ID\":" + pgs.getName() + 
                                ",\"PGS_MGEN\":" + pgsMasterGen + 
                                ",\"PG_CURRENT_MGEN\":" + currentGen + 
                                ",\"PGS_COMMIT\":" + targetLogSeq.getLogCommit() +
                                ",\"MGEN_COMMIT\":" + commitSeqOfPg + "}";

                workflowLogDao.log(jobID,
                        SEVERITY_CRITICAL, "RoleSlave",
                        LOG_TYPE_WORKFLOW, pgs.getClusterName(),
                        "PGS has an invalid masterGen.", worklogArgs);

                Logger.error(
                        "PGS has an invalid masterGen. {}, mgen: pg({}) pgs({}), cseq: pg({}) pgs({})",
                        new Object[] { pgs, currentGen, pgsMasterGen,
                                commitSeqOfPg, targetLogSeq.getLogCommit() });
                return new SlaveJoinInfo(false, 0L);
            }
        }
    }

    /**
     * @brief get Logger sequneces of all PGSes in a PG
     * @return return a map of Logger sequences, if error then return null
     */
    public Map<String, LogSequence> getLogSeqOfPG(List<PartitionGroupServer> pgsList) {
        Map<String, LogSequence> logSeqMap = new LinkedHashMap<String, LogSequence>();
        
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getData().getState().equals(SERVER_STATE_LCONN) ||
                    pgs.getData().getState().equals(SERVER_STATE_NORMAL)) {
                LogSequence logSeq = new LogSequence();
                try {
                    logSeq.initialize(pgs);
                    logSeqMap.put(pgs.getName(), logSeq);
                } catch (IOException e) {
                    logSeqMap.put(pgs.getName(), null);
                }
            }
        }
        
        return logSeqMap;
    }

    /**
     * @brief choose a name of pgs among candidates for the master
     * @return return a name of pgs
     */
    public String chooseMasterRandomly(
            final Map<String, LogSequence> logSeqMap, final Long maxSeq) {
        List<String> maxLogSeqList = new ArrayList<String>();
        for (Map.Entry<String, LogSequence> logSeq : logSeqMap.entrySet()) {
            if (logSeq.getValue().getMax() == maxSeq) {
                maxLogSeqList.add(logSeq.getKey());
            }
        }
        
        Collections.sort(maxLogSeqList);
        String nameOfMaster = null;
        final Integer sizeOfList = maxLogSeqList.size();
        if (sizeOfList > 1) {
            Random rand = new Random();
            nameOfMaster = maxLogSeqList.get(rand.nextInt(sizeOfList));
            return nameOfMaster;
        } else if (sizeOfList == 1) {
            return maxLogSeqList.get(0);
        }
        
        return "";
    }

    /*
     * @return Returns true if successful or false otherwise.
     */
    public boolean checkCopyQuorum(final int alive) {
        final boolean success = alive >= (getData().getCopy() - getData().getQuorum());
        if (success) {
            Logger.info(
                    "Check copy-quorum success. available: {}, pg(copy: {}, quorum: {})",
                    new Object[] { alive, getData().getCopy(),
                            getData().getQuorum() });
        } else {
            Logger.error(
                    "Check copy-quorum fail. available: {}, pg(copy: {}, quorum: {})",
                    new Object[] { alive, getData().getCopy(),
                            getData().getQuorum() });
        }

        return success;
    }
    
    @Override
    public String toString() {
        return fullName(getClusterName(), getName());
    }
    
    public static String fullName(String clusterName, String pgId) {
        return clusterName + "/pg:" + pgId;
    }
    
}
