package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.LOG_TYPE_WORKFLOW;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_MAJOR;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.SET_QUORUM;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtDuplicatedReservedCallException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.context.Context;
import com.navercorp.nbasearc.confmaster.context.ContextType;
import com.navercorp.nbasearc.confmaster.context.ExecutionContextPar;
import com.navercorp.nbasearc.confmaster.io.RedisReplPing;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PgDataBuilder;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;

public class SetquorumWorkflow {
    
    private String clusterName;
    private String pgid;
    
    private final long jobID = JobIDGenerator.getInstance().getID();

    private final ClusterImo clusterImo;
    private final PartitionGroupServerImo pgsImo;
    private final PartitionGroupImo pgImo;
    private final RedisServerImo rsImo;
    
    private final PartitionGroupDao pgDao;
    private final WorkflowLogDao workflowLogDao; 
    
    private final WorkflowExecutor workflowExecutor;

    private final ThreadPool executor;
    
    private final Config config;
    
    protected SetquorumWorkflow(String clusterName, String pgid, ApplicationContext context) {
        this.clusterName = clusterName;
        this.pgid = pgid;
        
        this.clusterImo = context.getBean(ClusterImo.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        this.pgImo = context.getBean(PartitionGroupImo.class);
        this.rsImo = context.getBean(RedisServerImo.class);
        
        this.pgDao = context.getBean(PartitionGroupDao.class);
        this.workflowLogDao = context.getBean(WorkflowLogDao.class);
        
        this.workflowExecutor = context.getBean(WorkflowExecutor.class);
        
        this.executor = context.getBean(ThreadPool.class);
        
        this.config = context.getBean(Config.class);
    }
    
    public String execute(ThreadPool executor) throws MgmtDuplicatedReservedCallException {
        // Get cluster, pg, pgs
        Cluster cluster = clusterImo.get(
                clusterName);
        if (null == cluster) {
            Logger.error("Cluster does not exist. {}", clusterName);
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }
        
        PartitionGroup pg = pgImo.get(pgid, clusterName);
        if (null == pg) {
            Logger.error("Pg does not exist. {}", pgid);
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                            + PartitionGroup.fullName(clusterName, pgid));
        }
        
        int livePgsCount = 0;
        PartitionGroupServer master = null;
        Map<String, LogSequence> logSeqMap = new LinkedHashMap<String, LogSequence>();
        
        List<PartitionGroupServer> joinedPgsList = 
                pg.getJoinedPgsList(pgsImo.getList(clusterName, Integer.parseInt(pgid)));
        
        Logger.info("Setquorum Workflow begin. cluster:{}/pg:{}", clusterName, pgid);
        
        // Check master and get count of live pgs
        for (PartitionGroupServer pgs : joinedPgsList) {
            if (!pgs.getData().getState().equals(Constant.SERVER_STATE_NORMAL)) {
                continue;
            }
            livePgsCount++;

            LogSequence logSeq = new LogSequence();
            try {
                logSeq.initialize(pgs);
                logSeqMap.put(pgs.getName(), logSeq);
            } catch (IOException e) {
                Logger.warn("Get log sequence fail. {}", pgs, e);
            }

            if (pgs.getData().getRole().equals(Constant.PGS_ROLE_MASTER)) {
                master = pgs;
            }
        }

        if (null == master) {
            Logger.error("There is no master.");
            return null;
        }

        LogSequence masterLogSeq = logSeqMap.get(master.getName());
        if (null == masterLogSeq) {
            Logger.error("Get log sequence of master fail. {}", master);
            return null;
        }

        // Get quorum peer list.
        List<PartitionGroupServer> quorumPeerList = new ArrayList<PartitionGroupServer>();
        for (String pgsId : pg.getClosePgsIds(master, logSeqMap)) {
            quorumPeerList.add(pgsImo.get(pgsId, cluster.getName()));
        }
        
        final Integer quorum = cluster.getQuorum(quorumPeerList.size());
        final Integer copy = pg.getData().getCopy();
        if (pg.getData().getQuorum() != quorum) {
            // Get quorum for rollback (uses it when failed.)
            Integer rollbackQuorum = getQuorum(master);
            if (rollbackQuorum == -1) {
                rerun(executor);
                return null;
            }
            
            // Try to modify quorum
            try {
                setQuorum(pg, master, quorum, copy);
            } catch (MgmtZooKeeperException e) {
                rerun(executor);
                return null;
            }
            
            if (!checkQuorumCommit(master, quorumPeerList, cluster.getName())) {
                // Rollback quorum
                Logger.error("Rollback. {}, quorum: {}", master, rollbackQuorum);
                try {
                    setQuorum(pg, master, rollbackQuorum, copy);
                } catch (MgmtZooKeeperException e) {
                    return null;
                }

                rerun(executor);
                return null;
            }
        }
        
        final Integer safeQuorum = cluster.getQuorum(livePgsCount);
        if (safeQuorum != quorum) {
            rerun(executor);
        }

        return null;
    }
    
    /**
     * @return Returns an integer larger than or equal to 0 if successful or -1 otherwise. 
     */
    private Integer getQuorum(PartitionGroupServer master) {
        String command = "getquorum";
        try {
            String reply = master.executeQuery(command);
            try {
                return Integer.parseInt(reply); 
            } catch (NumberFormatException e) {
                Logger.error("Get quorum fail. {}, command: \"{}\", reply: \"{}\"",
                        new Object[]{master, command, reply}, e);
                return -1;
            }
        } catch (IOException e) {
            Logger.error("Get quorum fail. {}, command: \"{}\"",
                    new Object[]{master, command}, e);
            return -1;
        }
    }
    
    private boolean setQuorum(PartitionGroup pg, PartitionGroupServer master,
            Integer quorum, Integer copy) throws MgmtZooKeeperException {
        // Update database
        PartitionGroupData pgModified = 
            new PgDataBuilder().from(
                pg.getData()).withQuorum(quorum).build();

        try {
            pgDao.updatePg(pg.getPath(), pgModified);
        } catch (MgmtZooKeeperException e) {
            Logger.error("Update pg fail. {}, {}", pg, pgModified);
            throw e;
        }
        
        pg.setData(pgModified);
        
        // Set quorum
        String command = "setquorum " + quorum;
        try {
            String reply = master.executeQuery(command);
            
            if (reply.equals(Constant.PGS_RESPONSE_OK)) {
                Logger.info("Setquorum success. {}, quorumPeer: {}, command: \"{}\", reply: \"{}\"",
                        new Object[]{master, copy, command, reply});
                workflowLog(master, quorum, true, "setquorum succeeded", workflowLogDao);
            } else {
                Logger.error("Setquorum fail. {}, quorumPeer: {}, command: \"{}\", reply: \"{}\"",
                        new Object[]{master, copy, command, reply});
                workflowLog(master, quorum, false,
                        String.format("setquorum fail. reply=%s", reply), workflowLogDao);
                
            }
        } catch (IOException e) {
            Logger.error("Setquorum fail. {}, quorumPeer: {}, command: \"{}\"",
                    new Object[]{master, copy, command}, e);
            workflowLog(master, quorum, false,
                    String.format("setquorum fail. exception=%s",
                            e.getMessage()), workflowLogDao);
            return false;
        }
        
        return true;
    }
    
    /**
     * Send replicated ping to all joined PGS in order to check log catch up.
     */
    private boolean checkQuorumCommit(PartitionGroupServer master,
            List<PartitionGroupServer> quorumPeerList, String clusterName) {
        List<Future<RedisReplPing.Result>> futureTasks = 
                new ArrayList<Future<RedisReplPing.Result>>();
        
        // Check ping replication
        for (PartitionGroupServer pgs : quorumPeerList) {
            RedisReplPing job = new RedisReplPing(clusterName, pgs, 
                    rsImo.get(pgs.getName(), clusterName), config);

            Context<RedisReplPing.Result> context = 
                new ExecutionContextPar<RedisReplPing.Result>(
                    job, ContextType.RS, Logger.getLogHistory());
            
            Future<RedisReplPing.Result> future = executor.perform(context);

            futureTasks.add(future);
        }
        
        boolean success = true;
        List<RedisReplPing.Result> results = new ArrayList<RedisReplPing.Result>();
        for (Future<RedisReplPing.Result> future : futureTasks) {
            try {
                results.add(future.get(1000, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                Logger.error("-ERR failed to get result from SMRRoleGetter", e);
                success = false;
            } catch (ExecutionException e) {
                Logger.error("-ERR failed to get result from SMRRoleGetter", e);
                success = false;
            } catch (TimeoutException e) {
                Logger.error("-ERR failed to get result from SMRRoleGetter", e);
                success = false;
            }
        }
        
        for (RedisReplPing.Result result : results) {
            if (!result.isSuccess()) {
                success = false;
            }
        }
        
        return success;
    }

    private void workflowLog(PartitionGroupServer master, int quorumPolicy,
            boolean success, String msg, WorkflowLogDao workflowLogDao) {
        String arguments = String.format(
                "{\"quorum_policy\":\"%d\",\"pgid\":%d,\"pgsid\":%s}",
                quorumPolicy, master.getData().getPgId(), master.getName());
        if (success) {
            workflowLogDao.log(jobID,
                    Constant.SEVERITY_MODERATE, "SetquorumWorkflow",
                    Constant.LOG_TYPE_WORKFLOW, master.getClusterName(), msg,
                    arguments);
        } else {
            workflowLogDao.log(jobID,
                    Constant.SEVERITY_MAJOR, "SetquorumWorkflow",
                    Constant.LOG_TYPE_WORKFLOW, master.getClusterName(), msg,
                    arguments);
        }
    }

    public void rerun(ThreadPool executor) throws MgmtDuplicatedReservedCallException {
        Logger.info("Schedule setquorum workflow to run after {}ms. {}", 
                Constant.SETQUORUM_SCHEDULE_INTERVAL, PartitionGroup.fullName(clusterName, pgid));
        workflowExecutor.performContextContinueDelayed(SET_QUORUM,
                Constant.SETQUORUM_SCHEDULE_INTERVAL, TimeUnit.MILLISECONDS,
                clusterName, pgid);
    }

}
