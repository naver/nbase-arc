package com.navercorp.nbasearc.confmaster.server.command;

import static com.navercorp.nbasearc.confmaster.Constant.ALL;
import static com.navercorp.nbasearc.confmaster.Constant.ALL_IN_PG;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_PG_NOT_EMPTY;
import static com.navercorp.nbasearc.confmaster.Constant.GW_PING;
import static com.navercorp.nbasearc.confmaster.Constant.GW_PONG;
import static com.navercorp.nbasearc.confmaster.Constant.GW_RESPONSE_OK;
import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_NO;
import static com.navercorp.nbasearc.confmaster.Constant.LOG_TYPE_COMMAND;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_LCONN_IN_PONG;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_MASTER;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_SLAVE;
import static com.navercorp.nbasearc.confmaster.Constant.REDIS_PONG;
import static com.navercorp.nbasearc.confmaster.Constant.REDIS_REP_PING;
import static com.navercorp.nbasearc.confmaster.Constant.S2C_OK;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_LCONN;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_NORMAL;
import static com.navercorp.nbasearc.confmaster.Constant.SETQUORUM_ADMITTABLE_RANGE;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_MODERATE;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtCommandWrongArgumentException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtRoleChangeException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.io.MultipleGatewayInvocator;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkNotificationDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkWorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockPGSList;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.GatewayImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;

@Service
public class PartitionGroupService {

    @Autowired
    private Config config;
    @Autowired
    private ThreadPool executor;

    @Autowired
    private ClusterImo clusterImo;
    @Autowired
    private PartitionGroupImo pgImo;
    @Autowired
    private PartitionGroupServerImo pgsImo;
    @Autowired
    private RedisServerImo rsImo;
    @Autowired
    private GatewayImo gwImo;

    @Autowired
    private PartitionGroupDao pgDao;
    @Autowired
    private ZkNotificationDao notificationDao;
    @Autowired
    private ZkWorkflowLogDao workflowLogDao;
    
    @CommandMapping(name="pg_add",
            usage="pg_add <cluster_name> <pgid>\r\n" +
                    "add a single partition group")
    public String pgAdd(String clusterName, String pgId)
            throws MgmtCommandWrongArgumentException, NodeExistsException,
            MgmtZooKeeperException, NoNodeException {
        // Check
        if (null != pgImo.get(pgId, clusterName)) {
            throw new MgmtCommandWrongArgumentException("-ERR duplicated pgid");
        }
        
        // In Memory
        Cluster cluster = clusterImo.get(clusterName);
        List<Gateway> gwList = gwImo.getList(clusterName);

        // Prepare        
        String reply = cluster.isGatewaysAlive(gwImo);
        if (null != reply) {
            Logger.error(reply);
            return reply;
        }

        MultipleGatewayInvocator broadcast = new MultipleGatewayInvocator(); 
        reply = broadcast.request(clusterName, gwList, GW_PING, GW_PONG, executor);
        if (null != reply) {
            Logger.error(reply);
            return reply;
        }

        PartitionGroupData data = 
            PartitionGroupData.builder().from(new PartitionGroupData()).build();
        
        // DB
        pgDao.createPg(pgId, clusterName, data);
        
        // In Memory
        PartitionGroup pg = pgImo.load(pgId, clusterName);

        String cmd = String.format("pg_add %s", pgId);
        reply = broadcast.request(clusterName, gwList, cmd, GW_RESPONSE_OK, executor);
        if (null != reply) {
            Logger.error(reply);
            return reply;
        }

        // Log
        workflowLogDao.log(0, SEVERITY_MODERATE, 
                "PGAddCommand", LOG_TYPE_COMMAND, 
                clusterName, "Add pg success. " + pg, 
                String.format("{\"cluster_name\":\"%s\",\"pg_id\":\"%s\"}", 
                        clusterName, pgId));
        
        return S2C_OK;
    }

    @LockMapping(name="pg_add")
    public void pgAddLock(HierarchicalLockHelper lockHelper, String clusterName) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(WRITE).pgsList(READ)
                .gwList(READ).gw(WRITE, ALL);
    }

    @CommandMapping(name="pg_del",
            usage="pg_del <cluster_name> <pgid>\r\n" +
                    "delete a single partition group")
    public String pgDel(String clusterName, String pgId)
            throws MgmtZooKeeperException {
        // Check
        PartitionGroup pg = pgImo.get(pgId, clusterName); 
        if (null == pg) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST
                            + PartitionGroup.fullName(clusterName, pgId));
        }

        if (!pg.getData().getPgsIdList().isEmpty()) {
            throw new IllegalArgumentException(EXCEPTIONMSG_PG_NOT_EMPTY
                    + PartitionGroup.fullName(clusterName, pgId));
        }

        // In Memory
        Cluster cluster = clusterImo.get(clusterName);
        List<Gateway> gwList = gwImo.getList(clusterName);
        
        String reply = cluster.isGatewaysAlive(gwImo);
        if (null != reply) {
            Logger.error(reply);
            return reply;
        }

        // Prepare
        MultipleGatewayInvocator broadcast = new MultipleGatewayInvocator(); 
        reply = broadcast.request(clusterName, gwList, GW_PING, GW_PONG, executor);
        if (null != reply) {
            Logger.error(reply);
            return reply;
        }
        
        // DB
        pgDao.deletePg(pgId, clusterName);
        
        // In Memory
        pgImo.delete(PathUtil.pgPath(pgId, clusterName));

        String cmd = String.format("pg_del %s", pgId);
        reply = broadcast.request(clusterName, gwList, cmd, GW_RESPONSE_OK, executor);
        if (null != reply) {
            Logger.error(reply);
            return reply;
        }
        
        // Logging
        workflowLogDao.log(0, SEVERITY_MODERATE, 
                "PGDelCommand", LOG_TYPE_COMMAND, 
                clusterName, "Delte pg success. " + pg, 
                String.format("{\"cluster_name\":\"%s\",\"pg_id\":\"%s\"}", 
                        clusterName, pgId));
        
        return S2C_OK;
    }

    @LockMapping(name="pg_del")
    public void pgDelLock(HierarchicalLockHelper lockHelper, String clusterName, String pgId) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(WRITE).pgsList(READ).pg(WRITE, pgId)
                .gwList(READ).gw(WRITE, ALL);
    }

    @CommandMapping(name="pg_info",
            usage="pg_info <cluster_name> <pg_id>\r\n" +
                    "get information of a Partition Group")
    public String pgInfo(String clusterName, String pgid) {
        // Check
        if (null == clusterImo.get(clusterName)) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        // In Memory
        PartitionGroup pg = pgImo.get(pgid, clusterName);
        if (null == pg) {
            return "-ERR pgs does not exist.";
        }

        // Do
        try {
            return pg.getData().toString();
        } catch (RuntimeException e) {
            return "-ERR internal data of pgs is not correct.";
        }
    }

    @LockMapping(name="pg_info")
    public void pgInfoLock(HierarchicalLockHelper lockHelper, String clusterName, String pgid) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(READ).pg(READ, pgid);
    }

    @CommandMapping(name="pg_ls",
            usage="pg_ls <cluster_name>\r\n")
    public String pgLs(String clusterName) throws KeeperException,
            InterruptedException {
        // In Memory
        List<PartitionGroup> pgList = pgImo.getList(clusterName);
        Cluster cluster = clusterImo.get(clusterName);
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }

        // Do
        StringBuilder reply = new StringBuilder();
        reply.append("{\"list\":[");
        for (PartitionGroup pg : pgList) {
            reply.append("\"").append(pg.getName()).append("\", ");
        }
        if (!pgList.isEmpty()) {
            reply.delete(reply.length()-2,reply.length()).append("]}");
        } else {
            reply.append("]}");
        }
        return reply.toString();
    }

    @LockMapping(name="pg_ls")
    public void pgLsLock(HierarchicalLockHelper lockHelper, String clusterName) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(READ)
                .pg(READ, ALL);
    }

    @CommandMapping(name="role_change",
            usage="role_change <cluster_name> <pgsid>")
    public String roleChange(String clusterName, String pgsid)
            throws KeeperException, InterruptedException {
        Cluster cluster = clusterImo.get(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }

        PartitionGroupServer masterCandidate = pgsImo.get(pgsid, clusterName);
        if (masterCandidate == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST
                            + PartitionGroupServer.fullName(clusterName, pgsid));
        }
        
        PartitionGroup pg = pgImo.get(String.valueOf(masterCandidate.getData().getPgId()), clusterName);
        if (pg == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                            + PartitionGroup.fullName(clusterName,
                                    String.valueOf(masterCandidate.getData().getPgId())));
        }
        
        try {
            return roleChangeImpl(cluster, pg, masterCandidate);
        } finally {
            try {
                notificationDao.updateGatewayAffinity(cluster);
            } catch (MgmtZooKeeperException e) {
                String msg = "-ERR failed to update gateway affinity. cluster: " + clusterName;
                Logger.error(msg, e);
                return msg;
            }
        }
    }
    
    public String roleChangeImpl(Cluster cluster, PartitionGroup pg, PartitionGroupServer target) 
            throws KeeperException, InterruptedException {
        final long jobID = JobIDGenerator.getInstance().getID();

        if (target.getData().getHb().equals(HB_MONITOR_NO)) {
            String msg = "-ERR PGS has not joined."; 
            Logger.error(msg);
            return msg;
        }
        
        if (!target.getState().equals(SERVER_STATE_LCONN) &&
                !target.getState().equals(SERVER_STATE_NORMAL)) {
            String msg = "-ERR target's state is '" + target.getState() + "'"; 
            Logger.error(msg);
            return msg;
        }

        // Get available PGS list
        final List<PartitionGroupServer> availablePgsList = 
                pg.getAvailablePgsList(
                        pgsImo.getList(target.getClusterName(), target.getData().getPgId()));

        // Find master
        PartitionGroupServer master = null;
        for (PartitionGroupServer pgs : availablePgsList) {
            if (pgs.getData().getRole().equals(PGS_ROLE_MASTER)) {
                master = pgs;
                break;
            }
        }
        
        // Check master (and master must be joined)
        if (master == null) {
            String msg = "-ERR pgs does not have the master. " + cluster
                    + ", PG=" + target.getData().getPgId();
            Logger.error(msg);
            return msg;
        }
        
        if (target.equals(master)) {
            String msg = "-ERR target is the master. " + cluster + ", PG="
                    + target.getData().getPgId(); 
            Logger.error(msg);
            return msg;
        }
        
        // Check slave count
        if (availablePgsList.size() == 1) {
            String msg = "-ERR pg doesn't have any slave. " + cluster + ", PG="
                    + target.getData().getPgId();
            Logger.error(msg);
            return msg;
        }
        
        RedisServer rs = rsImo.get(master.getName(), cluster.getName());
        if (rs == null) {
            String msg = "-ERR Redis '" + master.getName() + "' does not exist.";
            Logger.error(msg);
            return msg;
        }
        
        // Check redis replication
        for (PartitionGroupServer pgs : availablePgsList) {
            RedisServer r = rsImo.get(pgs.getName(), cluster.getName());
            try {
                if (null == r) {
                    String msg = "-ERR check redis replication ping fail, redis object doesn't exist, "
                            + cluster + ", REDIS=" + pgs.getName();
                    Logger.error(msg);
                    return msg;
                }
                
                String reply = r.executeQuery(REDIS_REP_PING);
                if (!reply.equals(REDIS_PONG)) {
                    String msg = "-ERR check redis replication ping fail, invalid reply. "
                            + cluster
                            + ", REDIS=" + r.getIP() + ":" + r.getPort() + 
                            ", REPLY=" + reply;
                    Logger.error(msg);
                    return msg;
                }
            } catch (Exception e) {
                String msg = "-ERR check redis replication ping fail, Exception. "
                        + cluster
                        + ", REDIS=" + r.getIP() + ":" + r.getPort()
                        + ", EXCEPTION=" + e.getMessage();
                Logger.error(msg, e);
                return msg;
            }
        }
        
        // Quorum escalation
        Integer quorum = cluster.getQuorum(availablePgsList.size());
        String setquorum = "setquorum " + quorum;
        String reply;
        try {
            reply = master.executeQuery(setquorum);
            if (!reply.equals(S2C_OK)) {
                String msg = "-ERR setquorum fail. CMD=" + setquorum + ", REPLY=" + reply;
                Logger.error(msg);
                return msg;
            }
        } catch (Exception e) {
            String msg = "-ERR setquorum fail. check state of PGS '" + master.getName() + "'"; 
            Logger.error(msg, e);
            return msg;
        }
        
        // Check quorum escalation
        try {
            reply = master.executeQuery("getquorum");
            if (quorum != Integer.parseInt(reply)) {
                String msg = "-ERR check quorum fail. consider setquorum to master. EXPECTED_QUORUM=" + quorum + ", ACTIVE_QUORUM=" + reply;
                Logger.error(msg);
                return msg;
            }
        } catch (Exception e) {
            String msg = "-ERR check quorum fail. consider setquorum to master. EXCEPTION=" + e.getMessage();
            Logger.error(msg, e);
            return msg;
        }
        
        // Wait catch up
        long stx = System.currentTimeMillis();
        boolean rollback = false;
        while (true) {
            LogSequence masterLogSeq = master.getLogSeq();
            LogSequence targetLogSeq = target.getLogSeq();
            
            if (masterLogSeq == null || targetLogSeq == null) {
                rollback = true;
                break;
            }
            
            if (masterLogSeq.getLogCommit() - targetLogSeq.getLogCommit() 
                    < SETQUORUM_ADMITTABLE_RANGE) {
                break;
            }
            
            if (System.currentTimeMillis() - stx 
                    > config.getServerCommandRolechangeLogcatchTimeout()) {
                rollback = true;
                break;
            }
        }
        
        if (!rollback) {
            // Check ping replication
            try {
                reply = rs.executeQuery(REDIS_REP_PING);
                if (!reply.equals(REDIS_PONG)) {
                    Logger.error("Failed to redis replication ping, invalid reply, "
                            + cluster
                            + ", REDIS=" + master.getName() 
                            + ", REPLY=" + reply);
                    rollback = true;
                }
            } catch (IOException e) {
                Logger.error("Failed to redis replication ping, " + cluster
                        + ", REDIS=" + master.getName(), e);
                rollback = true;
            } catch (Exception e) {
                Logger.error("Failed to redis replication ping, " + cluster
                        + ", REDIS=" + master.getName(), e);
                rollback = true;
            }
        }
        
        // Rollback quorum
        if (rollback) {
            int closePgsCount = pg.getClosePgsIds(master, pg.getLogSeqOfPG(availablePgsList)).size();
            quorum = cluster.getQuorum(closePgsCount);
            setquorum = "setquorum " + quorum;

            try {
                reply = master.executeQuery(setquorum);
                if (!reply.equals(S2C_OK)) {
                    String msg = "-ERR rollback quorum fail. consider setquorum to master. "
                            + cluster
                            + ", MASTER=" + master.getIP() + ":" + master.getData().getSmrMgmtPort()
                            + ", QUORUM=" + quorum 
                            + ", REPLY=" + reply;
                    Logger.error(msg);
                    return msg;
                }
            } catch (Exception e) {
                String msg = "-ERR rollback quorum fail. check state of master. "
                        + cluster
                        + ", MASTER=" + master.getIP() + ":" + master.getData().getSmrMgmtPort()
                        + ", QUORUM=" + quorum 
                        + ", EXCEPTION=" + e.getMessage();
                Logger.error(msg, e);
                return msg;
            }
            
            return "-ERR catch up fail, rollback done.";
        }
        
        // Change master role to LCONN
        try {
            reply = master.executeQuery("role lconn");
            if (!reply.equals(S2C_OK)) {
                String msg = "-ERR role lconn fail. check state of PGS. "
                        + cluster
                        + ", MASTER=" + master.getIP() + ":" + master.getData().getSmrMgmtPort() 
                        + ", REPLY=" + reply;
                Logger.error(msg);
                return msg;
            }
        } catch (Exception e) {
            String msg = "-ERR role lconn fail. check state of PGS. " 
                    + cluster
                    + ", MASTER=" + master.getIP() + ":" + master.getData().getSmrMgmtPort() 
                    + ", EXCEPTION=" + e.getMessage();
            Logger.error(msg, e);
            return msg;
        }
        
        // Wait until all PGSs is LCONN
        stx = System.currentTimeMillis();
        while (true) {
            boolean ok = true;
            
            for (PartitionGroupServer pgs : availablePgsList) {
                String activeRole;
                try {
                    activeRole = pgs.getActiveRole();
                    if (!activeRole.equals(PGS_ROLE_LCONN_IN_PONG)) {
                        ok = false;
                        break;
                    }
                } catch (Exception e) {
                    String msg = "-ERR role lconn fail. check state of PGS. " 
                            + cluster 
                            + ", PGS=" + pgs.getName() 
                            + ", PG=" + pg.getName() 
                            + ", EXCEPTION=" + e.getMessage();
                    Logger.error(msg, e);
                    return msg;
                }
            }
            
            if (ok)
                break;

            long cur = System.currentTimeMillis();
            if (cur - stx > config.getServerCommandRolechangeLconnTimeout()) {
                String msg = "-ERR role lconn timeout. check state of PGS. "
                        + cluster + ", PG=" + pg.getName();
                Logger.error(msg);
                return msg;
            }
        }
        
        // Get Log sequences
        Logger.info(pg.getData().getMasterGenMap().toString());
        Map<String, LogSequence> logSeqMap = pg.getLogSeqOfPG(availablePgsList);
        
        // If the target is unavailable, it replaces target with another slave.
        long maxCommitSeq = 0;
        PartitionGroupServer substitute = null;
        for (PartitionGroupServer pgs : availablePgsList) {
            if (pgs.getData().getRole().equals(PGS_ROLE_SLAVE)) {
                LogSequence logSeq = logSeqMap.get(pgs.getName());
                if (logSeq == null) {
                    continue;
                }
                
                if (maxCommitSeq <= logSeq.getMax()) {
                    maxCommitSeq = logSeq.getMax();
                    substitute = pgs;
                }
            }
        }

        // If there is no substitute PGS, recover master and slaves
        if (substitute == null) {
            return recoverMasterSlave(master, pg, availablePgsList, logSeqMap,
                    cluster, jobID, workflowLogDao);
        }
        
        if (logSeqMap.get(target.getName()).getMax() == 
                logSeqMap.get(substitute.getName()).getMax()) {
            substitute = target;
        }
        
        // Role master
        try {
            substitute.roleMaster(pg, availablePgsList, logSeqMap, cluster,
                    quorum, jobID, workflowLogDao);
        } catch (MgmtRoleChangeException e) {
            String msg = "-ERR role master fail. PGS=" + substitute.getIP()
                    + ":" + substitute.getData().getSmrMgmtPort()
                    + ", EXCEPTION=" + e.getMessage();
            Logger.error(msg, e);
            return msg;
        }
        
        // Role slave
        StringBuilder failedSlaves = new StringBuilder();
        for (PartitionGroupServer pgs : availablePgsList) {
            if (pgs.equals(substitute))
                continue;
            
            LogSequence logSeq = logSeqMap.get(pgs.getName());
            try {
                pgs.roleSlave(pg, logSeq, substitute, jobID, workflowLogDao);
            } catch (MgmtRoleChangeException e) {
                Logger.error("-ERR role slave fail. PGS=" + pgs.getIP() + ":"
                        + pgs.getData().getSmrMgmtPort(), e);
                failedSlaves.append("\"").append(pgs.getIP()).append(":")
                        .append(pgs.getData().getSmrMgmtPort()).append("\",");
            }
        }
        
        // Delete ',' at the tail of the string in failedSlaves. 
        if (failedSlaves.length() > 0)
            failedSlaves.deleteCharAt(failedSlaves.length() - 1);
        
        return "{\"master\":" + substitute.getName() + ",\"role_slave_error\":[" + failedSlaves + "]}";
    }
    
    public String recoverMasterSlave(PartitionGroupServer master, PartitionGroup pg,
            List<PartitionGroupServer> availablePgsList,
            Map<String, LogSequence> logSeqMap, Cluster cluster, long jobID,
            WorkflowLogDao workflowLogDao) {
        // Recover master
        try {
            master.roleMaster(pg, availablePgsList, logSeqMap, cluster, 0,
                    jobID, workflowLogDao);
        } catch (MgmtRoleChangeException e) {
            String msg = "-ERR there is no substitute and recover master fail. MASTER="
                    + master.getIP()
                    + ":"
                    + master.getData().getSmrMgmtPort()
                    + ", EXCEPTION="
                    + e.getMessage();
            Logger.error(msg, e);
            return msg;
        }
        
        // Role slave
        StringBuilder failedSlaves = new StringBuilder();
        for (PartitionGroupServer pgs : availablePgsList) {
            if (pgs.equals(master))
                continue;
            
            LogSequence logSeq = logSeqMap.get(pgs.getName());
            try {
                pgs.roleSlave(pg, logSeq, master, jobID, workflowLogDao);
            } catch (MgmtRoleChangeException e) {
                Logger.error("-ERR role slave fail. PGS=" + pgs.getIP() + ":"
                        + pgs.getData().getSmrMgmtPort(), e);
                failedSlaves.append(pgs.getIP()).append(":")
                        .append(pgs.getData().getSmrMgmtPort()).append(",");
            }
        }
        
        // Delete ',' at the tail of the string in failedSlaves. 
        if (failedSlaves.length() > 0)
            failedSlaves.deleteCharAt(failedSlaves.length() - 1);
        
        return "-ERR there is no substitute. rollback. role_slave_error:[" + failedSlaves + "]";
    }

    @LockMapping(name="role_change")
    public void roleChangeLock(HierarchicalLockHelper lockHelper,
            String clusterName, String pgsid) {
        HierarchicalLockPGSList lock = lockHelper.root(READ)
                .cluster(READ, clusterName).pgList(READ).pgsList(READ);
        PartitionGroupServer pgs = pgsImo.get(pgsid, clusterName);
        lock.pg(WRITE, String.valueOf(pgs.getData().getPgId())).pgs(WRITE,
                ALL_IN_PG).gwList(READ);
    }

}
