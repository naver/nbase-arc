package com.navercorp.nbasearc.confmaster.server.command;

import static com.navercorp.nbasearc.confmaster.Constant.ALL;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_PHYSICAL_MACHINE_CLUSTER_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_PHYSICAL_MACHINE_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_ZOOKEEPER;
import static com.navercorp.nbasearc.confmaster.Constant.GW_PING;
import static com.navercorp.nbasearc.confmaster.Constant.GW_PONG;
import static com.navercorp.nbasearc.confmaster.Constant.GW_RESPONSE_OK;
import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_NO;
import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_YES;
import static com.navercorp.nbasearc.confmaster.Constant.LOG_TYPE_COMMAND;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_MASTER;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_NONE;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_SLAVE;
import static com.navercorp.nbasearc.confmaster.Constant.S2C_OK;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_FAILURE;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_MODERATE;
import static com.navercorp.nbasearc.confmaster.Constant.TEMP_ZNODE_NAME_FOR_CHILDEVENT;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.FAILOVER_PGS;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.SET_QUORUM;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtDuplicatedReservedCallException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.io.MultipleGatewayInvocator;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkNotificationDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkWorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.RedisServerData;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.GatewayImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;

@Service
public class PartitionGroupServerService {

    @Autowired
    private WorkflowExecutor workflowExecutor;

    @Autowired
    private ZooKeeperHolder zookeeper;
    @Autowired
    private Config config;
    @Autowired
    private ThreadPool executor;

    @Autowired
    private ClusterImo clusterImo;
    @Autowired
    private PhysicalMachineImo pmImo;
    @Autowired
    private PartitionGroupImo pgImo;
    @Autowired
    private PartitionGroupServerImo pgsImo;
    @Autowired
    private RedisServerImo rsImo;
    @Autowired
    private GatewayImo gwImo;
    @Autowired
    private PhysicalMachineClusterImo pmClusterImo;

    @Autowired
    private PartitionGroupDao pgDao;
    @Autowired
    private PartitionGroupServerDao pgsDao;
    @Autowired
    private ZkNotificationDao notificationDao;
    @Autowired
    private ZkWorkflowLogDao workflowLogDao;
 
    @CommandMapping(
            name="pgs_add",
            usage="pgs_add <cluster_name> <pgsid> <pgid> <pm_name> <pm_ip> <base_port> <backend_port>")
    public String pgsAdd(String clusterName, String pgsId, String pgId,
            String pmName, String pmIp, int basePort, int backendPort)
            throws MgmtZooKeeperException, NoNodeException {
        List<PartitionGroupServer> pgsList = pgsImo.getList(clusterName);
        PhysicalMachine pm = pmImo.get(pmName);

        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getName().equals(pgsId)) {
                throw new IllegalArgumentException("-ERR duplicated. cluster: "
                        + clusterName + ", pgs: " + pgsId);
            }
        }
        
        if (null == pm) {
            return "-ERR pm does not exist.";
        }
        
        if (!pm.getData().getIp().equals(pmIp)) {
            return "-ERR pm ip does not match.";
        }

        Cluster cluster = clusterImo.get(clusterName);
        if (null == cluster) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }

        if (null == pmImo.get(pmName)) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PHYSICAL_MACHINE_DOES_NOT_EXIST
                            + PhysicalMachine.fullName(pmName));
        }

        PartitionGroup pg = pgImo.get(pgId, clusterName);
        if (null == pg) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                            + PartitionGroup.fullName(clusterName, pgId));
        }
//
        // Do
        PartitionGroupServerData data = new PartitionGroupServerData();
        data.initialize(
                Integer.parseInt(pgId), pmName, pmIp, backendPort,
                basePort, basePort + 3, SERVER_STATE_FAILURE,
                PGS_ROLE_NONE, pg.getData().currentGen(),
                HB_MONITOR_NO);
        
        // DB
        PhysicalMachineCluster pmCluster = pmClusterImo.get(clusterName, pmName);
        pgsDao.createPgs(pgsId, clusterName, data, pg, pmCluster);
        
        PartitionGroupServer pgs = pgsImo.load(pgsId, clusterName);
        rsImo.load(pgsId, clusterName);

        // Add to pg 
        PartitionGroupData pgModified = 
            PartitionGroupData.builder().from(pg.getData())
                .addPgsId(Integer.valueOf(pgsId)).build();
        pg.setData(pgModified);
    
        // Add to pm-cluster
        if (null != pmCluster) {
            pmCluster.getData().addPgsId(Integer.valueOf(pgsId));
        } else {
            pmClusterImo.load(clusterName, pmName);
        }
        
        // Log
        workflowLogDao.log(0, SEVERITY_MODERATE, 
                "PGSAddCommand", LOG_TYPE_COMMAND, 
                clusterName, "Add pgs success. " + pgs, 
                String.format("{\"cluster_name\":\"%s\",\"pgsid\":\"%s\",\"pgid\":\"%s\",\"host\":\"%s\",\"pm_ip\":\"%s\",\"base_port\":%d,\"backend_port\":%d}", 
                clusterName, pgsId, pgId, pmName, pmIp, basePort, backendPort));
        
        return S2C_OK; 
    }

    @LockMapping(name="pgs_add")
    public void pgsAddLock(HierarchicalLockHelper lockHelper,
            String clusterName, String pgsId, String pgId, String host) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(WRITE)
                .pg(WRITE, pgId);
        lockHelper.pmList(READ).pm(WRITE, host);
    }
    
    @CommandMapping(
            name="pgs_del",
            usage="pgs_del <cluster_name> <pgsid>")
    public String pgsDel(String clusterName, String pgsId) throws MgmtZooKeeperException {
        // Check & Cache
        PartitionGroupServer pgs = pgsImo.get(pgsId, clusterName);
        if (null == pgs) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST + "pgs:"
                            + pgsId);
        }
        
        if (pgs.getData().getHb().equals(HB_MONITOR_YES)) {
            return "-ERR pgs has been joined yet.";
        }

        PartitionGroup pg = pgImo.get(String.valueOf(pgs.getData().getPgId()), clusterName);
        if (null == pg) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                            + PartitionGroup.fullName(clusterName,
                                    String.valueOf(pgs.getData().getPgId())));
        }
        
        String pmName = pgs.getData().getPmName();
        PhysicalMachineCluster pmCluster = pmClusterImo.get(
                clusterName, pgs.getData().getPmName());
        if (null == pmCluster) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PHYSICAL_MACHINE_CLUSTER_DOES_NOT_EXIST
                            + PhysicalMachineCluster.fullName(pmName,
                                    clusterName));
        }
        
        // DB
        pgsDao.deletePgs(pgsId, clusterName, pg, pmCluster);
        // don't need to call deleteRs, because deletePgs handles it. TODO : explicitly delete rs
        
        // In Memory
        pgsImo.delete(pgsId, clusterName);
        rsImo.delete(pgsId, clusterName);

        // PG
        PartitionGroupData pgModified = 
            PartitionGroupData.builder().from(pg.getData())
                .deletePgsId(Integer.valueOf(pgsId)).build();
        pg.setData(pgModified);

        // Delete PGS in Physical Machine
        final PhysicalMachineCluster clusterInPm = pmClusterImo.get(clusterName, pmName);
        if (clusterInPm != null) {
            clusterInPm.getData().deletePgsId(pgsId);
            if (clusterInPm.isEmpty()) {
                pmClusterImo.delete(PathUtil.pmClusterPath(clusterName, pmName));
            }
        }
        
        // Log
        workflowLogDao.log(0, SEVERITY_MODERATE, 
                "PGSDelCommand", LOG_TYPE_COMMAND, 
                clusterName, "Delete pgs success. " + pgs, 
                String.format("{\"cluster_name\":\"%s\",\"pgsid\":\"%s\"}", clusterName, pgsId));
        
        return S2C_OK;
    }

    @LockMapping(name="pgs_del")
    public void pgsDelLock(HierarchicalLockHelper lockHelper,
            String clusterName, String pgsid) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(WRITE)
                .pg(WRITE, null).pgs(WRITE, pgsid);
        PartitionGroupServer pgs = pgsImo.get(pgsid, clusterName);
        lockHelper.pmList(READ).pm(WRITE, pgs.getData().getPmName());
    }

    @CommandMapping(
            name="pgs_info_all",
            usage="pgs_info_all <cluster_name> <pgs_id>\r\n" +
                    "get all information of a Partition Group Server")
    public String pgsInfoAll(String clusterName, String pgsid) throws InterruptedException {
        // In Memory
        PartitionGroupServer pgs = pgsImo.get(pgsid, clusterName);
        Cluster cluster = clusterImo.get(clusterName);
        
        // Check
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        if (null == pgs) {
            return "-ERR pgs does not exist.";
        }

        // Do
        try {
            String data = pgs.getData().toString();
            String extraData = pgs.getRefData().toString();
            return "{\"MGMT\":" + data + ",\"HBC\":" + extraData + "}";
        } catch (RuntimeException e) {
            return "-ERR internal data of pgs is not correct.";
        }
    }
    
    @LockMapping(name="pgs_info_all")
    public void pgsInfoAllLock(HierarchicalLockHelper lockHelper,
            String clusterName, String pgsid) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(READ, null).pgs(READ, pgsid);
    }

    @CommandMapping(
            name="pgs_info",
            usage="pgs_info <cluster_name> <pgs_id>\r\n" +
                    "get information of a Partition Group Server")
    public String pggInfo(String clusterName, String pgsid) throws InterruptedException {
        // In Memory
        Cluster cluster = clusterImo.get(clusterName);
        PartitionGroupServer pgs = pgsImo.get(pgsid, clusterName);
        
        // Check
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        if (null == pgs) {
            return "-ERR pgs does not exist.";
        }

        // Do
        try {
            return pgs.getData().toString();
        } catch (RuntimeException e) {
            return "-ERR internal data of pgs is not correct.";
        }
    }

    @LockMapping(name="pgs_info")
    public void pggInfoLock(HierarchicalLockHelper lockHelper,
            String clusterName, String pgsid) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(READ, null).pgs(READ, pgsid);
    }
    
    @CommandMapping(
            name="pgs_join",
            usage="pgs_join <cluster_name> <pgsid>")
    public String pgsJoin(String clusterName, String pgsid) {
        // In Memory
        PartitionGroupServer pgs = pgsImo.get(pgsid, clusterName);
        if (pgs.getData().getHb().equals(HB_MONITOR_YES)) {
            return "-ERR pgs already joined";
        }
        
        RedisServer rs = rsImo.get(pgsid, clusterName);
        if (rs.getData().getHB().equals(HB_MONITOR_YES)) {
            return "-ERR rs already joined";
        }
        
        Cluster cluster = clusterImo.get(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }
        
        PartitionGroup pg = pgImo.get(String.valueOf(pgs.getData().getPgId()), clusterName);
        if (pg == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                            + PartitionGroup.fullName(clusterName,
                                    String.valueOf(pgs.getData().getPgId())));
        }
        
        PhysicalMachineCluster pmCluster = pmClusterImo.get(clusterName,
                pgs.getData().getPmName());
        if (pmCluster == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PHYSICAL_MACHINE_CLUSTER_DOES_NOT_EXIST
                            + PhysicalMachineCluster.fullName(pgs.getData()
                                    .getPmName(), clusterName));
        }
        
        List<Gateway> gwList = gwImo.getList(clusterName);

        // Do
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
        
        PartitionGroupData pgModified =
            PartitionGroupData.builder().from(pg.getData())
                .withCopy(pg.getJoinedPgsList(
                    pgsImo.getList(
                        clusterName, pgs.getData().getPgId())).size() + 1).build();
        PartitionGroupServerData pgsModified = 
            PartitionGroupServerData.builder().from(pgs.getData())
                .withHb(HB_MONITOR_YES).build();
        RedisServerData rsModified = 
            RedisServerData.builder().from(rs.getData())
                .withHb(HB_MONITOR_YES).build();

        try {
            List<Op> ops = new ArrayList<Op>();
            
            ops.add(pgDao.createUpdatePgOperation(pg.getPath(), pgModified));
            ops.add(pgsDao.createUpdatePgsOperation(pgs.getPath(), pgsModified));
            ops.add(pgsDao.createUpdateRsOperation(rs.getPath(), rsModified));

            ZooKeeper zk = zookeeper.getZooKeeper();
            List<OpResult> results = null;
            try {
                results = zk.multi(ops);

                OpResult.SetDataResult rsd = (OpResult.SetDataResult)results.get(0);
                pgs.setStat(rsd.getStat());

                rsd = (OpResult.SetDataResult)results.get(1);
                rs.setStat(rsd.getStat());
                
                pg.setData(pgModified);
                pgs.setData(pgsModified);
                rs.setData(rsModified);
            } finally {
                zookeeper.handleResultsOfMulti(results);
            }
        } catch (Exception e) {
            return EXCEPTIONMSG_ZOOKEEPER;
        }

        String cmd = String.format("pgs_add %s %d %s %d", 
                pgsid, pgs.getData().getPgId(), 
                pgs.getData().getPmIp(), 
                pgs.getData().getRedisPort());
        reply = broadcast.request(clusterName, gwList, cmd, GW_RESPONSE_OK, executor);
        if (null != reply) {
            return reply;
        }

        // Log
        workflowLogDao.log(0, SEVERITY_MODERATE, 
                "PGSJoinCommand", LOG_TYPE_COMMAND, 
                clusterName, "Join pgs success. " + pgs, 
                String.format("{\"cluster_name\":\"%s\",\"pgsid\":\"%s\"}", clusterName, pgsid));
        
        return S2C_OK;
    }

    @LockMapping(name="pgs_join")
    public void pgsJoinLock(HierarchicalLockHelper lockHelper,
            String clusterName, String pgsid) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(READ)
                .pg(READ, null).pgs(WRITE, pgsid)
                .gwList(READ).gw(WRITE, ALL);
    }
    
    @CommandMapping(
            name="pgs_lconn",
            usage="pgs_lconn <cluster_name> <pgsid>")
    public String pgsLconn(String clusterName, String pgsid)
            throws NodeExistsException, MgmtZooKeeperException,
            MgmtDuplicatedReservedCallException, IOException {
        PartitionGroupServer pgs = pgsImo.get(pgsid, clusterName);
        if (pgs.getData().getHb().equals(HB_MONITOR_YES)) {
            return "-ERR pgs does not left yet";
        }
        
        RedisServer rs = rsImo.get(pgsid, clusterName);
        if (rs.getData().getHB().equals(HB_MONITOR_YES)) {
            return "-ERR rs does not left yet";
        }
        
        String reply = roleLconn(pgs, clusterName, executor);
        if (null != reply) {
            return reply;
        }
        
        return S2C_OK;
    }

    public String roleLconn(PartitionGroupServer pgs, String clusterName,
            ThreadPool executor) throws MgmtZooKeeperException,
            NodeExistsException, MgmtDuplicatedReservedCallException,
            IOException {
        String cmd = "role lconn";
        String reply; 
        try {
            reply = pgs.executeQuery(cmd);
            if (!reply.equals(S2C_OK)) {
                Logger.error("Role lconn fail. pgs; {}, reply: {}", pgs, reply);
                return String.format("-ERR cmd=\"%s\", reply=\"%s\"", cmd, reply);
            }
            Logger.info("Role lconn success. pgs; {}, reply: {}", pgs, reply);
        } catch (IOException e) {
            Logger.info("Role lconn fail. pgs; {}", pgs, e);
            throw e;
        }
        
        List<PartitionGroupServer> pgsList =
                pgsImo.getList(clusterName,  pgs.getData().getPgId());
        
        if (pgs.getData().getRole().equals(PGS_ROLE_MASTER)) {
            for (PartitionGroupServer p : pgsList) {
                PartitionGroupServerData pgsModified = 
                        PartitionGroupServerData.builder().from(p.getData())
                            .withRole(PGS_ROLE_NONE).build();
                
                pgsDao.updatePgs(p.getPath(), pgsModified);
                
                p.setData(pgsModified);
            }
            
            // in order to have confmaster put an opinion quickly
            for (PartitionGroupServer pgsInPg : pgsList) {
                String path = String.format("%s/%s_%s", pgsInPg.getPath(), TEMP_ZNODE_NAME_FOR_CHILDEVENT, config.getIp() + ":" + config.getPort());                
                zookeeper.createEphemeralZNode(path);
                zookeeper.deleteZNode(path, -1);
            }
            
            workflowExecutor.performContextContinue(FAILOVER_PGS, pgs);
        } else {
            PartitionGroupServerData pgsModified = 
                    PartitionGroupServerData.builder().from(pgs.getData())
                        .withRole(PGS_ROLE_NONE).build();
            
            pgsDao.updatePgs(pgs.getPath(), pgsModified);
            
            pgs.setData(pgsModified);
        }
        
        return null;
    }
    
    @LockMapping(name="pgs_lconn")
    public void pgsLconnLock(HierarchicalLockHelper lockHelper,
            String clusterName, String pgsid) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(READ)
                .pg(WRITE, null).pgs(WRITE, pgsid);
    }

    @CommandMapping(
            name="pgs_leave",
            usage="pgs_leave <cluster_name> <pgsid>")
    public String pgsLeave(String clusterName, String pgsid) {
        // In Memory
        Cluster cluster = clusterImo.get(clusterName);
        if (null == cluster) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }
        PartitionGroupServer pgs = pgsImo.get(pgsid, clusterName);
        if (null == pgs) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST 
                            + PartitionGroupServer.fullName(clusterName, pgsid));
        }
        RedisServer rs = rsImo.get(pgsid, clusterName);
        if (null == rs) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST 
                            + RedisServer.fullName(clusterName, pgsid));
        }
        PartitionGroup pg = pgImo.get(String.valueOf(pgs.getData().getPgId()), clusterName);
        if (null == pg) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                            + PartitionGroup.fullName(clusterName, 
                                String.valueOf(pgs.getData().getPgId())));
        }

        List<Gateway> gwList = gwImo.getList(clusterName);
        
        // Check
        if (pgs.getData().getHb().equals(HB_MONITOR_NO)) {
            return "-ERR pgs already left";
        }
        
        if (rs.getData().getHB().equals(HB_MONITOR_NO)) {
            return "-ERR rs already left";
        }
        
        // Do
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
        
        final String oldRole = pgs.getData().getRole();

        PartitionGroupData pgModified = 
            PartitionGroupData.builder().from(pg.getData())
                .withCopy(pg.getJoinedPgsList(
                    pgsImo.getList(
                        clusterName, pgs.getData().getPgId())).size() - 1).build();
        PartitionGroupServerData pgsModified = 
            PartitionGroupServerData.builder().from(pgs.getData())
                .withHb(HB_MONITOR_NO)
                .withRole(PGS_ROLE_NONE).build();
        RedisServerData rsModified = 
                RedisServerData.builder().from(rs.getData())
                    .withHb(HB_MONITOR_NO).build();
        try {
            List<Op> ops = new ArrayList<Op>();
            
            ops.add(pgDao.createUpdatePgOperation(pg.getPath(), pgModified));
            ops.add(pgsDao.createUpdatePgsOperation(pgs.getPath(), pgsModified));
            ops.add(pgsDao.createUpdateRsOperation(rs.getPath(), rsModified));
            ops.add(notificationDao.createGatewayAffinityUpdateOperation(cluster));

            ZooKeeper zk = zookeeper.getZooKeeper();
            List<OpResult> results = null;
            try {
                results = zk.multi(ops);

                OpResult.SetDataResult rsd = (OpResult.SetDataResult)results.get(0);
                pgs.setStat(rsd.getStat());

                rsd = (OpResult.SetDataResult)results.get(1);
                rs.setStat(rsd.getStat());
                
                pg.setData(pgModified);
                pgs.setData(pgsModified);
                rs.setData(rsModified);
            } finally {
                zookeeper.handleResultsOfMulti(results);
            }
        } catch (Exception e) {
            return EXCEPTIONMSG_ZOOKEEPER;
        }

        if (oldRole.equals(PGS_ROLE_SLAVE)) {
            workflowExecutor.perform(SET_QUORUM, clusterName,
                    String.valueOf(pgs.getData().getPgId()));
        }
        
        String cmd = String.format("pgs_del %s %d", pgsid, pgs.getData().getPgId());
        reply = broadcast.request(clusterName, gwList, cmd, GW_RESPONSE_OK, executor);
        if (null != reply) {
            Logger.error(reply);
            return reply;
        }

        StringBuilder errMsg = new StringBuilder();
        try {
            pgs.updateHBRef();
        } catch (Exception e) {
            Logger.error(
                    "Update heartbeat-reference-data error. cluster: {}, pgs: {}, hb: {}",
                    new Object[]{clusterName, pgsid, pgs.getData().getHb()}, e);
            errMsg.append("update PGS heartbeat fail");
        }

        try {
            rs.updateHBRef();
        } catch (Exception e) {
            Logger.error(
                    "Update heartbeat-reference-data error. cluster: {}, rs: {}, hb: {}",
                    new Object[]{clusterName, pgsid, pgs.getData().getHb()}, e);
            errMsg.append(", update RS heartbeat fail");
        }
        
        if (errMsg.length() == 0) {
            workflowLogDao.log(0, SEVERITY_MODERATE, 
                    "PGSLeaveCommand", LOG_TYPE_COMMAND, 
                    clusterName, "pgs was left.", 
                    String.format("{\"cluster_name\":\"%s\",\"pgsid\":\"%s\",\"error\":\"\"}", clusterName, pgsid));
            return S2C_OK;
        } else {
            workflowLogDao.log(0, SEVERITY_MODERATE, 
                    "PGSLeaveCommand", LOG_TYPE_COMMAND, 
                    clusterName, "Leave pgs success. " + pgs, 
                    String.format("{\"cluster_name\":\"%s\",\"pgsid\":\"%s\",\"error\":\"%s\"}", clusterName, pgsid, errMsg.toString()));
            errMsg.insert(0, "PGSLeave done but, ");
            return errMsg.toString();
        }
    }
    
    @LockMapping(name="pgs_leave")
    public void pgsLeaveLock(HierarchicalLockHelper lockHelper, String clusterName, String pgsid) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(READ, null).pgs(WRITE, pgsid).gwList(READ)
                .gw(WRITE, ALL);
    }

    @CommandMapping(
            name="pgs_ls",
            usage="pgs_ls <cluster_name>\r\n" +
                    "show a list of Partition Group Servers")
    public String execute(String clusterName) {
        // In Memory
        Cluster cluster = clusterImo.get(clusterName);
        List<PartitionGroupServer> pgsList = pgsImo.getList(clusterName);

        // Check
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }

        // Do
        StringBuilder reply = new StringBuilder();
        reply.append("{\"list\":[");
        for (PartitionGroupServer pgs : pgsList) {
            reply.append("\"").append(pgs.getName()).append("\", ");
        }
        if (!pgsList.isEmpty()) {
            reply.delete(reply.length() - 2, reply.length()).append("]}");
        } else {
            reply.append("]}");
        }
        return reply.toString();
    }

    @LockMapping(name="pgs_ls")
    public void lock(HierarchicalLockHelper lockHelper, String clusterName) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(READ)
                .pg(READ, ALL).pgs(READ, ALL);
    }
    
    @CommandMapping(
            name="pgs_sync",
            usage="pgs_sync <cluster_name> <pgsid>")
    public String pgsSync(String clusterName, String pgsid) {
        if (LeaderState.isLeader()) {
            return workLeader(clusterName, pgsid);
        } else {
            return workFollower(clusterName, pgsid);
        }
    }
    
    public String workLeader(String clusterName, String pgsid) {
        // In Memory
        PartitionGroupServer pgs = pgsImo.get(pgsid, clusterName);
        RedisServer rs = rsImo.get(pgsid, clusterName);
        
        // Check
        if (pgs == null) {
            return "-ERR pgs does not exist";
        }
        
        if (rs == null) {
            return "-ERR rs does not exist";
        }

        StringBuilder sb = new StringBuilder();
        final PartitionGroupServer.RealState smrState = pgs.getRealState();
        final String rsState = rs.getRealState();

        PartitionGroupServerData pgsModified = 
            PartitionGroupServerData.builder().from(pgs.getData())
                .withRole(smrState.getRole())
                .withState(smrState.getState())
                .withStateTimestamp(smrState.getStateTimestamp()).build();
        RedisServerData rsModified = 
            RedisServerData.builder().from(rs.getData())
                .withState(rsState).build();

        try {
            pgsDao.updatePgs(pgs.getPath(), pgsModified);
            pgsDao.updateRs(rs.getPath(), rsModified);
            
            pgs.setData(pgsModified);
            rs.setData(rsModified);
        } catch (MgmtZooKeeperException e) {
            sb.append(", " + EXCEPTIONMSG_ZOOKEEPER);
        }
        
        try {
            pgs.updateHBRef();
        } catch (Exception e) {
            Logger.error(
                    "Update heartbeat-reference-data error. cluster: {}, pgs: {}, hb: {}",
                    new Object[]{clusterName, pgsid, pgs.getData().getHb()}, e);
            sb.append(", Update PGS heartbeat fail");
        }

        try {
            rs.updateHBRef();
        } catch (Exception e) {
            Logger.error(
                    "Update heartbeat-reference-data error. cluster: {}, pgs: {}, hb: {}",
                    new Object[]{clusterName, pgsid, pgs.getData().getHb()}, e);
            sb.append(", Update RS heartbeat fail");
        }

        pgs.syncLastState();

        if (sb.length() == 0) {
            return S2C_OK;
        } else {
            return "-ERR " + sb.substring(2);
        }
    }
    
    public String workFollower(String clusterName, String pgsid) {
        PartitionGroupServer pgs = pgsImo.get(pgsid, clusterName);
        if (pgs == null) {
            return "-ERR pgs does not exist";
        }
        
        pgs.syncLastState();
        return S2C_OK;
    }
    
    @LockMapping(name="pgs_sync")
    public void pgsSyncLock(HierarchicalLockHelper lockHelper,
            String clusterName, String pgsid) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(READ, null).pgs(WRITE, pgsid);
    }
    
}
