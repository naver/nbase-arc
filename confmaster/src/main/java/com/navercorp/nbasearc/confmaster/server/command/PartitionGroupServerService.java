/*
// * Copyright 2015 Naver Corp.
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

package com.navercorp.nbasearc.confmaster.server.command;

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;
import static com.navercorp.nbasearc.confmaster.server.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.server.lock.LockType.WRITE;
import static com.navercorp.nbasearc.confmaster.server.mapping.ArityType.GREATER;
import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.NULLABLE;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.ROLE_ADJUSTMENT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtDuplicatedReservedCallException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtInvalidQuorumPolicyException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtNoAvaliablePgsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.io.MultipleGatewayInvocator;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.GatewayLookup;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;
import com.navercorp.nbasearc.confmaster.server.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.server.lock.HierarchicalLockPGSList;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.Param;
import com.navercorp.nbasearc.confmaster.server.mapping.ClusterHint;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;
import com.navercorp.nbasearc.confmaster.server.workflow.DecreaseCopyWorkflow;
import com.navercorp.nbasearc.confmaster.server.workflow.IncreaseCopyWorkflow;
import com.navercorp.nbasearc.confmaster.server.workflow.QuorumAdjustmentWorkflow;
import com.navercorp.nbasearc.confmaster.server.workflow.StartOfTheEpochWorkflow;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowLogger;

@Service
public class PartitionGroupServerService {
    
    @Autowired
    private ApplicationContext context;

    @Autowired
    private WorkflowExecutor workflowExecutor;

    @Autowired
    private ZooKeeperHolder zk;
    @Autowired
    private Config config;
    @Autowired
    private ThreadPool executor;

    @Autowired
    private ClusterComponentContainer container;

    @Autowired
    private GatewayLookup gwInfoNotifier;
    @Autowired
    private WorkflowLogger workflowLogger;

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
    
    @CommandMapping(
            name="pgs_add",
            usage="pgs_add <cluster_name> <pgsid> <pgid> <pm_name> <pm_ip> <base_port> <backend_port>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String pgsAdd(@ClusterHint String clusterName, String pgsId, String pgId,
            String pmName, String pmIp, int basePort, int backendPort)
            throws MgmtZooKeeperException, NoNodeException {
        List<PartitionGroupServer> pgsList = container.getPgsList(clusterName);
        PhysicalMachine pm = container.getPm(pmName);

        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getName().equals(pgsId)) {
                throw new IllegalArgumentException("-ERR duplicated. cluster: "
                        + clusterName + ", pgs: " + pgsId);
            }
        }
        
        if (null == pm) {
            return "-ERR pm does not exist.";
        }
        
        if (!pm.getIp().equals(pmIp)) {
            return "-ERR pm ip does not match.";
        }

        Cluster cluster = container.getCluster(clusterName);
        if (null == cluster) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }
        
        if (null == container.getPm(pmName)) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PHYSICAL_MACHINE_DOES_NOT_EXIST
                            + PhysicalMachine.fullName(pmName));
        }

        PartitionGroup pg = container.getPg(clusterName, pgId);
        if (null == pg) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                            + PartitionGroup.fullName(clusterName, pgId));
        }

        // Do
        PartitionGroupServer pgs = new PartitionGroupServer(context, clusterName, pgsId, pgId, pmName, pmIp, basePort, backendPort, 0);
        RedisServer rs = new RedisServer(context, clusterName, pgsId, pmName, pmIp, backendPort, pgs.getPgId(), 0);
        createPgsObject(cluster, pg, pgs, rs);
        
        // Log
        workflowLogger.log(0, SEVERITY_MODERATE, 
                "PGSAddCommand", LOG_TYPE_COMMAND, 
                clusterName, "Add pgs success. " + pgs, 
                String.format("{\"cluster_name\":\"%s\",\"pgsid\":\"%s\",\"pgid\":\"%s\",\"host\":\"%s\",\"pm_ip\":\"%s\",\"base_port\":%d,\"backend_port\":%d}", 
                clusterName, pgsId, pgId, pmName, pmIp, basePort, backendPort));
        
        return S2C_OK; 
    }
    
    protected void createPgsObject(Cluster cluster,
            PartitionGroup pg, PartitionGroupServer pgs, RedisServer rs)
            throws NoNodeException, MgmtZooKeeperException {
        // DB
        PhysicalMachineCluster cim = null;
        PhysicalMachineCluster pmCluster = container.getPmc(pgs.getPmName(), cluster.getName());

        List<Op> ops = new ArrayList<Op>();

        ops.add(Op.create(pgs.getPath(), pgs.persistentDataToBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        ops.add(Op.create(rs.getPath(), rs.persistentDataToBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));

        if (null != pmCluster) {
            PhysicalMachineCluster.PmClusterData cimDataClon = pmCluster.clonePersistentData();
            cimDataClon.addPgsId(Integer.valueOf(pgs.getName()));
            ops.add(Op.setData(pmCluster.getPath(),
                    mapper.writeValueAsBytes(cimDataClon), -1));
        } else {
            cim = new PhysicalMachineCluster(cluster.getName(), pgs.getPmName());
            cim.addPgsId(Integer.valueOf(pgs.getName()));
            ops.add(Op.create(cim.getPath(), cim.persistentDataToBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        }
        
        PartitionGroup.PartitionGroupData pgModified = pg.clonePersistentData();
        pgModified.addPgsId(Integer.parseInt(pgs.getName()));
        
        ops.add(Op.setData(pg.getPath(), mapper.writeValueAsBytes(pgModified), -1));
        
        List<OpResult> results = zk.multi(ops);
        zk.handleResultsOfMulti(results);

        // Watcher
        zk.registerChangedEventWatcher(pgs.getPath());
        zk.registerChildEventWatcher(pgs.getPath());
        
        zk.registerChangedEventWatcher(rs.getPath());
        zk.registerChildEventWatcher(rs.getPath());
        
        // In memory
        container.put(pgs.getPath(), pgs);
        container.put(rs.getPath(), rs);
        
        pg.setPersistentData(pgModified);

        if (null != pmCluster) {
            pmCluster.addPgsId(Integer.valueOf(pgs.getName()));
        } else {
            container.put(cim.getPath(), cim);
        }
    }

    protected void deletePgsObject(String clusterName, PartitionGroupServer pgs)
            throws MgmtZooKeeperException {
        PartitionGroup pg = container.getPg(clusterName, String.valueOf(pgs.getPgId()));
        if (null == pg) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                            + PartitionGroup.fullName(clusterName,
                                    String.valueOf(pgs.getPgId())));
        }
        
        String pmName = pgs.getPmName();
        
        PhysicalMachineCluster pmCluster = container.getPmc(pgs.getPmName(), clusterName);
        if (null == pmCluster) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PHYSICAL_MACHINE_CLUSTER_DOES_NOT_EXIST
                            + PhysicalMachineCluster.fullName(pmName,
                                    clusterName));
        }
        
        // DB
        deletePgsZooKeeperNZnodes(pgs.getName(), clusterName, pg, pmCluster);
        // don't need to call deleteRs, because deletePgs handles it.
        
        // In Memory
        container.delete(pgs.getPath());
        container.delete(PathUtil.rsPath(pgs.getName(), clusterName));

        // PG
        PartitionGroup.PartitionGroupData pgModified = pg.clonePersistentData();
        pgModified.deletePgsId(Integer.valueOf(pgs.getName()));
        pg.setPersistentData(pgModified);

        // Delete PGS in Physical Machine
        final PhysicalMachineCluster clusterInPm = container.getPmc(pmName, clusterName);
        if (clusterInPm != null) {
            clusterInPm.deletePgsId(pgs.getName());
            if (clusterInPm.isEmpty()) {
                container.delete(PathUtil.pmClusterPath(clusterName, pmName));
            }
        }
    }

    /** 
     * 
     * SUBJECT 
     *   Q. Why deletePgs() method uses for-loop statement?
     *   A. Due to the synchronization problem for creating or deleting opinions 
     *      with pgs_leave and pgs_del
     *   
     * SCENARIO (remove an opinion) 
     * 
     *         PGS
     *        /   \            OP1 : Leader's
     *       /     \           OP2 : Follower's
     *      OP1    OP2
     *    
     *        LEADER                          FOLLOWER
     *   1.   pgs_leave                       
     *   2.   pgs_del                         get a watch event of pgs_leave
     *   2.1. get children of PGS z-node.
     *   2.2.                                 delete an opinion, OP2
     * F 2.3. (fail) delete the children.
     *
     * SCENARIO (put an opinion)
     * 
     *         PGS
     *        /                OP1 : Leader's
     *       /    
     *      OP1    
     *       
     *        LEADER                          FOLLOWER
     *   1.   pgs_leave                       
     *   2.   pgs_del                         
     *   2.1. get children of PGS z-node.
     *   2.2.                                 put an opinion, OP2
     *   2.3. delete the children.
     *         PGS
     *            \            
     *             \           OP2 : Follower's
     *             OP2
     * F 2.4. (fail) delete the PGS z-node.
     *   2.5.                                 get a watch event of pgs_leave
     *
     * But, eventually good things will happen.
     * 
     * @Return Returns the number of retries if successful.
     */
    protected int deletePgsZooKeeperNZnodes(String name, String clusterName,
            PartitionGroup pg, PhysicalMachineCluster pmCluster)
            throws MgmtZooKeeperException {

        final String path = PathUtil.pgsPath(name, clusterName);
        final String pathForRs = PathUtil.rsPath(name, clusterName);
        final int MAX = config.getServerCommandPgsdelMaxretry();
        int retryCnt;

        for (retryCnt = 1; retryCnt <= MAX; retryCnt++) {
            // Delete children(opinions) of PGS
            try {
                zk.deleteChildren(path);
            } catch (MgmtZooKeeperException e) {
                if (e.getCause() instanceof KeeperException.NoNodeException
                        && retryCnt != MAX) {
                    // Retry
                    Logger.info("Delete children of {} fail. retry {}",
                            PartitionGroupServer.fullName(clusterName, name),
                            retryCnt, e);
                    continue;
                } else {
                    throw e;
                }
            }

            // Delete children(opinions) of RS
            try {
                zk.deleteChildren(pathForRs);
            } catch (MgmtZooKeeperException e) {
                if (e.getCause() instanceof KeeperException.NoNodeException
                        && retryCnt != MAX) {
                    // Retry
                    Logger.info("Delete children of {} fail. retry {}",
                            RedisServer.fullName(clusterName, name), retryCnt,
                            e);
                    continue;
                } else {
                    throw e;
                }
            }

            // Delete PGS and RS & Update PG
            List<Op> ops = new ArrayList<Op>();
            ops.add(Op.delete(path, -1));
            ops.add(Op.delete(pathForRs, -1));

            PhysicalMachineCluster.PmClusterData cimData = pmCluster.clonePersistentData();
            cimData.deletePgsId(Integer.valueOf(name));
            byte cimDataOfBytes[] = mapper.writeValueAsBytes(cimData);
            ops.add(Op.setData(pmCluster.getPath(), cimDataOfBytes, -1));

            PartitionGroup.PartitionGroupData pgModified = pg.clonePersistentData();
            pgModified.deletePgsId(Integer.parseInt(name));

            byte pgDataOfBytes[] = mapper.writeValueAsBytes(pgModified);
            ops.add(Op.setData(pg.getPath(), pgDataOfBytes, -1));

            if (cimData.getPgsIdList().isEmpty()
                    && cimData.getGwIdList().isEmpty()) {
                ops.add(Op.delete(pmCluster.getPath(), -1));
            }

            try {
                List<OpResult> results = zk.multi(ops);
                zk.handleResultsOfMulti(results);
                return retryCnt;
            } catch (MgmtZooKeeperException e) {
                if (e.getCause() instanceof KeeperException.NotEmptyException
                        && retryCnt != MAX) {
                    // Retry
                    Logger.info("Delete {} fail. retry {}",
                            PartitionGroupServer.fullName(clusterName, name),
                            retryCnt, e);
                } else {
                    throw e;
                }
            }
        }

        return retryCnt;
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
            usage="pgs_del <cluster_name> <pgsid>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String pgsDel(@ClusterHint String clusterName, String pgsId) throws MgmtZooKeeperException {
        // Check & Cache
        Cluster cluster = container.getCluster(clusterName);
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        PartitionGroupServer pgs = container.getPgs(clusterName, pgsId);
        if (null == pgs) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST + "pgs:"
                            + pgsId);
        }
        
        if (pgs.getHeartbeat().equals(HB_MONITOR_YES)) {
            return "-ERR pgs has been joined yet.";
        }

        deletePgsObject(clusterName, pgs);
        
        // Log
        workflowLogger.log(0, SEVERITY_MODERATE, 
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
        PartitionGroupServer pgs = container.getPgs(clusterName, pgsid);
        lockHelper.pmList(READ).pm(WRITE, pgs.getPmName());
    }

    @CommandMapping(
            name="pgs_info_all",
            usage="pgs_info_all <cluster_name> <pgs_id>\r\n" +
                    "get all information of a Partition Group Server",
            requiredState=ConfMaster.READY,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public String pgsInfoAll(@ClusterHint String clusterName, String pgsid) throws InterruptedException {
        // In Memory
        PartitionGroupServer pgs = container.getPgs(clusterName, pgsid);
        Cluster cluster = container.getCluster(clusterName);
        
        // Check
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        if (null == pgs) {
            return "-ERR pgs does not exist.";
        }

        // Do
        try {
            String data = pgs.persistentDataToString();
            String extraData = pgs.getHeartbeatState().toString();
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
                    "get information of a Partition Group Server",
            requiredState=ConfMaster.READY,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public String pggInfo(@ClusterHint String clusterName, String pgsid) throws InterruptedException {
        // In Memory
        Cluster cluster = container.getCluster(clusterName);
        PartitionGroupServer pgs = container.getPgs(clusterName, pgsid);
        
        // Check
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        if (null == pgs) {
            return "-ERR pgs does not exist.";
        }

        // Do
        try {
            return pgs.persistentDataToString();
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
            usage="pgs_join <cluster_name> <pgsid>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String pgsJoin(@ClusterHint String clusterName, String pgsid) {
        // In Memory
        PartitionGroupServer pgs = container.getPgs(clusterName, pgsid);
        Logger.info("pgs_join {} HB: {}", pgs, pgs.getHeartbeat());
        if (pgs.getHeartbeat().equals(HB_MONITOR_YES)) {
            return "-ERR pgs already joined";
        }
        
        RedisServer rs = container.getRs(clusterName, pgsid);
        if (rs.getHeartbeat().equals(HB_MONITOR_YES)) {
            return "-ERR rs already joined";
        }
        
        Cluster cluster = container.getCluster(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }

        PartitionGroup pg = container.getPg(clusterName, String.valueOf(pgs.getPgId()));
        if (pg == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                            + PartitionGroup.fullName(clusterName,
                                    String.valueOf(pgs.getPgId())));
        }
        
        PhysicalMachineCluster pmCluster = container.getPmc(pgs.getPmName(), clusterName);
        if (pmCluster == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PHYSICAL_MACHINE_CLUSTER_DOES_NOT_EXIST
                            + PhysicalMachineCluster.fullName(pgs.getPmName(), clusterName));
        }
        
        List<Gateway> gwList = container.getGwList(clusterName);

        // Do
        String reply = cluster.isGatewaysAlive();
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
        
        try {
            if (pg.currentGen() == -1 && pg.getCopy() == 0) {
                StartOfTheEpochWorkflow se = new StartOfTheEpochWorkflow(pg, pgs, context);
                se.execute();
            } else {
                IncreaseCopyWorkflow ic = new IncreaseCopyWorkflow(pgs, pg, context);
                ic.execute();
            }
        } catch (Exception e) {
            return EXCEPTIONMSG_ZOOKEEPER;
        }

        String cmd = String.format("pgs_add %s %d %s %d", 
                pgsid, pgs.getPgId(), 
                pgs.getPmIp(), 
                pgs.getRedisPort());
        reply = broadcast.request(clusterName, gwList, cmd, GW_RESPONSE_OK, executor);
        if (null != reply) {
            return reply;
        }

        // Log
        workflowLogger.log(0, SEVERITY_MODERATE, 
                "PGSJoinCommand", LOG_TYPE_COMMAND, 
                clusterName, "Join pgs success. " + pgs, 
                String.format("{\"cluster_name\":\"%s\",\"pgsid\":\"%s\"}", clusterName, pgsid));
        
        return S2C_OK;
    }

    @LockMapping(name="pgs_join")
    public void pgsJoinLock(HierarchicalLockHelper lockHelper,
            String clusterName, String pgsid) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(READ)
                .pg(WRITE, null).pgs(WRITE, pgsid)
                .gwList(READ).gw(WRITE, ALL);
    }
    
    @CommandMapping(
            name="pgs_lconn",
            usage="pgs_lconn <cluster_name> <pgsid>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String pgsLconn(@ClusterHint String clusterName, String pgsid)
            throws NodeExistsException, MgmtZooKeeperException,
            MgmtDuplicatedReservedCallException, IOException {
        Cluster cluster = container.getCluster(clusterName);
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        PartitionGroupServer pgs = container.getPgs(clusterName, pgsid);
        if (pgs.getHeartbeat().equals(HB_MONITOR_YES)) {
            return "-ERR pgs does not left yet";
        }
        
        RedisServer rs = container.getRs(clusterName, pgsid);
        if (rs.getHeartbeat().equals(HB_MONITOR_YES)) {
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

        PartitionGroup pg = container.getPg(clusterName, String.valueOf(pgs.getPgId()));
        List<PartitionGroupServer> pgsList = pg.getJoinedPgsList(
                container.getPgsList(clusterName, String.valueOf(pgs.getPgId())));
        
        if (pgs.getRole().equals(PGS_ROLE_MASTER)) {
            // in order to have confmaster put an opinion quickly
            for (PartitionGroupServer pgsInPg : pgsList) {
                String path = String.format("%s/%s_%s", pgsInPg.getPath(), 
                        TEMP_ZNODE_NAME_FOR_CHILDEVENT, config.getIp() + ":" + config.getPort());                
                zk.createEphemeralZNode(path);
                zk.deleteZNode(path, -1);
            }

            workflowExecutor.performContextContinue(ROLE_ADJUSTMENT,
                    pg, pg.nextWfEpoch(), context);
        }
        
        PartitionGroupServer.PartitionGroupServerData pgsModified = pgs.clonePersistentData();
        pgsModified.setRole(PGS_ROLE_NONE);
        zk.setData(pgs.getPath(), pgsModified.toBytes(), -1);
        pgs.setPersistentData(pgsModified);
        
        return null;
    }
    
    @LockMapping(name="pgs_lconn")
    public void pgsLconnLock(HierarchicalLockHelper lockHelper,
            String clusterName, String pgsid) {
        HierarchicalLockPGSList lock = lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(READ);
        PartitionGroupServer pgs = container.getPgs(clusterName, pgsid);
        lock.pg(WRITE, String.valueOf(pgs.getPgId())).pgs(WRITE, ALL_IN_PG);
    }

    @CommandMapping(
            name="pgs_leave",
            arityType=GREATER,
            usage="pgs_leave <cluster_name> <pgsid>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String pgsLeave(@ClusterHint String clusterName, String pgsid,
            @Param(type = NULLABLE) String mode) throws Exception {
        // In Memory
        Cluster cluster = container.getCluster(clusterName);
        if (null == cluster) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }        

        PartitionGroupServer pgs = container.getPgs(clusterName, pgsid);
        if (null == pgs) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST 
                            + PartitionGroupServer.fullName(clusterName, pgsid));
        }
        Logger.info("pgs_leave {} HB: {}", pgs, pgs.getHeartbeat());

        RedisServer rs = container.getRs(clusterName, pgsid);
        if (null == rs) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST 
                            + RedisServer.fullName(clusterName, pgsid));
        }
        PartitionGroup pg = container.getPg(clusterName, String.valueOf(pgs.getPgId()));
        if (null == pg) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                            + PartitionGroup.fullName(clusterName, 
                                String.valueOf(pgs.getPgId())));
        }

        List<Gateway> gwList = container.getGwList(clusterName);
        
        // Check
        if (pgs.getHeartbeat().equals(HB_MONITOR_NO)) {
            return "-ERR pgs already left";
        }
        
        if (rs.getHeartbeat().equals(HB_MONITOR_NO)) {
            return "-ERR rs already left";
        }
        
        // Do
        String reply = cluster.isGatewaysAlive();
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
        
        List<PartitionGroupServer> joinedPgsList = pg
                .getJoinedPgsList(container.getPgsList(pg.getClusterName(), pg.getName()));
        DecreaseCopyWorkflow dc = new DecreaseCopyWorkflow(pgs, pg, mode, context);
        try {
            dc.execute();
        } catch (MgmtZooKeeperException e) {
            Logger.error("pgs_leave fail. {}", pgs, e);
            return EXCEPTIONMSG_ZOOKEEPER;
        } catch (MgmtNoAvaliablePgsException e) {
            Logger.error("pgs_leave fail {}, PG.COPY: {}, PG.D: {}", 
                    new Object[]{pgs, pg.getCopy(), pg.getD(joinedPgsList)}, e);
            return EXCEPTIONMSG_NO_AVAILABLE_PGS;
        } catch (MgmtInvalidQuorumPolicyException e) {
            return ERROR + " " + e.getMessage();
        }

        joinedPgsList.remove(pgs);
        QuorumAdjustmentWorkflow qa = new QuorumAdjustmentWorkflow(pg, true,
                context);
        qa.execute();
        
        String cmd = String.format("pgs_del %s %d", pgsid, pgs.getPgId());
        reply = broadcast.request(clusterName, gwList, cmd, GW_RESPONSE_OK, executor);
        if (null != reply) {
            Logger.error(reply);
            return reply;
        }

        StringBuilder errMsg = new StringBuilder();
        try {
            pgs.propagateStateToHeartbeatSession();
        } catch (Exception e) {
            Logger.error(
                    "Update heartbeat-reference-data error. cluster: {}, pgs: {}, hb: {}",
                    new Object[]{clusterName, pgsid, pgs.getHeartbeat()}, e);
            errMsg.append("update PGS heartbeat fail");
        }

        try {
            rs.propagateStateToHeartbeatSession();
        } catch (Exception e) {
            Logger.error(
                    "Update heartbeat-reference-data error. cluster: {}, rs: {}, hb: {}",
                    new Object[]{clusterName, pgsid, pgs.getHeartbeat()}, e);
            errMsg.append(", update RS heartbeat fail");
        }
        
        if (errMsg.length() == 0) {
            workflowLogger.log(0, SEVERITY_MODERATE, 
                    "PGSLeaveCommand", LOG_TYPE_COMMAND, 
                    clusterName, "pgs was left.", 
                    String.format("{\"cluster_name\":\"%s\",\"pgsid\":\"%s\",\"error\":\"\"}", clusterName, pgsid));
            return S2C_OK;
        } else {
            workflowLogger.log(0, SEVERITY_MODERATE, 
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
                .pgsList(READ).pg(WRITE, null).pgs(WRITE, pgsid).gwList(READ)
                .gw(WRITE, ALL);
    }

    @CommandMapping(
            name="pgs_ls",
            usage="pgs_ls <cluster_name>\r\n" +
                    "show a list of Partition Group Servers",
            requiredState=ConfMaster.READY,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public String execute(@ClusterHint String clusterName) {
        // In Memory
        Cluster cluster = container.getCluster(clusterName);
        List<PartitionGroupServer> pgsList = container.getPgsList(clusterName);

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
            usage="pgs_sync <cluster_name> <pgsid> <mode>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String pgsSync(@ClusterHint String clusterName, String pgsid, String mode) {
        Cluster cluster = container.getCluster(clusterName);
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        if (LeaderState.isLeader()) {
            return workLeader(cluster, pgsid, mode);
        } else {
            return workFollower(cluster, pgsid);
        }
    }
    
    public String workLeader(Cluster cluster, String pgsid, String mode) {
        if (!mode.equals(FORCED)) {
            return EXCEPTIONMSG_NOT_FORCED_MODE;
        }
        
        // In Memory
        PartitionGroupServer pgs = container.getPgs(cluster.getName(), pgsid);
        RedisServer rs = container.getRs(cluster.getName(), pgsid);
        
        // Check
        if (pgs == null) {
            return "-ERR pgs does not exist";
        }
        
        if (rs == null) {
            return "-ERR rs does not exist";
        }

        StringBuilder sb = new StringBuilder();
        final PartitionGroupServer.RealState smrState = pgs.getRealState();
        final String rsState = rs.replPing();
        
        Color color = YELLOW;
        if ((smrState.getRole().equals(PGS_ROLE_MASTER) || smrState.getRole().equals(PGS_ROLE_SLAVE)) 
                && rsState.equals(SERVER_STATE_NORMAL)) {
            color = GREEN;
        }

        PartitionGroupServer.PartitionGroupServerData pgsModified = pgs.clonePersistentData();
        pgsModified.setRole(smrState.getRole());
        pgsModified.color = color;
        pgsModified.stateTimestamp = smrState.getStateTimestamp();
        RedisServer.RedisServerData rsModified = rs.clonePersistentData();
        rsModified.state = rsState;

        try {
            zk.setData(pgs.getPath(), pgsModified.toBytes(), -1);
            zk.setData(rs.getPath(), rsModified.toBytes(), -1);
            
            pgs.setPersistentData(pgsModified);
            rs.setPersistentData(rsModified);
        } catch (MgmtZooKeeperException e) {
            sb.append(", " + EXCEPTIONMSG_ZOOKEEPER);
        }
        
        try {
            pgs.propagateStateToHeartbeatSession();
        } catch (Exception e) {
            Logger.error(
                    "Update heartbeat-reference-data error. cluster: {}, pgs: {}, hb: {}",
                    new Object[]{cluster.getName(), pgsid, pgs.getHeartbeat()}, e);
            sb.append(", Update PGS heartbeat fail");
        }

        try {
            rs.propagateStateToHeartbeatSession();
        } catch (Exception e) {
            Logger.error(
                    "Update heartbeat-reference-data error. cluster: {}, pgs: {}, hb: {}",
                    new Object[]{cluster.getName(), pgsid, pgs.getHeartbeat()}, e);
            sb.append(", Update RS heartbeat fail");
        }

        pgs.syncLastState();

        if (sb.length() == 0) {
            return S2C_OK;
        } else {
            return "-ERR " + sb.substring(2);
        }
    }
    
    public String workFollower(Cluster cluster, String pgsid) {
        PartitionGroupServer pgs = container.getPgs(cluster.getName(), pgsid);
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
