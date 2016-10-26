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

package com.navercorp.nbasearc.confmaster.server.command;

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;
import static com.navercorp.nbasearc.confmaster.server.mapping.ArityType.GREATER;
import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.NULLABLE;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.ROLE_ADJUSTMENT;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtDuplicatedReservedCallException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtNoAvaliablePgsException;
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
import com.navercorp.nbasearc.confmaster.server.mapping.Param;
import com.navercorp.nbasearc.confmaster.server.mapping.ParamClusterHint;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;
import com.navercorp.nbasearc.confmaster.server.workflow.DecreaseCopyWorkflow;
import com.navercorp.nbasearc.confmaster.server.workflow.IncreaseCopyWorkflow;
import com.navercorp.nbasearc.confmaster.server.workflow.QuorumAdjustmentWorkflow;
import com.navercorp.nbasearc.confmaster.server.workflow.StartOfTheEpochWorkflow;

@Service
public class PartitionGroupServerService {
    
    @Autowired
    private ApplicationContext context;

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
            usage="pgs_add <cluster_name> <pgsid> <pgid> <pm_name> <pm_ip> <base_port> <backend_port>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String pgsAdd(@ParamClusterHint String clusterName, String pgsId, String pgId,
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

        // Do
        PartitionGroupServerData data = new PartitionGroupServerData();
        data.initialize(
                Integer.parseInt(pgId), pmName, pmIp, backendPort,
                basePort, basePort + 3, SERVER_STATE_FAILURE,
                PGS_ROLE_NONE, Color.RED, -1,
                HB_MONITOR_NO);
        
        PartitionGroupServer pgs = createPgsObject(pmName, cluster, pg, pgsId, data);
        
        // Log
        workflowLogDao.log(0, SEVERITY_MODERATE, 
                "PGSAddCommand", LOG_TYPE_COMMAND, 
                clusterName, "Add pgs success. " + pgs, 
                String.format("{\"cluster_name\":\"%s\",\"pgsid\":\"%s\",\"pgid\":\"%s\",\"host\":\"%s\",\"pm_ip\":\"%s\",\"base_port\":%d,\"backend_port\":%d}", 
                clusterName, pgsId, pgId, pmName, pmIp, basePort, backendPort));
        
        return S2C_OK; 
    }
    
    protected PartitionGroupServer createPgsObject(String pmName, Cluster cluster,
            PartitionGroup pg, String pgsId, PartitionGroupServerData data)
            throws NoNodeException, MgmtZooKeeperException {
        // DB
        PhysicalMachineCluster pmCluster = pmClusterImo.get(cluster.getName(), pmName);
        pgsDao.createPgs(pgsId, cluster.getName(), data, pg, pmCluster);

        PartitionGroupServer pgs = pgsImo.load(pgsId, cluster);
        rsImo.load(pgsId, cluster);

        // Add to pg
        PartitionGroupData pgModified = PartitionGroupData.builder()
                .from(pg.getData()).addPgsId(Integer.valueOf(pgsId)).build();
        pg.setData(pgModified);

        // Add to pm-cluster
        if (null != pmCluster) {
            pmCluster.getData().addPgsId(Integer.valueOf(pgsId));
        } else {
            pmClusterImo.load(cluster.getName(), pmName);
        }
        
        return pgs;
    }

    protected void deletePgsObject(String clusterName, PartitionGroupServer pgs)
            throws MgmtZooKeeperException {
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
        pgsDao.deletePgs(pgs.getName(), clusterName, pg, pmCluster);
        // don't need to call deleteRs, because deletePgs handles it. TODO : explicitly delete rs
        
        // In Memory
        pgsImo.delete(pgs.getName(), clusterName);
        rsImo.delete(pgs.getName(), clusterName);

        // PG
        PartitionGroupData pgModified = 
            PartitionGroupData.builder().from(pg.getData())
                .deletePgsId(Integer.valueOf(pgs.getName())).build();
        pg.setData(pgModified);

        // Delete PGS in Physical Machine
        final PhysicalMachineCluster clusterInPm = pmClusterImo.get(clusterName, pmName);
        if (clusterInPm != null) {
            clusterInPm.getData().deletePgsId(pgs.getName());
            if (clusterInPm.isEmpty()) {
                pmClusterImo.delete(PathUtil.pmClusterPath(clusterName, pmName));
            }
        }
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
    public String pgsDel(@ParamClusterHint String clusterName, String pgsId) throws MgmtZooKeeperException {
        // Check & Cache
        Cluster cluster = clusterImo.get(clusterName);
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        PartitionGroupServer pgs = pgsImo.get(pgsId, clusterName);
        if (null == pgs) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST + "pgs:"
                            + pgsId);
        }
        
        if (pgs.getData().getHb().equals(HB_MONITOR_YES)) {
            return "-ERR pgs has been joined yet.";
        }

        deletePgsObject(clusterName, pgs);
        
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
                    "get all information of a Partition Group Server",
            requiredState=ConfMaster.READY,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public String pgsInfoAll(@ParamClusterHint String clusterName, String pgsid) throws InterruptedException {
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
                    "get information of a Partition Group Server",
            requiredState=ConfMaster.READY,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public String pggInfo(@ParamClusterHint String clusterName, String pgsid) throws InterruptedException {
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
            usage="pgs_join <cluster_name> <pgsid>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String pgsJoin(@ParamClusterHint String clusterName, String pgsid) {
        // In Memory
        PartitionGroupServer pgs = pgsImo.get(pgsid, clusterName);
        Logger.info("pgs_join {} HB: {}", pgs, pgs.getData().getHb());
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
        
        try {
            if (pg.getData().currentGen() == -1 && pg.getData().getCopy() == 0) {
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
            usage="pgs_lconn <cluster_name> <pgsid>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String pgsLconn(@ParamClusterHint String clusterName, String pgsid)
            throws NodeExistsException, MgmtZooKeeperException,
            MgmtDuplicatedReservedCallException, IOException {
        Cluster cluster = clusterImo.get(clusterName);
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
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

        PartitionGroup pg = pgImo.get(String.valueOf(pgs.getData().getPgId()), clusterName);
        List<PartitionGroupServer> pgsList = pg.getJoinedPgsList(
        		pgsImo.getList(clusterName,  pgs.getData().getPgId()));
        
        if (pgs.getData().getRole().equals(PGS_ROLE_MASTER)) {
            // in order to have confmaster put an opinion quickly
            for (PartitionGroupServer pgsInPg : pgsList) {
                String path = String.format("%s/%s_%s", pgsInPg.getPath(), 
                		TEMP_ZNODE_NAME_FOR_CHILDEVENT, config.getIp() + ":" + config.getPort());                
                zookeeper.createEphemeralZNode(path);
                zookeeper.deleteZNode(path, -1);
            }

            workflowExecutor.performContextContinue(ROLE_ADJUSTMENT,
                    pg, pg.nextWfEpoch(), context);
        }
        
        PartitionGroupServerData pgsModified = 
                PartitionGroupServerData.builder().from(pgs.getData())
                    .withRole(PGS_ROLE_NONE).build();
        pgsDao.updatePgs(pgs.getPath(), pgsModified);
        pgs.setData(pgsModified);
        
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
            arityType=GREATER,
            usage="pgs_leave <cluster_name> <pgsid>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String pgsLeave(@ParamClusterHint String clusterName, String pgsid,
            @Param(type = NULLABLE) String mode) throws Exception {
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
        Logger.info("pgs_leave {} HB: {}", pgs, pgs.getData().getHb());

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
        
		List<PartitionGroupServer> joinedPgsList = pg
				.getJoinedPgsList(pgsImo.getList(pg.getClusterName(), Integer.valueOf(pg.getName())));
		DecreaseCopyWorkflow dc = new DecreaseCopyWorkflow(pgs, pg, mode, context);
		try {
			dc.execute();
		} catch (MgmtZooKeeperException e) {
            Logger.error("pgs_leave fail. {}", pgs, e);
            return EXCEPTIONMSG_ZOOKEEPER;
		} catch (MgmtNoAvaliablePgsException e) {
			Logger.error("pgs_leave fail {}, PG.COPY: {}, PG.D: {}", 
					new Object[]{pgs, pg.getData().getCopy(), pg.getD(joinedPgsList)}, e);
			return EXCEPTIONMSG_NO_AVAILABLE_PGS;
		}

		joinedPgsList.remove(pgs);
        QuorumAdjustmentWorkflow qa = new QuorumAdjustmentWorkflow(pg, true,
                context);
		qa.execute();
        
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
                .pgsList(READ).pg(WRITE, null).pgs(WRITE, pgsid).gwList(READ)
                .gw(WRITE, ALL);
    }

    @CommandMapping(
            name="pgs_ls",
            usage="pgs_ls <cluster_name>\r\n" +
                    "show a list of Partition Group Servers",
            requiredState=ConfMaster.READY,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public String execute(@ParamClusterHint String clusterName) {
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
            usage="pgs_sync <cluster_name> <pgsid> <mode>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String pgsSync(@ParamClusterHint String clusterName, String pgsid, String mode) {
        Cluster cluster = clusterImo.get(clusterName);
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        if (LeaderState.isLeader()) {
            return workLeader(clusterName, pgsid, mode);
        } else {
            return workFollower(clusterName, pgsid);
        }
    }
    
    public String workLeader(String clusterName, String pgsid, String mode) {
        if (!mode.equals(FORCED)) {
            return EXCEPTIONMSG_NOT_FORCED_MODE;
        }
        
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
        final String rsState = rs.replPing();
        
        Color color = YELLOW;
        if ((smrState.getRole().equals(PGS_ROLE_MASTER) || smrState.getRole().equals(PGS_ROLE_SLAVE)) 
                && rsState.equals(SERVER_STATE_NORMAL)) {
            color = GREEN;
        }

        PartitionGroupServerData pgsModified = 
            PartitionGroupServerData.builder().from(pgs.getData())
                .withRole(smrState.getRole())
                .withColor(color)
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
