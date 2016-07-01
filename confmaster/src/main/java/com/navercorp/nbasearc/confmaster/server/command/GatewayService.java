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

import static com.navercorp.nbasearc.confmaster.Constant.ALL;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_GATEWAY_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_PHYSICAL_MACHINE_CLUSTER_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_PHYSICAL_MACHINE_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_YES;
import static com.navercorp.nbasearc.confmaster.Constant.LOG_TYPE_COMMAND;
import static com.navercorp.nbasearc.confmaster.Constant.S2C_OK;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_FAILURE;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_MODERATE;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.ConfMasterException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.dao.GatewayDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkNotificationDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkWorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockPMList;
import com.navercorp.nbasearc.confmaster.repository.znode.GatewayData;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.GatewayImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineImo;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;

@Service
public class GatewayService {

    @Autowired
    private ClusterImo clusterImo;
    @Autowired
    private GatewayImo gwImo;
    @Autowired
    private PhysicalMachineImo pmImo;
    @Autowired
    private PhysicalMachineClusterImo pmClusterImo;
    
    @Autowired
    private GatewayDao gwDao;
    @Autowired
    private ZkNotificationDao notificationDao;
    @Autowired
    private ZkWorkflowLogDao workflowLogDao;

    @CommandMapping(
            name="gw_add",
            usage="gw_add <cluster_name> <gwid> <pm_name> <pm_ip> <port>",
            requiredState=ConfMaster.RUNNING)
    public String gwAdd(String clusterName, String gwId, String pmName,
            String pmIp, Integer port) throws MgmtZooKeeperException,
            NoNodeException {
        // In Memory
        List<Gateway> gwList = gwImo.getList(clusterName);
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

        // Do not check... if pmCluster is null than it will make it 
        PhysicalMachineCluster pmCluster = pmClusterImo.get(clusterName, pmName);
        
        // Prepare
        GatewayData data = new GatewayData();

        for (Gateway gw : gwList) {
            if (gw.getName().equals(gwId)) {
                throw new IllegalArgumentException(
                        "-ERR duplicated. cluster: " + clusterName + ", gw: " + gwId);
            }
        }

        data.initialize(pmName, pmIp, port, SERVER_STATE_FAILURE, HB_MONITOR_YES);

        // DB
        gwDao.createGw(gwId, data, clusterName, pmName, pmCluster);
        
        // In Memory
        Gateway gw = gwImo.load(gwId, clusterName);
        
        // Physical Machine Cluster info
        PhysicalMachineCluster clusterInPm = pmClusterImo.get(clusterName, pmName); 
        if (null == clusterInPm) {
            pmClusterImo.load(clusterName, pmName);
        } else {
            clusterInPm.getData().addGwId(Integer.valueOf(gwId));
        }
        
        // Update gateway affinity
        notificationDao.updateGatewayAffinity(cluster);

        // TODO
        workflowLogDao.log(0, SEVERITY_MODERATE, 
                "GWAddCommand", LOG_TYPE_COMMAND, 
                clusterName, "Add gateway success. " + gw, 
                String.format("{\"cluster_name\":\"%s\",\"gw_id\":\"%s\",\"host\":\"%s\",\"pm_ip\":\"%s\",\"port\":%d}", 
                        clusterName, gwId, pmName, pmIp, port));

        return S2C_OK;
    }
    
    @LockMapping(name="gw_add")
    public void gwAddLock(HierarchicalLockHelper lockHelper, String clusterName,
            String gwid, String host) {
        lockHelper.root(READ).cluster(READ, clusterName).gwList(WRITE);
        lockHelper.pmList(READ).pm(WRITE, host);
    }
    
    @CommandMapping(
            name="gw_affinity_sync",
            usage="gw_affinity_sync <cluster_name>",
            requiredState=ConfMaster.RUNNING)
    public String gwAffinitySync(String clusterName) throws MgmtZooKeeperException {
        Cluster cluster = clusterImo.get(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }
        
        return notificationDao.updateGatewayAffinity(cluster);
    }
    
    @LockMapping(name="gw_affinity_sync")
    public void gwAffinitySyncLock(HierarchicalLockHelper lockHelper, String clusterName) {
        lockHelper.root(READ).cluster(WRITE, clusterName).gwList(READ).gw(READ, ALL);
    }
    
    @CommandMapping(
            name="gw_del",
            usage="gw_del <cluster_name> <gwid>",
            requiredState=ConfMaster.RUNNING)
    public String gwDel(String clusterName, String gwId)
            throws MgmtZNodeDoesNotExistException, ConfMasterException,
            MgmtZooKeeperException {
        // Check
        Cluster cluster = clusterImo.get(clusterName);
        if (null == cluster) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }
        
        Gateway gw = gwImo.get(gwId, clusterName);
        if (null == gw) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_GATEWAY_DOES_NOT_EXIST
                            + Gateway.fullName(clusterName, gwId));
        }

        String pmName = gw.getData().getPmName();
        PhysicalMachineCluster pmCluster = pmClusterImo.get(clusterName, pmName);
        if (null == pmCluster) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PHYSICAL_MACHINE_CLUSTER_DOES_NOT_EXIST
                            + PhysicalMachineCluster.fullName(pmName,
                                    clusterName));
        }
        
        // DB
        gwDao.deleteGw(gwId, clusterName, pmCluster);

        // In Memory
        gwImo.delete(gwId, clusterName);
        
        // Update gateway affinity
        notificationDao.updateGatewayAffinity(cluster);
        
        // Delete from pm-cluster
        pmCluster.getData().deleteGwId(gwId);
        if (pmCluster.isEmpty()) {
            pmClusterImo.delete(PathUtil.pmClusterPath(clusterName, pmName));
        }

        // Log
        workflowLogDao.log(0, SEVERITY_MODERATE, 
                "GWDelCommand", LOG_TYPE_COMMAND, 
                clusterName, "Delete gateway success. " + gw, 
                String.format("{\"cluster_name\":\"%s\",\"gw_id\":\"%s\"}", clusterName, gwId));
        
        return S2C_OK;
    }

    @LockMapping(name="gw_del")
    public void gwDelLock(HierarchicalLockHelper lockHelper,
            String clusterName, String gwid) {
        lockHelper.root(READ).cluster(READ, clusterName).gwList(WRITE).gw(WRITE, gwid);
        HierarchicalLockPMList lockPMList = lockHelper.pmList(READ);
        Gateway gw = gwImo.get(gwid, clusterName);
        lockPMList.pm(WRITE, gw.getData().getPmName());
    }
    
    @CommandMapping(
            name="gw_info",
            usage="gw_info <cluster_name> <gw_id>\r\n" +
                    "get information of a Gateway",
            requiredState=ConfMaster.READY)
    public String gwInfo(String clusterName, String gwid)
            throws KeeperException, InterruptedException, IOException {
        Cluster cluster = clusterImo.get(clusterName);
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        if (gwid.equals(ALL)) {
            return allGatewaysInfo(cluster);
        } else {
            return gatewayInfo(cluster, gwid);
        }
    }

    private String allGatewaysInfo(Cluster cluster) throws KeeperException,
            InterruptedException, IOException {
        // In Memory
        List<Gateway> gwList = gwImo.getList(cluster.getName());
        
        // Prepare 
        StringBuffer sb = new StringBuffer();
        sb.append("{\"gw_list\":[");

        for (Gateway gw : gwList) {
            sb.append(gw.getData().toString());
            sb.append(",");
        }
        
        if (!gwList.isEmpty()) {
            sb.delete(sb.length() - 1, sb.length()).append("]}");
        } else {
            sb.append("]}");
        }
        
        return sb.toString();
    }
    
    private String gatewayInfo(Cluster cluster, String gwid) {
        Gateway gw = gwImo.get(gwid, cluster.getName());
        if (null == gw) {
            return "-ERR gw does not exist.";
        }

        try {
            return gw.getData().toString();
        } catch (RuntimeException e) {
            return "-ERR internal data of pgs is not correct.";
        }
    }
    
    @LockMapping(name="gw_info")
    public void gwInfoLock(HierarchicalLockHelper lockHelper, String clusterName, String gwid) {
        lockHelper.root(READ).cluster(READ, clusterName).gwList(READ).gw(READ, gwid);
    }
    
    @CommandMapping(
            name="gw_ls",
            usage="gw_ls <cluster_name>\r\n" +
                    "show a list of Gateways",
            requiredState=ConfMaster.READY)
    public String gwLs(String clusterName) {
        // Check
        if (null == clusterImo.get(clusterName)) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }

        // In Memory
        List<Gateway> gwList = gwImo.getList(clusterName);
        
        // Prepare
        StringBuilder reply = new StringBuilder();
        reply.append("{\"list\":[");
        for (Gateway gw : gwList) {
            reply.append("\"").append(gw.getName()).append("\", ");
        }
        if (!gwList.isEmpty()) {
            reply.delete(reply.length() - 2, reply.length()).append("]}");
        } else {
            reply.append("]}");
        }
        return reply.toString();
    }
    
    @LockMapping(name="gw_ls")
    public void gwLsLock(HierarchicalLockHelper lockHelper, String clusterName) {
        lockHelper.root(READ).cluster(READ, clusterName).gwList(READ).gw(READ, ALL);
    }
    
}
