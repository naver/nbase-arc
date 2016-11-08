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
import static com.navercorp.nbasearc.confmaster.server.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.server.lock.LockType.WRITE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.ConfMasterException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.GatewayLookup;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.server.lock.HierarchicalLockPMList;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.ClusterHint;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowLogger;

@Service
public class GatewayService {

    @Autowired
    private ClusterComponentContainer container;
    
    @Autowired
    private GatewayLookup gwInfoNotifier;
    @Autowired
    private WorkflowLogger workflowLogger;
    
    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ZooKeeperHolder zk;
    
    private MemoryObjectMapper mapper = new MemoryObjectMapper();

    @CommandMapping(
            name="gw_add",
            usage="gw_add <cluster_name> <gwid> <pm_name> <pm_ip> <port>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String gwAdd(@ClusterHint String clusterName, String gwId, String pmName,
            String pmIp, Integer port) throws MgmtZooKeeperException,
            NoNodeException {
        // In Memory
        List<Gateway> gwList = container.getGwList(clusterName);
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
        
        // Prepare
        for (Gateway gw : gwList) {
            if (gw.getName().equals(gwId)) {
                throw new IllegalArgumentException(
                        "-ERR duplicated. cluster: " + clusterName + ", gw: " + gwId);
            }
        }

        Gateway gw = new Gateway(context, clusterName, gwId, pmName, pmIp, port, 0);
        createGwZooKeeperZnode(cluster, gw);

        workflowLogger.log(0, SEVERITY_MODERATE, 
                "GWAddCommand", LOG_TYPE_COMMAND, 
                clusterName, "Add gateway success. " + gw, 
                String.format("{\"cluster_name\":\"%s\",\"gw_id\":\"%s\",\"host\":\"%s\",\"pm_ip\":\"%s\",\"port\":%d}", 
                        clusterName, gwId, pmName, pmIp, port));

        return S2C_OK;
    }
    
    protected void createGwZooKeeperZnode(Cluster cluster, Gateway gw) throws MgmtZooKeeperException,
            NoNodeException {
        // DB
        // Do not check... if pmCluster is null than it will make it 
        PhysicalMachineCluster pmCluster = container.getPmc(gw.getPmName(), cluster.getName());
        PhysicalMachineCluster cim = null;
        
        // Prepare
        List<Op> ops = new ArrayList<Op>();
        ops.add(Op.create(gw.getPath(), gw.persistentDataToBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));

        if (null != pmCluster) {
        	PhysicalMachineCluster.PmClusterData cimDataClon = pmCluster.clonePersistentData();
            cimDataClon.addGwId(Integer.valueOf(gw.getName()));
            ops.add(Op.setData(pmCluster.getPath(),
                    mapper.writeValueAsBytes(cimDataClon), -1));
        } else {
			cim = new PhysicalMachineCluster(cluster.getName(), gw.getPmName());
			cim.addGwId(Integer.valueOf(gw.getName()));
			ops.add(Op.create(cim.getPath(), cim.persistentDataToBytes(),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
		}
        
        List<OpResult> results = zk.multi(ops);
        zk.handleResultsOfMulti(results);

        // In memory
        container.put(gw.getPath(), gw);

        zk.registerChangedEventWatcher(gw.getPath());
        zk.registerChildEventWatcher(gw.getPath());
        
        // Physical Machine Cluster info
        PhysicalMachineCluster clusterInPm = container.getPmc(gw.getPmName(), cluster.getName()); 
        if (null == clusterInPm) {
            container.put(cim.getPath(), cim);
        } else {
            clusterInPm.addGwId(Integer.valueOf(gw.getName()));
        }
        
        // Update gateway affinity
        gwInfoNotifier.updateGatewayAffinity(cluster);
    }
    
    protected void deleteGwObject(String pmName, String clusterName, String gwId)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException {
        PhysicalMachineCluster pmCluster = container.getPmc(pmName, clusterName);
        if (null == pmCluster) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PHYSICAL_MACHINE_CLUSTER_DOES_NOT_EXIST
                            + PhysicalMachineCluster.fullName(pmName,
                                    clusterName));
        }
        
        // DB
        String path = PathUtil.gwPath(gwId, clusterName);

        List<Op> ops = new ArrayList<Op>();
        List<String> children = zk.getChildren(path);

        for (String childName : children) {
            StringBuilder builder = new StringBuilder(path);
            builder.append("/").append(childName);
            String childPath = builder.toString();

            ops.add(Op.delete(childPath, -1));
        }

        ops.add(Op.delete(path, -1));

        PhysicalMachineCluster.PmClusterData pmClusterDataClone = pmCluster.clonePersistentData();
        pmClusterDataClone.deleteGwId(Integer.valueOf(gwId));
        byte cimDataOfBytes[] = mapper.writeValueAsBytes(pmClusterDataClone);
        ops.add(Op.setData(pmCluster.getPath(), cimDataOfBytes, -1));

        if (pmClusterDataClone.getPgsIdList().isEmpty()
                && pmClusterDataClone.getGwIdList().isEmpty()) {
            ops.add(Op.delete(pmCluster.getPath(), -1));
        }

        if (gwInfoNotifier.isGatewayExist(clusterName, gwId)) {
            gwInfoNotifier.addDeleteGatewayOp(ops, clusterName, gwId);
        }
        
        List<OpResult> results = zk.multi(ops);
        zk.handleResultsOfMulti(results);

        // In Memory
        container.delete(PathUtil.gwPath(gwId, clusterName));
        
        // Delete from pm-cluster
        pmCluster.deleteGwId(gwId);
        if (pmCluster.isEmpty()) {
            container.delete(PathUtil.pmClusterPath(clusterName, pmName));
        }
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
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String gwAffinitySync(@ClusterHint String clusterName) throws MgmtZooKeeperException {
        Cluster cluster = container.getCluster(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }
        
        return gwInfoNotifier.updateGatewayAffinity(cluster);
    }
    
    @LockMapping(name="gw_affinity_sync")
    public void gwAffinitySyncLock(HierarchicalLockHelper lockHelper, String clusterName) {
        lockHelper.root(READ).cluster(WRITE, clusterName).gwList(READ).gw(READ, ALL);
    }
    
    @CommandMapping(
            name="gw_del",
            usage="gw_del <cluster_name> <gwid>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String gwDel(@ClusterHint String clusterName, String gwId)
            throws MgmtZNodeDoesNotExistException, ConfMasterException,
            MgmtZooKeeperException {
        // Check
        Cluster cluster = container.getCluster(clusterName);
        if (null == cluster) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }
        
        Gateway gw = container.getGw(clusterName, gwId);
        if (null == gw) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_GATEWAY_DOES_NOT_EXIST
                            + Gateway.fullName(clusterName, gwId));
        }

        deleteGwObject(gw.getPmName(), clusterName, gwId);
        
        // Update gateway affinity
        gwInfoNotifier.updateGatewayAffinity(cluster);

        // Log
        workflowLogger.log(0, SEVERITY_MODERATE, 
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
        Gateway gw = container.getGw(clusterName, gwid);
        lockPMList.pm(WRITE, gw.getPmName());
    }
    
    @CommandMapping(
            name="gw_info",
            usage="gw_info <cluster_name> <gw_id>\r\n" +
                    "get information of a Gateway",
            requiredState=ConfMaster.READY,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public String gwInfo(@ClusterHint String clusterName, String gwid)
            throws KeeperException, InterruptedException, IOException {
        Cluster cluster = container.getCluster(clusterName);
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
        List<Gateway> gwList = container.getGwList(cluster.getName());
        
        // Prepare 
        StringBuffer sb = new StringBuffer();
        sb.append("{\"gw_list\":[");

        for (Gateway gw : gwList) {
            sb.append(gw.persistentDataToString());
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
        Gateway gw = container.getGw(cluster.getName(), gwid);
        if (null == gw) {
            return "-ERR gw does not exist.";
        }

        try {
            return gw.persistentDataToString();
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
            requiredState=ConfMaster.READY,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public String gwLs(@ClusterHint String clusterName) {
        // Check
        if (null == container.getCluster(clusterName)) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }

        // In Memory
        List<Gateway> gwList = container.getGwList(clusterName);
        
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
