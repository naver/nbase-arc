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

import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowLogger;

@Service
public class PhysicalMachineService {

	@Autowired
	private ApplicationContext context;
	
    @Autowired
    private ClusterComponentContainer container;

    @Autowired
    private WorkflowLogger workflowLogger;
    
    @Autowired
    private ZooKeeperHolder zk;
 
    @CommandMapping(name="pm_add",
            usage="pm_add <pm_name> <pm_ip>\r\n" +
                    "add a physical machine",
            requiredState=ConfMaster.RUNNING)
    public String pmAdd(String pmName, String pmIp)
            throws MgmtZooKeeperException, NodeExistsException, NoNodeException {
        // Check
        if (container.getPm(pmName) != null) {
            return EXCEPTIONMSG_DUPLICATED_PM;
        }

        // Prepare
        PhysicalMachine pm = new PhysicalMachine(pmName, pmIp);
        createPmZooKeeperZnode(pm);
        
        // Log
        workflowLogger.log(0, Constant.SEVERITY_MODERATE, 
                "PMAddCommand", Constant.LOG_TYPE_COMMAND, 
                "", "Add pm success. " + pm, 
                String.format("{\"pm_name\":\"%s\",\"pm_ip\":\"%s\"}", pmName, pmIp));
        
        return S2C_OK;
    }
    
    protected void createPmZooKeeperZnode(PhysicalMachine pm) throws NodeExistsException,
            MgmtZooKeeperException, NoNodeException {
        // DB
        zk.createPersistentZNode(pm.getPath(), pm.persistentDataToBytes());

        // In Memory
        container.put(pm.getPath(), pm);
    }

    @LockMapping(name="pm_add")
    public void pmAddLock(HierarchicalLockHelper lockHelper) {
        lockHelper.pmList(WRITE);
    }

    @CommandMapping(name="pm_del",
            usage="pm_del <pm_name>\r\n" +
                    "delete a physical machine",
            requiredState=ConfMaster.RUNNING)
    public String pmDel(String pmName) throws MgmtZooKeeperException {
        // Check
        PhysicalMachine pm = container.getPm(pmName);
        if (pm == null) {
            return EXCEPTIONMSG_PHYSICAL_MACHINE_DOES_NOT_EXIST;
        }
        
        // In Memory
        List<PhysicalMachineCluster> clusterList = container.getPmcList(pmName);
        
        // Prepare
        StringBuilder err = new StringBuilder();
        for (PhysicalMachineCluster cluster : clusterList) {
            if (!cluster.isEmpty()) {
                if (err.length() == 0) {
                    err.append("-ERR pm has servers; ");
                }
                
                err.append(cluster.getName()).append("(");
                if (!cluster.getGwIdList().isEmpty()) {
                    err.append("gw:").append(
                            cluster.getGwIdList().toString());
                }
                if (!cluster.getPgsIdList().isEmpty()) {
                    if (err.length() != 0) {
                        err.append(", ");
                    }
                    err.append("pgs:").append(
                            cluster.getPgsIdList().toString());
                }
                err.append(") ");
            }
        }
        
        if (err.length() != 0) {
            return err.toString();
        }
        
        // DB
        zk.deleteZNode(pm.getPath(), -1);
        
        // In Memory
        container.delete(PathUtil.pmPath(pmName));

        // Log
        workflowLogger.log(0, Constant.SEVERITY_MODERATE, 
                "PMDelCommand", Constant.LOG_TYPE_COMMAND, 
                "", "Delete pm success. " + pm, 
                String.format("{\"pm_name\":\"%s\"}", pmName));
        
        return S2C_OK;
    }

    @LockMapping(name="pm_del")
    public void pmDelLock(HierarchicalLockHelper lockHelper, String pmName) {
        lockHelper.pmList(WRITE).pm(WRITE, pmName);
    }

    @CommandMapping(name="pm_info",
            usage="pm_info <pm_name>\r\n" +
                    "get information of a Physical Machine",
            requiredState=ConfMaster.READY)
    public String pmInfo(String pmName) {
        // In Memory
        PhysicalMachine pm = container.getPm(pmName);
        
        // Check
        if (pm == null) {
            return EXCEPTIONMSG_PHYSICAL_MACHINE_DOES_NOT_EXIST;
        }

        // Do
        StringBuilder reply = new StringBuilder();
        
        reply.append("{\"pm_info\":");
        reply.append(pm.persistentDataToString());
        reply.append(", \"cluster_list\":");        
        reply.append(pm.getClusterListString(container));
        reply.append("}");
        
        return reply.toString();
    }

    @LockMapping(name="pm_info")
    public void pmInfoLock(HierarchicalLockHelper lockHelper, String pmName) {
        lockHelper.pmList(READ).pm(READ, pmName);
    }

    @CommandMapping(name="pm_ls",
            usage="pm_ls\r\n" +
                    "show a list of Physical Machines",
            requiredState=ConfMaster.READY)
    public String pmLs() {
        // In Memory
        List<PhysicalMachine> pmList = container.getAllPm();

        // Do
        StringBuilder reply = new StringBuilder();
        reply.append("{\"list\":[");
        for (PhysicalMachine pm : pmList) {
            reply.append("\"").append(pm.getName()).append("\", ");
        }
        if (!pmList.isEmpty()) {
            reply.delete(reply.length() - 2, reply.length()).append("]}");
        } else {
            reply.append("]}");
        }
        return reply.toString();
    }

    @LockMapping(name="pm_ls")
    public void pmLsLock(HierarchicalLockHelper lockHelper) {
        lockHelper.pmList(READ).pm(READ, Constant.ALL);
    }
    
}
