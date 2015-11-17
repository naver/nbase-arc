package com.navercorp.nbasearc.confmaster.server.command;

import static com.navercorp.nbasearc.confmaster.Constant.S2C_OK;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;

import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.dao.PhysicalMachineDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkWorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.repository.znode.PhysicalMachineData;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineImo;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;

@Service
public class PhysicalMachineService {

    @Autowired
    private PhysicalMachineImo pmImo;
    @Autowired
    private PhysicalMachineClusterImo pmClusterImo;

    @Autowired
    private PhysicalMachineDao pmDao;
    @Autowired
    private ZkWorkflowLogDao workflowLogDao;
 
    @CommandMapping(name="pm_add",
            usage="pm_add <pm_name> <pm_ip>\r\n" +
                    "add a physical machine")
    public String pmAdd(String pmName, String pmIp)
            throws MgmtZooKeeperException, NodeExistsException, NoNodeException {
        // Check
        if (pmDao.pmExist(pmName)) {
            return "-ERR duplicated id";
        }

        // Prepare
        PhysicalMachineData data = new PhysicalMachineData();
        data.setIp(pmIp);

        // DB
        pmDao.createPm(pmName, data);

        // In Memory
        PhysicalMachine pm = pmImo.load(pmName);
        
        // Log
        workflowLogDao.log(0, Constant.SEVERITY_MODERATE, 
                "PMAddCommand", Constant.LOG_TYPE_COMMAND, 
                "", "Add pm success. " + pm, 
                String.format("{\"pm_name\":\"%s\",\"pm_ip\":\"%s\"}", pmName, pmIp));
        
        return S2C_OK;
    }

    @LockMapping(name="pm_add")
    public void pmAddLock(HierarchicalLockHelper lockHelper) {
        lockHelper.pmList(WRITE);
    }

    @CommandMapping(name="pm_del",
            usage="pm_del <pm_name>\r\n" +
                    "delete a physical machine")
    public String pmDel(String pmName) throws MgmtZooKeeperException {
        // Check
        PhysicalMachine pm = pmImo.get(pmName);
        if (pm == null) {
            return "-ERR pm does not exist";
        }
        
        // In Memory
        List<PhysicalMachineCluster> clusterList = pmClusterImo.getList(pmName);
        
        // TODO : validation class
        // Prepare
        StringBuilder err = new StringBuilder();
        for (PhysicalMachineCluster cluster : clusterList) {
            if (!cluster.isEmpty()) {
                if (err.length() == 0) {
                    err.append("-ERR pm has servers; ");
                }
                
                err.append(cluster.getName()).append("(");
                if (!cluster.getData().getGwIdList().isEmpty()) {
                    err.append("gw:").append(
                            cluster.getData().getGwIdList().toString());
                }
                if (!cluster.getData().getPgsIdList().isEmpty()) {
                    if (err.length() != 0) {
                        err.append(", ");
                    }
                    err.append("pgs:").append(
                            cluster.getData().getPgsIdList().toString());
                }
                err.append(") ");
            }
        }
        
        if (err.length() != 0) {
            return err.toString();
        }
        
        // DB
        pmDao.deletePm(pmName);
        
        // In Memory
        pmImo.delete(PathUtil.pmPath(pmName));

        // Log
        workflowLogDao.log(0, Constant.SEVERITY_MODERATE, 
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
                    "get information of a Physical Machine")
    public String pmInfo(String pmName) {
        // In Memory
        PhysicalMachine pm = pmImo.get(pmName);
        
        // Check
        if (pm == null) {
            return "-ERR pm does not exist.";
        }

        // Do
        StringBuilder reply = new StringBuilder();
        
        reply.append("{\"pm_info\":");
        reply.append(pm.getData().toString());
        
        reply.append(", \"cluster_list\":");        
        reply.append(pm.getClusterListString(pmClusterImo));
        reply.append("}");
        
        return reply.toString();
    }

    @LockMapping(name="pm_info")
    public void pmInfoLock(HierarchicalLockHelper lockHelper, String pmName) {
        lockHelper.pmList(READ).pm(READ, pmName);
    }

    @CommandMapping(name="pm_ls",
            usage="pm_ls\r\n" +
                    "show a list of Physical Machines")
    public String pmLs() {
        // In Memory
        List<PhysicalMachine> pmList = pmImo.getAll();

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
