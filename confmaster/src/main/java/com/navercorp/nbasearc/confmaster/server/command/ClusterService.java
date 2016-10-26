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
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;
import static com.navercorp.nbasearc.confmaster.server.mapping.ArityType.GREATER;
import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.STRING_VARG;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.ConfMasterException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtCommandWrongArgumentException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtInvalidQuorumPolicyException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeAlreayExistsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.io.MultipleGatewayInvocator;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.ClusterBackupScheduleDao;
import com.navercorp.nbasearc.confmaster.repository.dao.ClusterDao;
import com.navercorp.nbasearc.confmaster.repository.dao.NotificationDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkWorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockPMList;
import com.navercorp.nbasearc.confmaster.repository.znode.ClusterBackupScheduleData;
import com.navercorp.nbasearc.confmaster.repository.znode.ClusterData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PgDataBuilder;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterBackupSchedule;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.GatewayImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.Param;
import com.navercorp.nbasearc.confmaster.server.mapping.ParamClusterHint;

/* 
 * All services of commands throws Exception upward 
 * in order to notify error to a client who requests a command.
 */
@Service
public class ClusterService {
    
    @Autowired
    private ZooKeeperHolder zookeeper;

    @Autowired
    private ThreadPool executor;
    
    @Autowired
    private Config config;

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
    private PhysicalMachineClusterImo clusterInPmImo;

    @Autowired
    private ClusterDao clusterDao;
    @Autowired
    private ClusterBackupScheduleDao clusterBackupScheduleDao;
    @Autowired
    private NotificationDao notificationDao;
    @Autowired
    private ZkWorkflowLogDao workflowLogDao;
    
    @Autowired
    private PhysicalMachineService pmService;
    @Autowired
    private PartitionGroupService pgService;
    @Autowired
    private PartitionGroupServerService pgsService;
    @Autowired
    private GatewayService gwService;
    
    @CommandMapping(
            name="cluster_add",
            usage="cluster_add <cluster_name> <quorum policy>\r\n" +
                    "Ex) cluster_add cluster1 0:1\r\n" +
                   "add a new cluster",
            requiredState=ConfMaster.RUNNING)
    public String clusterAdd(String clusterName, String quorumPolicy)
            throws MgmtCommandWrongArgumentException,
            MgmtZNodeAlreayExistsException, MgmtZooKeeperException,
            NoNodeException {
        if (clusterImo.get(clusterName) != null) {
            return EXCEPTIONMSG_DUPLICATED_CLUSTER;
        }
        
        Integer keyspaceSize = KEY_SPACE_SIZE;

        // Prapare
        ClusterData data = new ClusterData();
        List<Integer> quorumPolicyAsList = new ArrayList<Integer>();
        List<Integer> pnPgMap = new ArrayList<Integer>(keyspaceSize);

        data.setKeySpaceSize(keyspaceSize);
        for (int i = 0; i < keyspaceSize; i++) {
            pnPgMap.add(-1);
        }
        data.setPnPgMap(pnPgMap);
        for (String item : quorumPolicy.split(":")) {
            try {
                quorumPolicyAsList.add(Integer.parseInt(item));
                isValidQuorumPolicy(quorumPolicyAsList);
            } catch (Exception e) {
                return "-ERR Invalid quorum policy";
            }
        }
        data.setQuorumPolicy(quorumPolicyAsList);
        data.setMode(CLUSTER_ON);
        
        Cluster cluster = createClusterObject(clusterName, data);
        
        // Log
        workflowLogDao.log(0, SEVERITY_MODERATE, 
                "ClusterAddCommand", LOG_TYPE_COMMAND, 
                clusterName, "Add cluster success. " + cluster, 
                String.format("{\"cluster_name\":\"%s\",\"key_space_size\":\"%s\",\"quorum_policy\":\"%s\"}", 
                        clusterName, keyspaceSize, quorumPolicy));
        
        return S2C_OK;
    }
    
    protected Cluster createClusterObject(String clusterName, ClusterData data)
            throws MgmtZNodeAlreayExistsException, MgmtZooKeeperException,
            NoNodeException {
        // DB
        clusterDao.createCluster(clusterName, data);

        // In Memory
        return clusterImo.load(clusterName);
    }
    
    protected void deleteClusterobject(String clusterName)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException {        
        List<PhysicalMachine> pmList = pmImo.getAll();
        for (PhysicalMachine pm : pmList) {
            PhysicalMachineCluster clusterInPm = 
                    clusterInPmImo.get(clusterName, pm.getName());
            if (null == clusterInPm) {
                continue;
            }

            if (!clusterInPm.getData().getGwIdList().isEmpty()) {
                throw new IllegalArgumentException(EXCEPTIONMSG_CLUSTER_HAS_GW
                        + Cluster.fullName(clusterName));
            }
            if (!clusterInPm.getData().getPgsIdList().isEmpty()) {
                throw new IllegalArgumentException(EXCEPTIONMSG_CLUSTER_HAS_PGS
                        + Cluster.fullName(clusterName));
            }
        }
        
        // DB
        clusterDao.deleteCluster(clusterName);

        // In Memory
        clusterImo.delete(PathUtil.clusterPath(clusterName));
    }

    @LockMapping(name="cluster_add")
    public void clusterAddLock(HierarchicalLockHelper lockHelper) {
        lockHelper.root(WRITE);
    }

    private void isValidQuorumPolicy(List<Integer> quorumPolicy) 
            throws MgmtInvalidQuorumPolicyException {
        if ((quorumPolicy.size() > 1)
                && (quorumPolicy.get(quorumPolicy.size() - 2) 
                        >= (quorumPolicy.get(quorumPolicy.size() - 1)))) {
            throw new MgmtInvalidQuorumPolicyException();
        }
    }
    
    @CommandMapping(
            name="cluster_del",
            usage="cluster_del <cluster_name>\r\n" +
                    "Ex) cluster_del cluster1\r\n" +
                    "delete a new cluster",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String clusterDel(@ParamClusterHint String clusterName)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException {
        // Check
        Cluster cluster = clusterImo.get(clusterName);
        if (null == cluster) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }

        // Check
        if (!pgImo.getList(clusterName).isEmpty()) {
            throw new IllegalArgumentException(EXCEPTIONMSG_CLUSTER_HAS_PG
                    + Cluster.fullName(clusterName));
        }
        
        deleteClusterobject(clusterName);
        
        // Log
        workflowLogDao.log(0, SEVERITY_MODERATE, 
                "ClusterDelCommand", LOG_TYPE_COMMAND, 
                clusterName, "Delete cluster success. " + cluster, 
                String.format("{\"cluster_name\":\"%s\"}", clusterName));
        
        return S2C_OK;
    }
    
    @LockMapping(name="cluster_del")
    public void clusterDelLock(HierarchicalLockHelper lockHelper, String clusterName) {
        lockHelper.root(WRITE);
        HierarchicalLockPMList lockPMList = lockHelper.pmList(READ);
        List<PhysicalMachine> pmList = pmImo.getAll();
        for (PhysicalMachine pm : pmList) {
            PhysicalMachineCluster clusterInPm = 
                    clusterInPmImo.get(clusterName, pm.getName());
            if (clusterInPm != null) {
                lockPMList.pm(READ, pm.getName());
            }
        }
    }
    
    @CommandMapping(
            name="cluster_info",
            usage="cluster_info <cluster_name>",
            requiredState=ConfMaster.READY,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public String clusterInfo(@ParamClusterHint String clusterName) throws InterruptedException,
            KeeperException, IOException {
        // Check
        Cluster cluster = clusterImo.get(clusterName);
        if (null == cluster) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }
        
        // In Memory
        List<PartitionGroup> pgList = pgImo.getList(clusterName);
        List<Gateway> gwList = gwImo.getList(clusterName);
        
        // Prepare
        StringBuilder sb = new StringBuilder();

        sb.append("{\"cluster_info\":{");
        sb.append("\"Key_Space_Size\":").append(cluster.getData().getKeySpaceSize()).append("");
        sb.append(",\"Quorum_Policy\":\"").append(cluster.getData().getQuorumPolicy()).append("\"");
        sb.append(",\"PN_PG_Map\":\"").append(cluster.getData().pNPgMapToRLS()).append("\"");
        sb.append(",\"Mode\":\"").append(cluster.getData().getMode()).append("\"}, ");
        
        sb.append("\"pg_list\":[");
        long wf = 0;
        for (PartitionGroup pg : pgList) {
            wf += pg.getWfCnt();
            sb.append("{\"pg_id\":\"").append(pg.getName()).append("\", \"pg_data\":");
            try {
                sb.append(pg.getData().toString()).append("}, ");
            } catch (RuntimeException e) {
                Logger.error("Convert to json fail. {} {}", cluster, cluster.getData());
                throw e;
            }
        }
        if (!pgList.isEmpty()) {
            sb.delete(sb.length() - 2, sb.length()).append("]");
        } else {
            sb.append("]");
        }
        
        sb.append(", \"gw_id_list\":[");
        for (Gateway gw : gwList) {
            sb.append(gw.getName()).append(", ");
        }
        if (!gwList.isEmpty()) {
            sb.delete(sb.length() - 2, sb.length()).append("]");
        } else {
            sb.append("]");
        }
        
        sb.append(",\"wf\":").append(wf).append("}");
        
        return sb.toString();
    }

    @LockMapping(name="cluster_info")
    public void clusterInfoLock(HierarchicalLockHelper lockHelper, String clusterName) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(READ, ALL).pgs(READ, ALL)
                .gwList(READ).gw(READ, ALL);
    }
    
    @CommandMapping(
            name="cluster_ls",
            usage="cluster_ls",
            requiredState=ConfMaster.READY)
    public String clusterLs() {
        // In Memory
        List<Cluster> clusters = clusterImo.getAll();
        
        // Prepare
        StringBuilder reply = new StringBuilder();
        reply.append("{\"list\":[");
        for (Cluster cluster : clusters) {
            reply.append("\"").append(cluster.getName()).append("\", ");
        }
        if (!clusters.isEmpty()) {
            reply.delete(reply.length()-2,reply.length()).append("]}");
        } else {
            reply.append("]}");
        }
        return reply.toString();
    }

    @LockMapping(name="cluster_ls")
    public void clusterLsLock(HierarchicalLockHelper lockHelper) {
        lockHelper.root(READ);
    }
    
    /*
     * This command is used by Gateway.
     * When a gateway begin, it send get_cluster_info to MGMT-CC in order to get cluster information.
     * This command replies formatted message using by gateway.
     */
    @CommandMapping(
            name="get_cluster_info",
            usage="get_cluster_info <cluster_name>",
            requiredState=ConfMaster.READY,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public String getClusterInfo(@ParamClusterHint String clusterName)
            throws InterruptedException, KeeperException {
        // In Memory
        Cluster cluster = clusterImo.get(clusterName);
        List<PartitionGroup> pgList = pgImo.getList(clusterName);
        List<PartitionGroupServer> pgsList = pgsImo.getList(clusterName);
        
        // Prepare
        StringBuilder sb = new StringBuilder();
        List<Integer> slotMap;
        int slotStart;
        
        /* cluster slot size */
        sb.append(cluster.getData().getKeySpaceSize()).append("\r\n");

        /* slot pg mapping(Run Length Encoding) */
        slotMap = cluster.getData().getPnPgMap();
        slotStart = 0;
        for (int i = 1; i < slotMap.size(); i++) {
            if (!slotMap.get(slotStart).equals(slotMap.get(i))) {
                sb.append(String.format("%d %d ", slotMap.get(slotStart), i - slotStart));
                slotStart = i;
            }
        }
        sb.append(String.format("%d %d\r\n", slotMap.get(slotStart), slotMap.size() - slotStart));

        /* pg pgs mapping */
        for (PartitionGroup pg : pgList) {
            List<Integer> pgPgsList = pg.getData().getPgsIdList();
            Collections.sort(pgPgsList);
            
            sb.append(pg.getName()).append(" ");
            for (Integer pgsid : pgPgsList) {
                PartitionGroupServer pgs = pgsImo.get(String.valueOf(pgsid), clusterName);
                if (pgs.getData().getHb().equals(HB_MONITOR_YES)) {
                    sb.append(pgsid).append(" ");
                }
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("\r\n");
        }

        /* pgs info */
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getData().getHb().equals(HB_MONITOR_YES)) {
                sb.append(
                    String.format(
                        "%s:%s:%d:%d ", 
                        pgs.getName(), pgs.getData().getPmIp(), 
                        pgs.getData().getSmrBasePort(), 
                        pgs.getData().getRedisPort()));
            }
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append("\r\n");

        return sb.toString();
    }

    @LockMapping(name="get_cluster_info")
    public void getClusterInfoLock(HierarchicalLockHelper lockHelper, String clusterName) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(READ)
                .pg(READ, ALL).pgs(READ, ALL)
                .gwList(READ).gw(READ, ALL);
    }
    
    @CommandMapping(
            name="appdata_set",
            arityType=GREATER,
            usage="appdata_set <cluster_name> backup <backup_id> <daemon_id> <period> <base_time> <holding period(day)> <net_limit(MB/s)> <output_format> [<service url>]\r\n" +
                    "Ex) appdata_set c1 backup 1 1 0 2 * * * * 02:00:00 3 70 base32hex rsync -az {FILE_PATH} 192.168.0.1::TEST/{MACHINE_NAME}-{CLUSTER_NAME}-{DATE}.json",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String clusterBackupScheduleSet(@ParamClusterHint String clusterName, String type,
            Integer backupID, Integer daemonID, String minute, String hour,
            String day, String month, String dayOfWeek, String year,
            String baseTime, Integer holdingPeriod, Integer netLimit,
            String outputFormat,
            @Param(type = STRING_VARG) String... serviceURLArgs)
            throws MgmtZooKeeperException, NodeExistsException {
        type = type.toLowerCase();
        StringBuilder serviceURLBuilder = new StringBuilder();
        String serviceURL;
        
        if (serviceURLArgs.length > 0) {
            for (int i = 0; i < serviceURLArgs.length; i ++) {
                serviceURLBuilder.append(serviceURLArgs[i].trim()).append(" ");
            }
            serviceURL = serviceURLBuilder.toString().trim();
        } else {
            serviceURL = "";
        }

        /* 
         * This cronExpression is used by python apscheduler-2.1.2.
         * Apscheduler defines cron expression as below.
         * (https://apscheduler.readthedocs.org/en/latest/modules/triggers/cron.html#module-apscheduler.triggers.cron)
         * 
         * Apscheduler cron expression
         * ---------------------------
         * minute        0-59
         * hour          0-23
         * day of month  1-31
         * month         1-12
         * day_of_week   0-6 or mon,tue,wed,thu,fri,sat,sun)
         * 
         * Spring cron expression
         * ----------------------
         * minute        0-59
         * hour          0-23
         * day of month  1-31
         * month         1-12 (or names, see below)
         * day of week   0-7 (0 or 7 is Sun, or use names)
         */
        String cronExpression = minute + " " + hour + " " + day + " " + month
                + " " + dayOfWeek + " " + year;

        // In order to verify cron expression.
        new CronTrigger(cronExpression);

        ClusterBackupScheduleData arg;
        arg = new ClusterBackupScheduleData();
        arg.setCluster_name(clusterName);
        arg.setType(type);
        arg.setBackup_id(backupID);
        arg.setDaemon_id(daemonID);
        arg.setPeriod(cronExpression);
        arg.setBase_time(baseTime);
        arg.setHolding_period(holdingPeriod);
        arg.setNet_limit(netLimit);
        arg.setOutput_format(outputFormat);
        arg.setService_url(serviceURL);
        
        Cluster cluster = clusterImo.get(arg.getCluster_name());
        if (cluster == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }

        ClusterBackupSchedule data;
        try {
            try {
                data = clusterBackupScheduleDao.loadClusterBackupSchedule(arg.getCluster_name());
                
                if (!data.existBackupJob(arg.getBackup_id())) {
                    data.addBackupSchedule(arg);
                } else {
                    data.updateBackupSchedule(arg);
                }
                clusterBackupScheduleDao.setAppData(arg.getCluster_name(), data);
            } catch (KeeperException.NoNodeException e) {
                // Create znode for BACKUP_PROP
                data =  new ClusterBackupSchedule();
                data.addBackupSchedule(arg);
                clusterBackupScheduleDao.createAppData(arg.getCluster_name(), data);
            }
        } catch (ConfMasterException e) {
            String logMsg = "-ERR " + e.getMessage();
            Logger.error("Set appdate fail. {} {}", clusterName, arg, e);
            return logMsg;
        }

        return S2C_OK;
    }

    @LockMapping(name="appdata_set")
    public void clusterBackupScheduleSetLock(HierarchicalLockHelper lockHelper,
            String clusterName) {
        lockHelper.root(READ).cluster(WRITE, clusterName);
    }
    
    @CommandMapping(
            name="appdata_get",
            usage="appdata_get <cluster_name> backup <backup_id>",
            requiredState=ConfMaster.READY,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public String clusterBackupScheduleGet(@ParamClusterHint String clusterName, String type,
            String backupID) throws MgmtZooKeeperException,
            ConfMasterException, NodeExistsException {
        Cluster cluster = clusterImo.get(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }
        
        ClusterBackupSchedule data = null;
        try {
            data = clusterBackupScheduleDao.loadClusterBackupSchedule(clusterName);
            
            if (type.equals(APPDATA_TYPE_BACKUP)) {
                return handleBackupData(data, backupID);
            } else {
                return "-ERR '" + type + "' is unsupported type";
            }
        } catch (KeeperException.NoNodeException e) {
            data =  new ClusterBackupSchedule();
            clusterBackupScheduleDao.createAppData(clusterName, data);
            return "-ERR backup '" + backupID + "' is not exist.";
        }
    }
    
    public String handleBackupData(ClusterBackupSchedule data, String backupID) {
        if (backupID.equals(ALL)) {
            return data.toString();
        } else {
            int id = Integer.parseInt(backupID);
            
            if (!data.existBackupJob(id)) {
                return "-ERR backup '" + backupID + "' is not exist.";
            }
            
            ClusterBackupScheduleData backupData = data.getBackupSchedule(id);
            return backupData.toString();
        }
    }

    @LockMapping(name="appdata_get")
    public void clusterBackupScheduleGetLock(HierarchicalLockHelper lockHelper, String clusterName) {
        lockHelper.root(READ).cluster(READ, clusterName);
    }
    
    @CommandMapping(
            name="appdata_del",
            usage="appdata_del <cluster_name> backup <backup_id>\r\n" +
                    "Ex) appdata_del c1 backup 1",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String clusterBackupScheduleDel(@ParamClusterHint String clusterName, String type,
            int backupID) throws NoNodeException, MgmtZooKeeperException,
            ConfMasterException {
        Cluster cluster = clusterImo.get(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }

        ClusterBackupSchedule data = clusterBackupScheduleDao
                .loadClusterBackupSchedule(clusterName);
        if (data == null) {
            return "-ERR  backup properties does not exists.";
        }
        
        data.deleteBackupSchedule(backupID);
        clusterBackupScheduleDao.setAppData(clusterName, data);
        
        return S2C_OK;
    }

    @LockMapping(name="appdata_del")
    public void clusterBackupScheduleDelLock(HierarchicalLockHelper lockHelper,
            String clusterName) {
        lockHelper.root(READ).cluster(WRITE, clusterName);
    }

    @CommandMapping(
            name="mig2pc",
            usage="mig2pc <cluster_name> <src_pgid> <dest_pgid> <range_from> <range_to>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String mig2pc(@ParamClusterHint String clusterName, String srcPgId, String destPgId,
            Integer rangeFrom, Integer rangeTo) {
        // In Memory
        Cluster cluster = clusterImo.get(clusterName);
        if (null == cluster) {
            Logger.error(EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST + ", cluster:"
                    + clusterName);
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        List<PartitionGroupServer> pgsList = pgsImo.getList(
                clusterName, Integer.valueOf(srcPgId));
        List<Gateway> gwList = gwImo.getList(clusterName);
        
        /* master pgs of source */
        PartitionGroupServer srcMaster = null;
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getData().getRole().equals(PGS_ROLE_MASTER)) {
                srcMaster = pgs;
                break;
            }
        }

        if (srcMaster == null) {
            workflowLog(SEVERITY_MODERATE,
                    "source master is not exist", clusterName, srcPgId,
                    destPgId, rangeFrom, rangeTo);
            return "-ERR Migration error, source master is not exist";
        }
        
        List<Integer> slotMap = cluster.getData().getPnPgMap();
        for (int slot = rangeFrom; slot <= rangeTo; slot++) {
            int owner_pgId = slotMap.get(slot);
            try {
                if (owner_pgId != Integer.parseInt(srcPgId)) {
                    String errMsg = "-ERR pg does not have a slot. PG:"
                            + srcPgId + ", SLOT:" + slot + ", OWNER_PG:"
                            + owner_pgId;
            Logger.error(errMsg);
                    return errMsg; 
                }
            } catch (IndexOutOfBoundsException e) {
                String errMsg = "-ERR pg does not have a slot. PG:" + srcPgId
                        + ", SLOT:" + slot + ", OWNER_PG:" + owner_pgId;
                Logger.error(errMsg);
                return errMsg;
            }
        }

        /* gateway ping check, before sending delay command */
        String ret = cluster.isGatewaysAlive(gwImo);
        if (null != ret) {
            return ret;
        }

        MultipleGatewayInvocator broadcast = new MultipleGatewayInvocator();
        ret = broadcast.request(clusterName, gwList, GW_PING, GW_PONG, executor);
        if (null != ret) {
            return ret;
        }

        /* send delay command to all gateways */
        String cmd = String.format("delay %d %d", rangeFrom, rangeTo);
        ret = broadcast.request(clusterName, gwList, cmd, GW_RESPONSE_OK, executor);
        if (null != ret) {
            return "-ERR Migration error, while sending delay-command to all gateways. (consider to cancel delay), " + ret;
        }

        /* catchup checking loop */
        long startTick = System.currentTimeMillis();
        long endTick = startTick + config.getServerCommandMig2pcCatchupTimeout();
        boolean rollback = false;
        String errMsg = null;
        
        long mig, logSeq, buf, acked;
        long minMigSeq = -1;
        long minBuf = -1;
        do {
            /* 
             * (reply example) +OK 
             * Logger:848339465 
             * mig:848335460 
             * buf:823233812 
             * sent:823211587 
             * acked:823211587 
             * */
            String reply = "";
            String[] tokens;
            try {
                reply = srcMaster.executeQuery("migrate info");
            } catch (IOException e) {
                rollback = true;
                errMsg = "Migration error. check srcMaster`s state";
                workflowLog(SEVERITY_MODERATE, errMsg, clusterName,
                        srcPgId, destPgId, rangeFrom, rangeTo);
                break;
            }

            tokens = reply.split(" ");
            if (!tokens[0].equals(S2C_OK)) {
                rollback = true;
                errMsg = "Migration error. unexpected reply from smrMaster, reply=" + reply;
                workflowLog(SEVERITY_MODERATE, errMsg, clusterName,
                        srcPgId, destPgId, rangeFrom, rangeTo);
                break;
            }
            logSeq = Long.parseLong(tokens[1].split(":")[1]);
            mig = Long.parseLong(tokens[2].split(":")[1]);
            buf = Long.parseLong(tokens[3].split(":")[1]);
            acked = Long.parseLong(tokens[5].split(":")[1]);
            
            /*
             * Save first return value of logSeq as minMigSeq. No more data
             * after minMigSeq is related to migration because all gateways are
             * already delayed.
             */
            if (minMigSeq == -1) {
                minMigSeq = logSeq;
            }

            /* When mig sequence exceeds minMigSeq, all migration data is copied to buf. */
            if (mig >= minMigSeq) {
                /* Check if data in buf is succesfully sent and acked. 
                 * 1. Save first return value of buf sequence as minBuf
                 * 2. wait until acked sequence exceeds minBuf */
                if (minBuf == -1) {
                    minBuf = buf;
                }

                if (acked >= minBuf) {
                    break;
                }
            }
            
            if (System.currentTimeMillis() > endTick) {
                rollback = true;
                errMsg = "Timeout";
                break;
            }
        } while (true);

        if (rollback) {
            Logger.error("Migration error. send cancel delay to gateway, "
                    + "cluster: " + clusterName + ", srcPgId: " + srcPgId
                    + ", destPgId: " + destPgId + ", rangeFrom: " + rangeFrom
                    + ", rangeTo: " + rangeTo + ", startTick: " + startTick
                    + ", endTick: " + endTick);
            
            /* cancel delay */
            cmd = String.format("redirect %d %d %s", rangeFrom, rangeTo, srcPgId);
            ret = broadcast.request(clusterName, gwList, cmd, GW_RESPONSE_OK, executor);
            if (null != ret) {
                return "-ERR Migration error, + " + errMsg
                        + ", (consider to cancel delay), " + ret;
            } else {
                return "-ERR Migration error. " + errMsg;
            }
        }
        
        /* send redirect command to all gateways */
        cmd = String.format("redirect %d %d %s", rangeFrom, rangeTo, destPgId);
        ret = broadcast.request(clusterName, gwList, cmd, GW_RESPONSE_OK, executor);
        if (null != ret) {
            return "-ERR Migration Partial Success, (consider to check states of gateways), " + ret;
        }
        
        try {
            ret = slotSetPgImpl(0, clusterName, Integer.parseInt(destPgId), rangeFrom, rangeTo);
        } catch (NumberFormatException e) {
            return "-ERR Migration error, (slot_set_pg fail), numberformat  exception, "
                    + e.getMessage();
        } catch (MgmtZooKeeperException e) {
            return "-ERR Migration error, (slot_set_pg fail), zookeeper exception, "
                    + e.getMessage();
        }
        if (null != ret) {
            return ret;
        }

        workflowLog(SEVERITY_MODERATE, "mi2pc succeeded.",
                clusterName, srcPgId, destPgId, rangeFrom, rangeTo);
        
        return S2C_OK;
    }
    
    @LockMapping(name="mig2pc")
    public void mig2pcLock(HierarchicalLockHelper lockHelper,
            String clusterName, String srcPgId, String destPgId) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(READ)
            .pg(WRITE, srcPgId).pg(WRITE, destPgId).pgs(READ, ALL_IN_PG)
            .gwList(READ).gw(WRITE, ALL);
    }

    private void workflowLog(String severity, String msg, String clusterName,
            String srcPgId, String destPgId, Integer rangeFrom, Integer rangeTo) {
        String arguments = String
                .format("{\"src_pg_id\":%s,\"dest_pg_id\":%s,\"range_from\":%d,\"range_to\":%d}",
                        srcPgId, destPgId, rangeFrom, rangeTo);
        // TODO
        workflowLogDao.log(0, severity, "Mig2PCCommand",
                LOG_TYPE_COMMAND, clusterName, msg, arguments);
    }
    
    @CommandMapping(
            name="slot_set_pg",
            usage="slot_set_pg <cluster_name> <pg_range_inclusive> <pgid>\r\n" +
                    "Ex) slot_set_pg cluster1 0:8191 1",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String slotSetPg(@ParamClusterHint String clusterName, String range, Integer pgid) {
        Integer rangeStart = Integer.parseInt(range.split(":")[0]);
        Integer rangeEnd = Integer.parseInt(range.split(":")[1]);

        Cluster cluster = clusterImo.get(clusterName);
        if (null == cluster) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        try {
            String ret = slotSetPgImpl(0, clusterName, pgid, rangeStart, rangeEnd);
            if (null != ret) {
                return ret;
            }
        } catch (NumberFormatException e) {
            return "-ERR Migration error, (slot_set_pg fail), numberformat  exception, "
                    + e.getMessage();
        } catch (MgmtZooKeeperException e) {
            return "-ERR Migration error, (slot_set_pg fail), zookeeper exception, "
                    + e.getMessage();
        }
        
        return S2C_OK;
    }

    private String slotSetPgImpl(long jobID, String clusterName, Integer pgid,
            Integer rangeStart, Integer rangeEnd) throws MgmtZooKeeperException {
        // In Memory
        Cluster cluster = clusterImo.get(clusterName);
        
        List<Integer> slotMap;
        int slotMapSize;
        
        slotMapSize = cluster.getData().getKeySpaceSize();
        slotMap = cluster.getData().getPnPgMap();

        if (rangeStart < 0 || rangeEnd < rangeStart || rangeEnd >= slotMapSize) {
            return "-ERR bad pg range:" + rangeStart + ":" + rangeEnd + " (try to slot_set_pg)";
        }

        for (int i = rangeStart; i <= rangeEnd; i++) {
            slotMap.set(i, pgid);
        }
        
        cluster.getData().setPnPgMap(slotMap);

        List<Op> ops = new ArrayList<Op>();
            
        ops.add(zookeeper.createReflectMemoryIntoZkOperation(cluster, -1));
        ops.add(notificationDao.createGatewayAffinityUpdateOperation(cluster));

        List<OpResult> results = null;
        try {
            results = zookeeper.multi(ops);
            OpResult.SetDataResult resultSetData = (OpResult.SetDataResult)results.get(0);
            cluster.setStat(resultSetData.getStat());
        } catch (MgmtZooKeeperException e) {
            Logger.error("Execute slot_set_pg fail. cluster: {}, pg: {}, range: {}-{}", 
                    new Object[]{clusterName, pgid, rangeStart, rangeEnd}, e);
            throw e;
        } finally {
            zookeeper.handleResultsOfMulti(results);
        }
        
        String message = "Set slot success. cluster:" + clusterName + "/pg:"
                + pgid + ", range: " + rangeStart + "~" + rangeEnd;
        workflowLogDao.log(jobID, SEVERITY_MODERATE, 
                "SlotSetPg", LOG_TYPE_COMMAND, 
                clusterName, message, 
                String.format("{\"cluster_name\":\"%s\",\"pg_id\":\"%s\",\"range_start\":%d,\"range_end\":%d}", 
                        clusterName, pgid, rangeStart, rangeEnd));
        
        return null;
    }
    
    @LockMapping(name="slot_set_pg")
    public void slotSetPgLock(HierarchicalLockHelper lockHelper,
            String clusterName, String range, Integer pgid) {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(WRITE, String.valueOf(pgid)).gwList(READ);
    }
    
    @CommandMapping(
            name="cluster_on",
            usage="cluster_on <cluster_name>\r\n",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_OFF)
    public String clusterOn(@ParamClusterHint String clusterName) {
        // In Memory
        Cluster cluster = clusterImo.get(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }
        
        Logger.info("cluster_on {}", Cluster.fullName(clusterName));
        
        // Do
        ClusterData clusterM = ClusterData.builder().from(cluster.getData())
                .withMode(CLUSTER_ON).build();
        try {
            clusterDao.updateCluster(cluster.getPath(), clusterM);
        } catch (MgmtZooKeeperException e) {
            return "-ERR Failed to cluster_on. zookeeper error. " + e.getMessage();
        }
        cluster.setData(clusterM);
        
        cluster.updateMode();
        
        return S2C_OK;
    }
    
    @LockMapping(name="cluster_on")
    public void clusterOnLock(HierarchicalLockHelper lockHelper,
            String clusterName) {
        lockHelper.root(READ).cluster(WRITE, clusterName);
    }
    
    @CommandMapping(
            name="cluster_off",
            usage="cluster_off <cluster_name>\r\n",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON)
    public String clusterOff(@ParamClusterHint String clusterName) {
        // In Memory
        Cluster cluster = clusterImo.get(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                            + Cluster.fullName(clusterName));
        }
        
        Logger.info("cluster_off {}", Cluster.fullName(clusterName));
        
        // Do
        ClusterData clusterM = ClusterData.builder().from(cluster.getData())
                .withMode(CLUSTER_OFF).build();
        try {
            clusterDao.updateCluster(cluster.getPath(), clusterM);
        } catch (MgmtZooKeeperException e) {
            return "-ERR Failed to cluster_off. zookeeper error. " + e.getMessage();
        }
        cluster.setData(clusterM);

        cluster.updateMode();
        
        return S2C_OK;
    }
    
    @LockMapping(name="cluster_off")
    public void clusterOffLock(HierarchicalLockHelper lockHelper,
            String clusterName) {
        lockHelper.root(READ).cluster(WRITE, clusterName);
    }
    
    @CommandMapping(
            name="cluster_purge",
            usage="cluster_purge <cluster_name>\r\n",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_OFF)
    public String clusterPurge(@ParamClusterHint String clusterName)
            throws MgmtZooKeeperException, MgmtZNodeDoesNotExistException {
        Cluster cluster = clusterImo.get(clusterName);
        if (cluster == null) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        
        // PGS
        for (PartitionGroupServer pgs : pgsImo.getList(clusterName)) {
            pgsService.deletePgsObject(clusterName, pgs);
        }
        
        // PG
        for (PartitionGroup pg : pgImo.getList(clusterName)) {
            pgService.deletePgObject(clusterName, pg.getName());
        }
        
        // GW
        for (Gateway gw : gwImo.getList(clusterName)) {
            gwService.deleteGwObject(gw.getData().getPmName(), clusterName, gw.getName());
        }
        
        // Cluster
        this.deleteClusterobject(clusterName);
        
        return S2C_OK;
    }
    
    @LockMapping(name="cluster_purge")
    public void clusterPurgeLock(HierarchicalLockHelper lockHelper,
            String clusterName) {
        lockHelper.root(READ).cluster(WRITE, clusterName);
    }

    @CommandMapping(
            name="cluster_dump",
            usage="cluster_dump <cluster_name>",
            requiredState=ConfMaster.RUNNING,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public String clusterDump(@ParamClusterHint String clusterName) {
        ClusterDump dump = new ClusterDump();
        Set<String> pmList = new HashSet<String>();
        
        // Cluster
        Cluster cluster = clusterImo.get(clusterName);
        if (cluster == null) {
            return EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;
        }
        dump.setClusterName(clusterName);
        dump.setCluster(cluster.getData());
        
        // PG
        List<PartitionGroup> pgList = pgImo.getList(clusterName);
        dump.setPgList(new ArrayList<PartitionGroupDump>());
        for (PartitionGroup pg : pgList) {
            PgDataBuilder builder = PartitionGroupData.builder().from(pg.getData());
            for (Integer pgsId : pg.getData().getPgsIdList()) {
                builder.deletePgsId(pgsId);
            }
            dump.getPgList().add(new PartitionGroupDump(pg.getName(), builder.build()));
        }

        // PGS
        List<PartitionGroupServer> pgsList = pgsImo.getList(clusterName);
        dump.setPgsList(new ArrayList<PartitionGroupServerDump>(pgsList.size()));
        for (PartitionGroupServer pgs : pgsList) {
            dump.getPgsList().add(new PartitionGroupServerDump(pgs.getName(), pgs.getData()));
            pmList.add(pgs.getData().getPmName());
        }
        
        // GW
        List<Gateway> gwList = gwImo.getList(clusterName);
        dump.setGwList(new ArrayList<GatewayDump>(gwList.size()));
        for (Gateway gw : gwList) {
            dump.getGwList().add(new GatewayDump(gw.getName(), gw.getData()));
            pmList.add(gw.getData().getPmName());
        }
        
        // PM
        dump.setPmList(new ArrayList<PhysicalMachineDump>(pmList.size()));
        for (String name : pmList) {
            dump.getPmList().add(new PhysicalMachineDump(name, pmImo.get(name).getData()));
        }
        
        // To JSON
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(dump);
        } catch (Exception e) {
            return "-ERR failed to convert object to JSON. "
                    + e.getMessage();
        }
    }
    
    @LockMapping(name="cluster_dump")
    public void clusterDumpLock(HierarchicalLockHelper lockHelper,
            String clusterName) {
        lockHelper.root(READ).cluster(READ, clusterName);
    }
    
    @CommandMapping(
            name="cluster_load",
            usage="cluster_load <dump>",
            requiredState=ConfMaster.RUNNING)
    public String clusterLoad(String data) throws NodeExistsException,
            NoNodeException, MgmtZooKeeperException,
            MgmtZNodeAlreayExistsException, MgmtZNodeDoesNotExistException {
        ClusterDump dump;
        ObjectMapper mapper = new ObjectMapper();
        try {
            dump = mapper.readValue(data, ClusterDump.class);
        } catch (Exception e) {
            return EXCEPTIONMSG_LOAD_FAIL + e.getMessage();
        }
        
        // PM
        for (PhysicalMachineDump pm : dump.getPmList()) {
            if (pmImo.get(pm.getPmName()) == null) {
                pmService.createPmObject(pm.getPmName(), pm.getData());
            }
        }        
        
        // Cluster
        if (clusterImo.get(dump.getClusterName()) != null) {
            return EXCEPTIONMSG_DUPLICATED_CLUSTER;
        }

        dump.getCluster().setMode(CLUSTER_OFF);
        this.createClusterObject(dump.getClusterName(), dump.getCluster());
                
        // PG
        for (PartitionGroupDump pg : dump.getPgList()) {
            pgService.createPgObject(dump.getClusterName(), pg.getPgId(), pg.getData());
        }
        
        // PGS
        Cluster cluster = clusterImo.get(dump.getClusterName());
        for (PartitionGroupServerDump pgs : dump.getPgsList()) {
            PartitionGroup pg = pgImo.get(String.valueOf(pgs.getData().getPgId()), dump.getClusterName());
            pgsService.createPgsObject(pgs.getData().getPmName(), cluster, pg, pgs.getPgsId(), pgs.getData());
        }

        List<Op> ops = new ArrayList<Op>();
        
        // GW
        for (GatewayDump gw : dump.getGwList()) {
            gwService.createGwObject(gw.getGwId(), gw.getData(), cluster, gw.getData().getPmName());
            
            // GW lookup
            notificationDao.addCreateGatewayOp(ops,
                    dump.getClusterName(), gw.getGwId(), gw.getData().getPmIp(),
                    gw.getData().getPort());
        }

        // Affinity
        ops.add(notificationDao.createGatewayAffinityUpdateOperation(cluster));
        zookeeper.handleResultsOfMulti(zookeeper.multi(ops));
        
        return S2C_OK;
    }
    
    @LockMapping(name="cluster_load")
    public void clusterLoadLock(HierarchicalLockHelper lockHelper,
            String clusterName) {
        lockHelper.pmList(WRITE);
        lockHelper.root(WRITE);
    }
    
}
