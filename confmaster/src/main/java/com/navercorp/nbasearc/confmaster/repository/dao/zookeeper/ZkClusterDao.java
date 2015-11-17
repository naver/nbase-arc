package com.navercorp.nbasearc.confmaster.repository.dao.zookeeper;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeAlreayExistsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.ClusterDao;
import com.navercorp.nbasearc.confmaster.repository.dao.NotificationDao;
import com.navercorp.nbasearc.confmaster.repository.znode.ClusterData;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;

@Repository
public class ZkClusterDao implements ClusterDao {
    
    @Autowired
    private ZooKeeperHolder zookeeper;
    @Autowired
    private NotificationDao notificationDao;

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();

    @Override
    public String createCluster(String name, ClusterData data)
            throws MgmtZNodeAlreayExistsException, MgmtZooKeeperException {
        String path = PathUtil.clusterPath(name);
        byte rawData[] = mapper.writeValueAsBytes(data);
        
        List<Op> ops = new ArrayList<Op>();
        ops.add(Op.create(path, rawData, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT));
        ops.add(Op.create(PathUtil.pgRootPath(name), Constant.ZERO_BYTE,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        ops.add(Op.create(PathUtil.pgsRootPath(name), Constant.ZERO_BYTE,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        ops.add(Op.create(PathUtil.gwRootPath(name), Constant.ZERO_BYTE,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        ops.add(Op.create(PathUtil.rsRootPath(name), Constant.ZERO_BYTE,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        notificationDao.addCreateClusterOp(ops, name);
        
        List<OpResult> results = zookeeper.multi(ops);
        zookeeper.handleResultsOfMulti(results);

        OpResult.CreateResult resultForCreatingRoot = (OpResult.CreateResult)results.get(0);
        
        return resultForCreatingRoot.getPath();
    }

    @Override
    public void deleteCluster(String name)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException {
        String path = PathUtil.clusterPath(name);

        List<Op> ops = new ArrayList<Op>();
        ops.add(Op.delete(PathUtil.pgRootPath(name), -1));
        ops.add(Op.delete(PathUtil.pgsRootPath(name), -1));
        ops.add(Op.delete(PathUtil.gwRootPath(name), -1));
        ops.add(Op.delete(PathUtil.rsRootPath(name), -1));

        if (zookeeper.isExists(PathUtil.clusterBackupSchedulePath(name))) {
            ops.add(Op.delete(PathUtil.clusterBackupSchedulePath(name), -1));
        }
        ops.add(Op.delete(path, -1));
        notificationDao.addDeleteClusterOp(ops, name);

        List<OpResult> results = zookeeper.multi(ops);
        zookeeper.handleResultsOfMulti(results);
    }
    
    @Override
    public byte[] loadCluster(String name, Stat stat, Watcher watch)
            throws NoNodeException, MgmtZooKeeperException {
        return zookeeper.getData(PathUtil.clusterPath(name), stat, watch);
    }
    
    @Override
    public void updateCluster(Cluster cluster) throws MgmtZooKeeperException {
        zookeeper.reflectMemoryIntoZk(cluster);
    }
    
}
