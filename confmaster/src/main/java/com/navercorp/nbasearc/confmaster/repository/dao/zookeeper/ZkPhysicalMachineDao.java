package com.navercorp.nbasearc.confmaster.repository.dao.zookeeper;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.PhysicalMachineDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PhysicalMachineData;

@Repository
public class ZkPhysicalMachineDao implements PhysicalMachineDao {
    
    @Autowired
    private ZooKeeperHolder zookeeper;

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();

    @Override
    public boolean pmExist(final String name) throws MgmtZooKeeperException {
        String path = PathUtil.pmPath(name);
        return zookeeper.isExists(path);
    }
    
    @Override
    public String createPm(final String name, final PhysicalMachineData data)
            throws NodeExistsException, MgmtZooKeeperException {
        String path = PathUtil.pmPath(name);
        String createdPath;
        byte rawData[];

        rawData = mapper.writeValueAsBytes(data);            
        createdPath = zookeeper.createPersistentZNode(path, rawData);
        return createdPath;
    }

    @Override
    public void deletePm(final String name) throws MgmtZooKeeperException  {
        String path = PathUtil.pmPath(name);
        zookeeper.deleteZNode(path, -1);
    }

    @Override
    public byte[] loadPm(String name) throws NoNodeException, MgmtZooKeeperException {
        return zookeeper.getData(PathUtil.pmPath(name), null, null);
    }
    
}
