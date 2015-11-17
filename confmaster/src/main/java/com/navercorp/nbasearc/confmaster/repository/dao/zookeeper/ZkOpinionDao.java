package com.navercorp.nbasearc.confmaster.repository.dao.zookeeper;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.OpinionDao;
import com.navercorp.nbasearc.confmaster.repository.znode.OpinionData;

@Repository
public class ZkOpinionDao implements OpinionDao {

    @Autowired
    private ZooKeeperHolder zookeeper;

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
    
    @Override
    public List<OpinionData> getOpinions(final String path)
            throws MgmtZooKeeperException, NoNodeException {
        List<OpinionData> opinions = new ArrayList<OpinionData>();

        List<String> children;
        children = zookeeper.getChildren(path);
        
        for (String childName : children) {
            StringBuilder builder = new StringBuilder(path);
            builder.append("/").append(childName);
            String childPath = builder.toString();

            byte data[] = zookeeper.getData(childPath, null);
            OpinionData opData = mapper.readValue(data, OpinionData.class);
            opinions.add(opData);
        }
        
        return opinions;
    }
    
}
