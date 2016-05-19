package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.RedisServerData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;

public class StartOfTheEpochWorkflow {
    final ApplicationContext context;
    final ZooKeeperHolder zookeeper;
    final PartitionGroupDao pgDao;
    final PartitionGroupServerDao pgsDao;
    final RedisServerImo rsImo;

    final PartitionGroup pg;
    final PartitionGroupServer pgs;
    RedisServer rs;

    PartitionGroupServerData pgsM;
    PartitionGroupData pgM;
    RedisServerData rsM;
    List<OpResult> results;

    public StartOfTheEpochWorkflow(PartitionGroup pg, PartitionGroupServer pgs,
            ApplicationContext context) {
        this.context = context;
        this.zookeeper = context.getBean(ZooKeeperHolder.class);
        this.pgDao = context.getBean(PartitionGroupDao.class);
        this.pgsDao = context.getBean(PartitionGroupServerDao.class);
        this.rsImo = context.getBean(RedisServerImo.class);

        this.pg = pg;
        this.pgs = pgs;
    }

    public void execute() throws InterruptedException, KeeperException {
        final int mgen = pg.getData().currentGen();
        final int copy = pg.getData().getCopy();

        if (mgen != -1 || copy != 0) {
            return;
        }

        pgsM = PartitionGroupServerData.builder().from(pgs.getData())
                .withHb(HB_MONITOR_YES).withColor(BLUE).build();

        rs = rsImo.get(pgs.getName(), pgs.getClusterName());
        rsM = RedisServerData.builder().from(rs.getData())
                .withHb(HB_MONITOR_YES).build();

        pgM = PartitionGroupData.builder().from(pg.getData()).withCopy(1)
                .withQuorum(0).build();

        List<Op> opList = new ArrayList<Op>();
        opList.add(pgsDao.createUpdatePgsOperation(pgs.getPath(), pgsM));
        opList.add(pgsDao.createUpdateRsOperation(rs.getPath(), rsM));
        opList.add(pgDao.createUpdatePgOperation(pg.getPath(), pgM));

        results = zookeeper.getZooKeeper().multi(opList);
        pgs.setStat(((OpResult.SetDataResult) results.get(0)).getStat());
        pgs.setData(pgsM);
        rs.setData(rsM);
        pg.setData(pgM);
    }
}
