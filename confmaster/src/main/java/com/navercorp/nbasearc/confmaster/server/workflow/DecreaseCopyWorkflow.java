package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtNoAvaliablePgsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.NotificationDao;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.RedisServerData;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;

public class DecreaseCopyWorkflow {
    final ApplicationContext context;
    final ZooKeeperHolder zookeeper;
    final PartitionGroupServerDao pgsDao;
    final PartitionGroupDao pgDao;
    final NotificationDao notificationDao;
    final ClusterImo clusterImo;
    final PartitionGroupServerImo pgsImo;
    final RedisServerImo rsImo;

    final PartitionGroupServer pgs;
    final PartitionGroup pg;
    final String mode;

    public DecreaseCopyWorkflow(PartitionGroupServer pgs, PartitionGroup pg,
            String mode, ApplicationContext context) {
        this.context = context;
        this.zookeeper = context.getBean(ZooKeeperHolder.class);
        this.pgsDao = context.getBean(PartitionGroupServerDao.class);
        this.pgDao = context.getBean(PartitionGroupDao.class);
        this.notificationDao = context.getBean(NotificationDao.class);
        this.clusterImo = context.getBean(ClusterImo.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        this.rsImo = context.getBean(RedisServerImo.class);

        this.pgs = pgs;
        this.pg = pg;
        this.mode = mode;
    }

    public void execute() throws MgmtZooKeeperException,
            MgmtNoAvaliablePgsException {
        final RedisServer rs = rsImo.get(pgs.getName(), pgs.getClusterName());
        final int d = pg.getD(pgsImo.getList(pgs.getClusterName(), pgs
                .getData().getPgId()));
        final Color c = pgs.getData().getColor();

        if (!(mode != null && mode.equals(FORCED))) {
            if ((pg.getData().getQuorum() - d <= 0)
                    && (c != YELLOW && c != RED)) {
                throw new MgmtNoAvaliablePgsException();
            }
        }

        PartitionGroupServerData pgsM = PartitionGroupServerData.builder()
                .from(pgs.getData()).withHb(HB_MONITOR_NO).build();
        RedisServerData rsM = RedisServerData.builder().from(rs.getData())
                .withHb(HB_MONITOR_NO).build();
        PartitionGroupData pgM = PartitionGroupData.builder()
                .from(pg.getData()).withCopy(pg.getData().getCopy() - 1)
                .withQuorum(pg.getData().getQuorum() - 1).build();

        Cluster cluster = clusterImo.get(pgs.getClusterName());

        List<Op> opList = new ArrayList<Op>();
        opList.add(pgsDao.createUpdatePgsOperation(pgs.getPath(), pgsM));
        opList.add(pgsDao.createUpdateRsOperation(rs.getPath(), rsM));
        opList.add(pgDao.createUpdatePgOperation(pg.getPath(), pgM));
        opList.add(notificationDao
                .createGatewayAffinityUpdateOperation(cluster));

        List<OpResult> results = null;
        try {
            results = zookeeper.multi(opList);
        } finally {
            zookeeper.handleResultsOfMulti(results);
        }

        OpResult.SetDataResult rsd = (OpResult.SetDataResult) results.get(0);
        pgs.setData(pgsM);
        pgs.setStat(rsd.getStat());

        rsd = (OpResult.SetDataResult) results.get(1);
        rs.setData(rsM);
        rs.setStat(rsd.getStat());

        pg.setData(pgM);
    }
}
