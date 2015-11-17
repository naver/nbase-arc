package com.navercorp.nbasearc.confmaster.repository;

import static org.junit.Assert.*;

import org.junit.Test;

import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;

public class PathUtilTest {

    @Test
    public void checkPaths() {
        String clusterName = "path_test";
        String pmName = "test01.arc";
        String pgName = "0";
        String pgsName = "50";
        String gwName = "1";
        
        assertEquals(
                PathUtil.rcRootPath(), 
                "/RC");
        assertEquals(
                PathUtil.pmRootPath(), 
                "/RC/PM");
        assertEquals(
                PathUtil.pmClusterRootPath(pmName), 
                "/RC/PM/" + pmName);
        assertEquals(
                PathUtil.clusterRootPath(), 
                "/RC/CLUSTER");
        assertEquals(
                PathUtil.ccRootPath(), 
                "/RC/CC");
        assertEquals(
                PathUtil.rootPathOfLE(), 
                "/RC/CC/LE");
        assertEquals(
                PathUtil.fdRootPath(), 
                "/RC/CC/FD");
        assertEquals(
                PathUtil.pgRootPath(clusterName), 
                "/RC/CLUSTER/" + clusterName + "/PG");
        assertEquals(
                PathUtil.pgsRootPath(clusterName), 
                "/RC/CLUSTER/" + clusterName + "/PGS");
        assertEquals(
                PathUtil.rsRootPath(clusterName), 
                "/RC/CLUSTER/" + clusterName + "/RS");
        assertEquals(
                PathUtil.gwRootPath(clusterName), 
                "/RC/CLUSTER/" + clusterName + "/GW");
        
        assertEquals(
                PathUtil.pmPath(pmName), 
                "/RC/PM/" + pmName);
        assertEquals(
                PathUtil.clusterBackupSchedulePath(clusterName), 
                "/RC/CLUSTER/" + clusterName + "/APP_DATA");
        assertEquals(
                PathUtil.clusterPath(clusterName), 
                "/RC/CLUSTER/" + clusterName);
        assertEquals(
                PathUtil.pgPath(pgName, clusterName), 
                "/RC/CLUSTER/" + clusterName + "/PG/" + pgName);
        assertEquals(
                PathUtil.pgsPath(pgsName, clusterName), 
                "/RC/CLUSTER/" + clusterName + "/PGS/" + pgsName);
        assertEquals(
                PathUtil.rsPath(pgsName, clusterName), 
                "/RC/CLUSTER/" + clusterName + "/RS/" + pgsName);
        assertEquals(
                PathUtil.gwPath(gwName, clusterName), 
                "/RC/CLUSTER/" + clusterName + "/GW/" + gwName);
        assertEquals(
                PathUtil.pmClusterPath(clusterName, pmName), 
                "/RC/PM/" + pmName + "/" + clusterName);
        
        assertEquals(
                PathUtil.getNodeTypeFromPath("/RC/CLUSTER/path_test/PGS/50"), 
                NodeType.PGS);
        assertEquals(
                PathUtil.getNodeTypeFromPath("/RC/CLUSTER/path_test/RS/50"), 
                NodeType.RS);
        assertEquals(
                PathUtil.getNodeTypeFromPath("/RC/CLUSTER/path_test/GW/1"), 
                NodeType.GW);
        assertEquals(
                PathUtil.getNodeTypeFromPath("/RC/CLUSTER/path_test/ABC/50"), 
                NodeType.UNKOWN);
        
        assertEquals(
                PathUtil.getClusterNameFromPath("/RC/CLUSTER/" + clusterName), 
                clusterName);
        assertEquals(
                PathUtil.getClusterNameInPMFromPath("/RC/PM/" + pmName + "/" + clusterName), 
                clusterName);
        assertEquals(
                PathUtil.getPgNameFromPath("/RC/CLUSTER/path_test/PG/" + pgName), 
                pgName);
        assertEquals(
                PathUtil.getPgsNameFromPath("/RC/CLUSTER/path_test/PGS/" + pgsName), 
                pgsName);
        assertEquals(
                PathUtil.getRsNameFromPath("/RC/CLUSTER/path_test/RS/" + pgsName), 
                pgsName);
        assertEquals(
                PathUtil.getGwNameFromPath("/RC/CLUSTER/path_test/GW/" + gwName), 
                gwName);
        assertEquals(
                PathUtil.getPmNameFromPath("/RC/PM/" + pmName), 
                pmName);
    }

}
