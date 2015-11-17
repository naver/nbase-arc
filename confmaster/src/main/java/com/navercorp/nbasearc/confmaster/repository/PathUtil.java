package com.navercorp.nbasearc.confmaster.repository;

import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;

public class PathUtil {

    public static final String RC = "RC";
    public static final String CC = "CC";
    public static final String LE = "LE";
    public static final String FD = "FD";
    public static final String PM = "PM";
    public static final String CLUSTER = "CLUSTER";
    public static final String APP_DATA = "APP_DATA";
    public static final String PG = "PG";
    public static final String PGS = "PGS";
    public static final String RS = "RS";
    public static final String GW = "GW";

    private static String ROOT_PATH_OF_RC = "/" + RC;
    private static String ROOT_PATH_OF_CC = ROOT_PATH_OF_RC + "/" + CC;
    private static String ROOT_PATH_OF_CC_LE = ROOT_PATH_OF_CC + "/" + LE;
    private static String ROOT_PATH_OF_FD = ROOT_PATH_OF_CC + "/" + FD;
    private static String ROOT_PATH_OF_PM = ROOT_PATH_OF_RC + "/" + PM;
    private static String ROOT_PATH_OF_CLUSTER = ROOT_PATH_OF_RC + "/" + CLUSTER;

    public static String rcRootPath() {
        return ROOT_PATH_OF_RC;
    }

    public static String pmRootPath() {
        return ROOT_PATH_OF_PM;
    }

    public static String pmClusterRootPath(String pmName) {
        return ROOT_PATH_OF_PM + "/" + pmName;
    }

    public static String clusterRootPath() {
        return ROOT_PATH_OF_CLUSTER;
    }

    public static String ccRootPath() {
        return ROOT_PATH_OF_CC;
    }

    public static String rootPathOfLE() {
        return ROOT_PATH_OF_CC_LE;
    }

    public static String fdRootPath() {
        return ROOT_PATH_OF_FD;
    }

    public static String pgRootPath(String cluster) {
        return ROOT_PATH_OF_CLUSTER + "/" + cluster + "/" + PG;
    }

    public static String pgsRootPath(String cluster) {
        return ROOT_PATH_OF_CLUSTER + "/" + cluster + "/" + PGS;
    }
    
    public static String rsRootPath(String cluster) {
        return ROOT_PATH_OF_CLUSTER + "/" + cluster + "/" + RS;
    }

    public static String gwRootPath(String cluster) {
        return ROOT_PATH_OF_CLUSTER + "/" + cluster + "/" + GW;
    }

    public static String pmPath(String pm) {
        return pmRootPath() + "/" + pm;
    }
    
    public static String clusterBackupSchedulePath(String cluster) {
        return clusterPath(cluster) + "/" + APP_DATA;
    }

    public static String clusterPath(String cluster) {
        return clusterRootPath() + "/" + cluster;
    }

    public static String pgPath(String pg, String cluster) {
        return pgRootPath(cluster) + "/" + pg;
    }

    public static String pgsPath(String pgs, String cluster) {
        return pgsRootPath(cluster) + "/" + pgs;
    }
    
    public static String rsPath(String rs, String cluster) {
        return rsRootPath(cluster) + "/" + rs;
    }

    public static String gwPath(String gw, String cluster) {
        return gwRootPath(cluster) + "/" + gw;
    }

    public static String pmClusterPath(String cluster, String pm) {
        return pmRootPath() + "/" + pm + "/" + cluster;
    }

    public static NodeType getNodeTypeFromPath(String path) {
        String nodeStr = path.split("/")[4];
        if (nodeStr.equals(GW)) {
            return NodeType.GW;
        } else if (nodeStr.equals(RS)) {
            return NodeType.RS;
        } else if (nodeStr.equals(PGS)) {
            return NodeType.PGS;
        }
        return NodeType.UNKOWN;
    }

    public static String getClusterNameFromPath(String path) {
        return path.split("/")[3];
    }

    public static String getClusterNameInPMFromPath(String path) {
        return path.split("/")[4];
    }
    
    public static String getPgNameFromPath(String path) {
        return path.split("/")[5];
    }
    
    public static String getPgsNameFromPath(String path) {
        return path.split("/")[5];
    }
    
    public static String getRsNameFromPath(String path) {
        return path.split("/")[5];
    }
    
    public static String getGwNameFromPath(String path) {
        return path.split("/")[5];
    }

    public static String getPmNameFromPath(String path) {
        return path.split("/")[3];
    }
    
}
