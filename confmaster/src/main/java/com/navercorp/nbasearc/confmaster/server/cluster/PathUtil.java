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

package com.navercorp.nbasearc.confmaster.server.cluster;

/*
                                             <RC>
                                              |
        +--------------+----------------------+-------+---------------------------------+
        |              |                              |                                 |
  <NOTOFICATION>      <PM>                        <CLUSTER>                            <CC>
        |              |                              |                                 |
        |              |                              +                     +-----------+---------------+
        |              |                              |                     |           |               |
    <CLUSTER>      test01.pm                     test_cluster             <LOG>        <FD>            <LE>
        |              |                              |                                 |               |
        +              +            +--------+--------+--------+---------+              +               +
        |              |            |        |        |        |         |              |               |
   test_cluster   test_cluster <APP_DATA>   <RS>     <PG>     <GW>     <PGS>    192.168.0.10:1122  n_0000000001
        |                                    |        |        |         |
    +---+---+                             +--+--+  +--+--+  +--+--+   +--+--+
    |       |                             |     |  |     |  |     |   |     |
<AFFINITY> <GW>                           0     1  0     1  1     2   0     1
            |
         +--+--+
         |     |
         1     2
 */
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
    private static final String NOTIFICATION = "NOTIFICATION";
    private static final String AFFINITY = "AFFINITY";

    private static String ROOT_PATH_OF_RC = "/" + RC;
    private static String ROOT_PATH_OF_CC = ROOT_PATH_OF_RC + "/" + CC;
    private static String ROOT_PATH_OF_CC_LE = ROOT_PATH_OF_CC + "/" + LE;
    private static String ROOT_PATH_OF_FD = ROOT_PATH_OF_CC + "/" + FD;
    private static String ROOT_PATH_OF_PM = ROOT_PATH_OF_RC + "/" + PM;
    private static String ROOT_PATH_OF_CLUSTER = ROOT_PATH_OF_RC + "/" + CLUSTER;

    private static final String ROOT_PATH_OF_GW_LOOKUP = 
            PathUtil.rcRootPath() + "/" + NOTIFICATION;
    private static final String ROOT_PATH_OF_GW_LOOKUP_CLUSTER = 
            ROOT_PATH_OF_GW_LOOKUP + "/" + CLUSTER;

    public static final String CLUSTER_PATH_S = "/RC/CLUSTER/";
    public static final String CLUSTER_PATH_E = CLUSTER_PATH_S + Character.MAX_VALUE;

    public static final String PM_PATH_S = "/RC/PM/";
    public static final String PM_PATH_E = PM_PATH_S + Character.MAX_VALUE;

    public static String PG_PATH_S(final String clusterName) {
        return "/RC/CLUSTER/" + clusterName + "/PG/";
    }
    
    public static String PG_PATH_E(final String clusterName) {
        return PG_PATH_S(clusterName) + Character.MAX_VALUE;
    }

    public static String PGS_PATH_S(final String clusterName) {
        return "/RC/CLUSTER/" + clusterName + "/PGS/";
    }
    
    public static String PGS_PATH_E(final String clusterName) {
        return PGS_PATH_S(clusterName) + Character.MAX_VALUE;
    }

    public static String RS_PATH_S(final String clusterName) {
        return "/RC/CLUSTER/" + clusterName + "/RS/";
    }
    
    public static String RS_PATH_E(final String clusterName) {
        return RS_PATH_S(clusterName) + Character.MAX_VALUE;
    }

    public static String GW_PATH_S(final String clusterName) {
        return "/RC/CLUSTER/" + clusterName + "/GW/";
    }
    
    public static String GW_PATH_E(final String clusterName) {
        return GW_PATH_S(clusterName) + Character.MAX_VALUE;
    }

    public static String PMC_PATH_S(final String pmName) {
        return "/RC/PM/" + pmName + "/";
    }
    
    public static String PMC_PATH_E(final String pmName) {
        return PMC_PATH_S(pmName) + Character.MAX_VALUE;
    }

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

    public static String rootPathOfGwLookup() {
        return ROOT_PATH_OF_GW_LOOKUP;
    }
    
    public static String rootPathOfGwLookupCluster() {
        return ROOT_PATH_OF_GW_LOOKUP_CLUSTER;
    }
    
    public static String rootPathOfGwLookup(String clusterName) {
        return pathOfGwLookupCluster(clusterName) + "/" + GW;
    }
    
    public static String pathOfGwLookupCluster(String clusterName) {
        return rootPathOfGwLookupCluster() + "/" + clusterName;
    }
    
    public static String pathOfGwLookup(String clusterName, String gatewayName) {
        return rootPathOfGwLookup(clusterName) + "/" + gatewayName;
    }
    
    public static String pathOfGwAffinity(String clusterName) {
        return pathOfGwLookupCluster(clusterName) + "/" + AFFINITY;
    }

    public static class WatchTarget {
        public NodeType nodeType;
        public String clusterName;
        public String name;
        
        public WatchTarget(NodeType t, String cn, String n) {
            nodeType = t;
            clusterName = cn;
            name = n;
        }

        public WatchTarget(NodeType t, String cn) {
            nodeType = t;
            clusterName = cn;
        }

        public WatchTarget(NodeType t) {
            nodeType = t;
        }
    }
    
    public static WatchTarget getWatchType(String path) {
        String []znodeNames = path.split("/");
        
        switch (znodeNames.length) {
        case 6:
            String clusterName = znodeNames[3];
            String type = znodeNames[4];
            String name = znodeNames[5];
            if (type.equals("PGS")) {
                return new WatchTarget(NodeType.PGS, clusterName, name);
            } else if (type.equals("RS")) {
                return new WatchTarget(NodeType.RS, clusterName, name);
            } else if (type.equals("GW")) {
                return new WatchTarget(NodeType.GW, clusterName, name);
            } else if (type.equals("PG")) {
                return new WatchTarget(NodeType.PG, clusterName, name);
            }
            break;
            
        case 5:
            clusterName = znodeNames[3];
            type = znodeNames[4];
            if (type.equals("PGS")) {
                return new WatchTarget(NodeType.PGS_ROOT, clusterName);
            } else if (type.equals("RS")) {
                return new WatchTarget(NodeType.RS_ROOT, clusterName);
            } else if (type.equals("GW")) {
                return new WatchTarget(NodeType.GW_ROOT, clusterName);
            } else if (type.equals("PG")) {
                return new WatchTarget(NodeType.PG_ROOT, clusterName);
            }
            break;
            
        case 4:
            if (znodeNames[3].equals("FD")) {
                return new WatchTarget(NodeType.FD);
            } else if (znodeNames[2].equals("CLUSTER")) {
                return new WatchTarget(NodeType.CLUSTER, znodeNames[3]);
            }
            break;
            
        case 3:
            if (path.equals(ROOT_PATH_OF_CLUSTER)) {
                return new WatchTarget(NodeType.CLUSTER_ROOT);
            }
            break;
        }
        
        return null;
    }
    
}
