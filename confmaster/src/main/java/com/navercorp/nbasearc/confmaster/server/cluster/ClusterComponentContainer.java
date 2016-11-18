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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import org.springframework.stereotype.Component;

@Component
public class ClusterComponentContainer {
    
    protected ConcurrentSkipListMap<String, ClusterComponent> map = 
            new ConcurrentSkipListMap<String, ClusterComponent>(new PathComparator());

    public void relase() {
        map.clear();
    }

    public void put(String path, ClusterComponent obj) {
        map.put(path, obj);
    }

    public void delete(String path) {
        ClusterComponent o = map.get(path);
        o.release();
        map.remove(path);
    }
    
    public ClusterComponent get(String path) {
        return map.get(path);
    }

    private class PathComparator implements Comparator<String> {
        public int compare(String s1, String s2) {
            final int d = depth(s1).compareTo(depth(s2));
            if (d == 0) {
                return s1.compareTo(s2);
            } else {
                return d;
            }
        }

        public Integer depth(String s) {
            final char delimiter = '/';
            Integer cnt = 0;
            int fromIdx = -1;
            while ((fromIdx = s.indexOf(delimiter, fromIdx)) != -1) {
                cnt += 1;
                fromIdx += 1;
            }
            return cnt;
        }
    }
    
    public PhysicalMachine getPm(String pmName) {
        return (PhysicalMachine) map.get(PathUtil.pmPath(pmName));
    }
    
    public List<PhysicalMachine> getAllPm() {
        List<PhysicalMachine> pml = new ArrayList<PhysicalMachine>();
        for (Entry<String, ClusterComponent> e : 
            map.subMap(PathUtil.PM_PATH_S, true, PathUtil.PM_PATH_E, true).entrySet()) {
            pml.add((PhysicalMachine) e.getValue());
        }
        return pml;
    }
    
    public Cluster getCluster(String clusterName) {
        return (Cluster) map.get(PathUtil.clusterPath(clusterName));
    }
    
    public List<Cluster> getAllCluster() {
        List<Cluster> cl = new ArrayList<Cluster>();
        for (Entry<String, ClusterComponent> e : 
            map.subMap(PathUtil.CLUSTER_PATH_S, true, PathUtil.CLUSTER_PATH_E, true).entrySet()) {
            cl.add((Cluster) e.getValue());
        }
        return cl;
    }
    
    public PartitionGroup getPg(String clusterName, String pgId) {
        return (PartitionGroup) map.get(PathUtil.pgPath(pgId, clusterName));
    }
    
    public List<PartitionGroup> getPgList(String clusterName) {
        List<PartitionGroup> pgl = new ArrayList<PartitionGroup>();
        for (Entry<String, ClusterComponent> e : 
            map.subMap(PathUtil.PG_PATH_S(clusterName), true, PathUtil.PG_PATH_E(clusterName), true).entrySet()) {
            pgl.add((PartitionGroup) e.getValue());
        }
        return pgl;
    }
    
    public PartitionGroupServer getPgs(String clusterName, String pgsId) {
        return (PartitionGroupServer) map.get(PathUtil.pgsPath(pgsId, clusterName));
    }
    
    public List<PartitionGroupServer> getPgsList(String clusterName) {
        List<PartitionGroupServer> pgsl = new ArrayList<PartitionGroupServer>();
        for (Entry<String, ClusterComponent> e : 
            map.subMap(PathUtil.PGS_PATH_S(clusterName), true, PathUtil.PGS_PATH_E(clusterName), true).entrySet()) {
            pgsl.add((PartitionGroupServer) e.getValue());
        }
        return pgsl;
    }
    
    public List<PartitionGroupServer> getPgsList(String clusterName, String pgName) {
        final int pgId = Integer.valueOf(pgName);
        List<PartitionGroupServer> pgsl = new ArrayList<PartitionGroupServer>();
        for (Entry<String, ClusterComponent> e : 
            map.subMap(PathUtil.PGS_PATH_S(clusterName), true, PathUtil.PGS_PATH_E(clusterName), true).entrySet()) {
            PartitionGroupServer pgs = (PartitionGroupServer) e.getValue();
            if (pgs.getPgId() == pgId) {
                pgsl.add(pgs);
            }
        }
        return pgsl;
    }
    
    public RedisServer getRs(String clusterName, String pgsId) {
        return (RedisServer) map.get(PathUtil.rsPath(pgsId, clusterName));
    }
    
    public List<RedisServer> getRsList(String clusterName) {
        List<RedisServer> rsl = new ArrayList<RedisServer>();
        for (Entry<String, ClusterComponent> e : 
            map.subMap(PathUtil.RS_PATH_S(clusterName), true, PathUtil.RS_PATH_E(clusterName), true).entrySet()) {
            rsl.add((RedisServer) e.getValue());
        }
        return rsl;
    }
    
    public List<RedisServer> getRsList(String clusterName, String pgName) {
        final int pgId = Integer.valueOf(pgName);
        List<RedisServer> rsl = new ArrayList<RedisServer>();
        for (Entry<String, ClusterComponent> e : 
            map.subMap(PathUtil.RS_PATH_S(clusterName), true, PathUtil.RS_PATH_E(clusterName), true).entrySet()) {
            RedisServer rs = (RedisServer) e.getValue();
            if (rs.getPgId() == pgId) {
                rsl.add(rs);
            }
        }
        return rsl;
    }
    
    public Gateway getGw(String clusterName, String gwId) {
        return (Gateway) map.get(PathUtil.gwPath(gwId, clusterName));
    }
    
    public List<Gateway> getGwList(String clusterName) {
        List<Gateway> gwl = new ArrayList<Gateway>();
        for (Entry<String, ClusterComponent> e : 
            map.subMap(PathUtil.GW_PATH_S(clusterName), true, PathUtil.GW_PATH_E(clusterName), true).entrySet()) {
            gwl.add((Gateway) e.getValue());
        }
        return gwl;
    }
    
    public PhysicalMachineCluster getPmc(String pmName, String clusterName) {
        return (PhysicalMachineCluster) map.get(PathUtil.pmClusterPath(clusterName, pmName));
    }
    
    public List<PhysicalMachineCluster> getPmcList(String pmName) {
        List<PhysicalMachineCluster> pmcl = new ArrayList<PhysicalMachineCluster>();
        for (Entry<String, ClusterComponent> e : 
            map.subMap(PathUtil.PMC_PATH_S(pmName), true, PathUtil.PMC_PATH_E(pmName), true).entrySet()) {
            pmcl.add((PhysicalMachineCluster) e.getValue());
        }
        return pmcl;
    }
    
}
