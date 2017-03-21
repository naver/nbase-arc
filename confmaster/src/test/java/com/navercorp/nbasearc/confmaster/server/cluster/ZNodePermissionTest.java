package com.navercorp.nbasearc.confmaster.server.cluster;

import static org.junit.Assert.*;
import static com.navercorp.nbasearc.confmaster.server.lock.LockType.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;

public class ZNodePermissionTest {

    static List<String> clusterPaths = Arrays.asList(new String[]{
            "/RC/CLUSTER/test",
            "/RC/NOTIFICATION/test",
    });
    static List<String> clusterPaths2 = Arrays.asList(new String[]{
            "/RC/CLUSTER/testtest",
            "/RC/NOTIFICATION/testtest",
    });
    static List<String> pgPaths = Arrays.asList(new String[]{
            "/RC/CLUSTER/test/PG", 
            "/RC/CLUSTER/test/PG/0",
    });
    static List<String> pgsPaths = Arrays.asList(new String[]{
            "/RC/CLUSTER/test/PGS", 
            "/RC/CLUSTER/test/PGS/0",
    });
    static List<String> gwPaths = Arrays.asList(new String[]{
            "/RC/CLUSTER/test/GW", 
            "/RC/CLUSTER/test/GW/0",
            "/RC/NOTIFICATION/test/GW",
            "/RC/NOTIFICATION/test/GW/0",
    });
    static List<String> affinityPaths = Arrays.asList(new String[]{
            "/RC/NOTIFICATION/test/AFFINITY",
    });
    static List<String> affinityPaths2 = Arrays.asList(new String[]{
            "/RC/NOTIFICATION/testtest/AFFINITY",
    });

    void check(List<List<String>> perm, List<List<String>> allow, List<List<String>> deny) {
        ZNodePermission zp = new ZNodePermission();
        
        for (List<String> p : perm) {
            for (String path : p) {
                zp.addPermission(path, WRITE);
            }
        }

        for (List<String> a : allow) {
            for (String path : a) {
                try {
                    zp.checkPermission(path, WRITE);
                } catch (MgmtZooKeeperException e) {
                    fail(path + " is not allowed.");
                }
            }
        }

        for (List<String> d : deny) {
            for (String path : d) {
                try {
                    zp.checkPermission(path, WRITE);
                    fail(path + " is not denied.");
                } catch (MgmtZooKeeperException e) {
                }
            }
        }
    }
    
    List<List<String>> ll(final List<String> ... lary) {
        List<List<String>> ll = new ArrayList<List<String>>();
        for (List<String> l : lary) {
            ll.add(l);
        }
        return ll;
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void permissionCheck() {
        check(ll(clusterPaths), ll(clusterPaths, pgPaths, pgsPaths, gwPaths), ll(clusterPaths2, affinityPaths2));
        check(ll(clusterPaths2), ll(clusterPaths2, affinityPaths2), ll(clusterPaths, pgPaths, pgsPaths, gwPaths));
        
        check(ll(gwPaths), ll(gwPaths), ll(clusterPaths, affinityPaths, pgPaths, pgsPaths, clusterPaths2, affinityPaths2));
        check(ll(pgPaths), ll(pgPaths), ll(clusterPaths, affinityPaths, gwPaths, pgsPaths, clusterPaths2, affinityPaths2));
        check(ll(pgsPaths), ll(pgsPaths), ll(clusterPaths, affinityPaths, gwPaths, pgPaths, clusterPaths2, affinityPaths2));
    }

}