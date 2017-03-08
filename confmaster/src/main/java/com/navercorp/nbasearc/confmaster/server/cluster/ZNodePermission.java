package com.navercorp.nbasearc.confmaster.server.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer.PathComparator;
import com.navercorp.nbasearc.confmaster.server.lock.LockType;


public class ZNodePermission {
    private final Map<LockType, TreeSet<String>> permissions = new HashMap<LockType, TreeSet<String>>(2);
    
    public ZNodePermission() {
        for (LockType type : LockType.values()) {
            permissions.put(type, new TreeSet<String>(new PathComparator()));
        }
    }
    
    public void addPermission(String path, LockType type) {
        permissions.get(type).add(path);
    }
    
    public void clearPermission(String path, LockType type) {
        permissions.get(type).remove(path);
    }
    
    public void checkPermission(final String path, final LockType type) throws MgmtZooKeeperException {
        for (Map.Entry<LockType, TreeSet<String>> perm: permissions.entrySet()) {
            if (perm.getKey().getLevel() < type.getLevel()) {
                continue;
            }

            String subPath = path;
            int endIndex = subPath.length();
            while (endIndex > 0) {
                subPath = subPath.substring(0, endIndex);
                if (perm.getValue().contains(subPath)) {
                    return;
                }

                endIndex = subPath.lastIndexOf('/');
            }
        }
        
        throw new MgmtZooKeeperException("No permission for " + path + ", permissions: " + permissions);
    }
    
}