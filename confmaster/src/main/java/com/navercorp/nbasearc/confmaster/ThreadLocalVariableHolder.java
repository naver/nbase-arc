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

package com.navercorp.nbasearc.confmaster;

import static com.navercorp.nbasearc.confmaster.server.lock.LockType.*;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.context.ReservedCallHolder;
import com.navercorp.nbasearc.confmaster.logger.LogHistoryHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.ZNodePermission;
import com.navercorp.nbasearc.confmaster.server.lock.LockType;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowLogger;

public class ThreadLocalVariableHolder {

    /* LogHistory */
    private static final ThreadLocal<LogHistoryHolder> logHistoryHolder = 
            new ThreadLocal<LogHistoryHolder>() {
        @Override
        protected synchronized LogHistoryHolder initialValue() {
            return new LogHistoryHolder();
        }
    };
    
    public static LogHistoryHolder getLogHistoryHolder() {
        return logHistoryHolder.get();
    }
    
    /* Workflow */
    public static final ThreadLocal<ReservedCallHolder> reservedCall = 
            new ThreadLocal<ReservedCallHolder>() {
        @Override
        protected synchronized ReservedCallHolder initialValue() {
            return new ReservedCallHolder();
        }
    };
    
    public static ReservedCallHolder getReservedCallHolder() {
        return reservedCall.get();
    }
    
    /* ZNodePermission */
    private static final ThreadLocal<ZNodePermission> znodePermission = new ThreadLocal<ZNodePermission>();
    
    public static void addPermission(String path, LockType type) {
        if (znodePermission.get() == null) {
            znodePermission.set(new ZNodePermission());
            addDefaultPermission();
        }
        znodePermission.get().addPermission(path, type);
    }
    
    public static void clearPermission(String path, LockType type) {
        if (znodePermission.get() != null) {
            znodePermission.get().clearPermission(path, type);
        }
    }
    
    public static void clearAllPermission() {
        if (znodePermission.get() != null) {
            znodePermission.set(null);
        }
    }

    /**
     * Clear ZNodePermission of current-thread to null.
     * 
     * If znodePermission.get() returns null, this method skip permission-check. Otherwise, do permission-check.
     * <pre>There are two different scenarios.
     * 1. workflows, commands, watch-event for clusters do permission-check. 
     * 2. leader-election, confmaster-init/release skip permission-check.</pre>
     * It is not deterministic that what thread will allocated to the scenarios.
     * Call this method not to affect a next thread, after calling addPermission() method.  
     */
    public static void checkPermission(String path, LockType type) throws MgmtZooKeeperException {
        if (znodePermission.get() != null) {
            znodePermission.get().checkPermission(path, type);
        }
    }
    
    private static void addDefaultPermission() {
        znodePermission.get().addPermission(WorkflowLogger.rootPathOfLog(), WRITE);
    }

}
