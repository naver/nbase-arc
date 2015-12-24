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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.navercorp.nbasearc.confmaster.ConfMasterException;
import com.navercorp.nbasearc.confmaster.repository.znode.ClusterBackupScheduleData;

public class ClusterBackupSchedule {
    
    private Map<Integer, ClusterBackupScheduleData> backupSchedules = 
            new HashMap<Integer, ClusterBackupScheduleData>();
    
    public ClusterBackupSchedule() {
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(); 
        
        sb.append("[");
        
        for (Entry<Integer, ClusterBackupScheduleData> entry : getBackupSchedules()
                .entrySet()) {
            sb.append(entry.getValue().toString()).append(",");
        }

        if (getBackupSchedules().size() != 0) {
            sb.setLength(sb.length() - 1);
        }
        sb.append("]");
        
        return sb.toString();
    }

    public void addBackupSchedule(ClusterBackupScheduleData backupSchedule) 
            throws ConfMasterException {
        if (getBackupSchedules().get(backupSchedule.getBackup_id()) != null) {
            throw new ConfMasterException("Backup '"
                    + backupSchedule.getBackup_id() + "' is already exist.");
        }
        
        getBackupSchedules().put(backupSchedule.getBackup_id(), backupSchedule);
    }

    public void updateBackupSchedule(ClusterBackupScheduleData backupSchedule) 
            throws ConfMasterException {
        ClusterBackupScheduleData old = getBackupSchedules().get(backupSchedule.getBackup_id());
        if (old == null) {
            throw new ConfMasterException("Backup '"
                    + backupSchedule.getBackup_id() + "' is not exist.");
        }

        backupSchedule.setVersion(old.getVersion() + 1);
        getBackupSchedules().put(backupSchedule.getBackup_id(), backupSchedule);
    }
    
    public void deleteBackupSchedule(int backupScheduleId) throws ConfMasterException {
        if (getBackupSchedules().remove(backupScheduleId) == null) {
            throw new ConfMasterException("Backup '" + backupScheduleId + "' is not exist.");
        }
    }
    
    public ClusterBackupScheduleData getBackupSchedule(int backupScheduleId) {
        return backupSchedules.get(backupScheduleId);
    }
    
    public boolean existBackupJob(int backupID) {
        return backupSchedules.containsKey(backupID);
    }

    public Map<Integer, ClusterBackupScheduleData> getBackupSchedules() {
        return backupSchedules;
    }

    public void setBackupSchedules(Map<Integer, ClusterBackupScheduleData> backupSchedules) {
        this.backupSchedules = backupSchedules;
    }

}
