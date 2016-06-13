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

package com.navercorp.nbasearc.confmaster.repository.znode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class PgDataBuilder {
    
    private List<Integer> pgsIdList;
    private ConcurrentSkipListMap<Integer, Long> masterGenMap;
    private Integer copy;
    private Integer quorum;

    public PgDataBuilder from(PartitionGroupData data) {
        withPgsIdList(data.getPgsIdList());
        withMasterGenMap(data.getMasterGenMap());
        withCopy(data.getCopy());
        withQuorum(data.getQuorum());
        return this;
    }
    
    public PgDataBuilder withPgsIdList(List<Integer> pgsIdList) {
        this.pgsIdList = pgsIdList;
        return this;
    }

    public PgDataBuilder withMasterGenMap(ConcurrentSkipListMap<Integer, Long> masterGenMap) {
        this.masterGenMap = masterGenMap;
        return this;
    }
    
    public PgDataBuilder withCopy(Integer copy) {
        this.copy = copy;
        return this;
    }
    
    public PgDataBuilder withQuorum(Integer quorum) {
        this.quorum = quorum;
        return this;
    }
    
    public  PgDataBuilder addPgsId(Integer id) {
        pgsIdList.add(id);
        return this;
    }
    
    public  PgDataBuilder deletePgsId(Integer id) {
        pgsIdList.remove(id);
        return this;
    }
    
    public  PgDataBuilder deletePgsId(String id) {
        pgsIdList.remove(Integer.valueOf(id));
        return this;
    }

    public PgDataBuilder addMasterGen(long last_commit_seq) {
        int gen;
        if (masterGenMap.isEmpty()) {
            gen = 0;
        } else {
            gen = masterGenMap.lastKey() + 1;
        }
        masterGenMap.put(gen, last_commit_seq);
        return this;
    }

    public PgDataBuilder cleanMGen(int mgenHistorySize) {
        final int lastKey = masterGenMap.lastKey();
        for (Map.Entry<Integer, Long> e : masterGenMap.entrySet()) {
            if (e.getKey() <= lastKey - mgenHistorySize) {
                masterGenMap.remove(e.getKey());
            }
        }
        return this;
    }
    
    public PartitionGroupData build() {
        PartitionGroupData data = new PartitionGroupData();
        data.setPgsIdList(pgsIdList);
        data.setMaster_Gen_Map(masterGenMap);
        data.setCopy(copy);
        data.setQuorum(quorum);
        return data;
    }
    
}
