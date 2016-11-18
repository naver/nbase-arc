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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.navercorp.nbasearc.confmaster.server.cluster.Opinion.OpinionData;

public class UsedOpinionSet {
    
    private List<OpinionData> usedOpinions = new LinkedList<OpinionData>();
    private List<OpinionData> deleted = new LinkedList<OpinionData>();
    
    public void update(List<OpinionData> opinions) {
        for (OpinionData o : usedOpinions) {
            deleted.add(o);
        }
        
        for (OpinionData o : opinions) {
            deleted.remove(o);
        }
        
        for (OpinionData o : deleted) {
            usedOpinions.remove(o);
        }

        deleted.clear();
    }
    
    public void add(OpinionData opinion) {
        if (!usedOpinions.contains(opinion)) {
            usedOpinions.add(opinion);
        }
    }
    
    public boolean contains(OpinionData opinion) {
        return usedOpinions.contains(opinion);
    }
    
    public Collection<OpinionData> collection() {
        return usedOpinions;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (OpinionData o : usedOpinions) {
            if (sb.length() != 0) {
                sb.append(", ");
            }
            sb.append(o.toString());
        }
        return sb.toString();
    }

}
