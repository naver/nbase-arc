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

import org.codehaus.jackson.type.TypeReference;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.repository.znode.FailureDetectorData;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;

public class FailureDetector extends ZNode<FailureDetectorData> {
    
    private int numberOfHeartbeatCheckers = 0;
    
    public FailureDetector(ApplicationContext context, String path, String name, byte[] data) {
        super(context);
        
        setTypeRef(new TypeReference<FailureDetectorData>(){});
    
        setPath(path);
        setName(name);
        setNodeType(NodeType.FD);
        setData(data);
    }
    
    public int getMajority() {
        return getNumberOfHeartbeatCheckers() / 2 + 1;
    }

    public int getNumberOfHeartbeatCheckers() {
        return numberOfHeartbeatCheckers;
    }

    public void setNumberOfHeartbeatCheckers(int numberOfHeartbeatChecker) {
        this.numberOfHeartbeatCheckers = numberOfHeartbeatChecker;
    }

}
