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

package com.navercorp.nbasearc.confmaster.heartbeat;

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;

import java.io.UnsupportedEncodingException;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;
import com.navercorp.nbasearc.confmaster.server.cluster.Opinion;
import com.navercorp.nbasearc.confmaster.server.cluster.Opinion.OpinionData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer.RealState;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;

@Component
public class HBResultProcessor {
    
    @Autowired
    private Config config;
    
    @Autowired
    private ZooKeeperHolder zk;
    
    @Autowired
    private Opinion opinion;

    @Autowired 
    private WorkflowExecutor workflowExecutor;
    
    public static String OPINION_FORMAT = 
            "{\"name\":\"{}\",\"opinion\":\"{}\",\"version\":{},\"state_timestamp\":{},\"creation_time\":{}}";

    public String proc(HBResult result, boolean putOpinion)
            throws NodeExistsException, MgmtZooKeeperException {
        HeartbeatTarget target = result.getTarget();
        HBState data = target.getHeartbeatState();
        
        if (target.getHeartbeat().equals(Constant.HB_MONITOR_NO)) {
            Logger.debug("{} is not a target of heartbeat. {}", 
                    target.getNodeType().toString() + target.getName(), result); 
            return null;
        }
        
        switch (target.getNodeType()) {
        case PGS:
            pgs(data, result, putOpinion);
            break;
            
        case RS:
        case GW:
            common(data, result, putOpinion);
            break;
            
        default:
            Logger.warn("An unkown type of a heartbeat target. type: {}", target.getNodeType());
            break;
        }
        
        return null;
    }
    
    private void pgs(HBState refData, HBResult result, boolean putOpinion) 
            throws MgmtZooKeeperException, NodeExistsException {
        final RealState newState = PartitionGroupServer.convertReplyToState(result.getResponse());
        long stateTimestamp = Constant.DEFAULT_STATE_TIMESTAMP;
        HBState.ZKData zkData = refData.getZkData(); 
        
        if (newState.getRole().equals(PGS_ROLE_NONE)) {
            stateTimestamp = zkData.stateTimestamp;
        } else {
            stateTimestamp = newState.getStateTimestamp();
        }
        
        if (newState.getRole().equals(refData.getLastState()) 
        		&& newState.getRole().equals(refData.getZkData().state)) {
            if (refData.isSubmitMyOpinion()) {
                if (putOpinion) {
                    removeMyOpinion(refData, result, newState.getRole());
                } else {
                    workflowExecutor.perform(OPINION_DISCARD, result.getTarget());
                }
            }
        } else {
            if (putOpinion) {
                if (refData.isSubmitMyOpinion()) {
                    removeMyOpinion(refData, result, newState.getRole());
                }
                putMyOpinion(stateTimestamp, refData, result, newState.getRole());
            } else {
                workflowExecutor.perform(OPINION_PUBLISH, result);
            }
        }
    }
    
    private void common(HBState refData, HBResult result, boolean putOpinion)
            throws MgmtZooKeeperException, NodeExistsException {
        HBState.ZKData zkData = refData.getZkData();
        
        if (zkData.state.equals(result.getState()) && refData.isSubmitMyOpinion())  {
            if (putOpinion) {
                removeMyOpinion(refData, result, result.getState());
            } else {
                workflowExecutor.perform(OPINION_PUBLISH, result);
            }
        } else if (!result.getState().equals(refData.getZkData().state) && 
                 !result.getState().equals(zkData.state))  {
            if (putOpinion) {
                putMyOpinion(0, refData, result, result.getState());
            } else {
                workflowExecutor.perform(OPINION_PUBLISH, result);
            }
        }
    }

    private void putMyOpinion(long stateTimestamp, HBState refData,
            HBResult result, String newState) throws MgmtZooKeeperException,
            NodeExistsException {
        String path = makePathOfMyOpinion(result.getTarget().getPath());

        if (refData.isSubmitMyOpinion()) {
        		try {
					OpinionData op = opinion.getOpinion(path);
					if (op.getOpinion().equals(newState) 
							&& op.getStatetimestamp() == stateTimestamp
							&& op.getVersion() == refData.getZkData().version) {
						return;
					} else {
						removeMyOpinion(refData, result, result.getState());
					}
				} catch (NoNodeException e) {
					// Ignore
				}
        }
        
        byte[] data = makeDataOfMyOpinion(refData, stateTimestamp, result, newState);
        
        try {
            zk.createEphemeralZNode(path, data);
        } catch (NodeExistsException e) {
            Logger.error("Put my opinion fail. path: {}, opinion: {}", 
                    path, makeStringOfMyOpinion(refData, stateTimestamp, newState), e);
            throw e;
        } catch (MgmtZooKeeperException e) {
            Logger.error("Put my opinion fail. path: {}, opinion: {}", 
                    path, makeStringOfMyOpinion(refData, stateTimestamp, newState), e);
            throw e;
        }
        
        Logger.info("Put " + result.toString());
        
        refData.setLastState(newState);
        refData.setLastStateTimestamp(stateTimestamp);
        refData.setSubmitMyOpinion(true);
    }
    
    private void removeMyOpinion(HBState refData, HBResult result,
            String newView) throws MgmtZooKeeperException {
        String path = makePathOfMyOpinion(result.getTarget().getPath());

        zk.deleteZNode(path, -1);

        refData.setSubmitMyOpinion(false);
    }
    
    private String makePathOfMyOpinion(String targetPath) {
        String path = targetPath + "/" + config.getIp() + ":" + config.getPort();
        return path;
    }

    private byte[] makeDataOfMyOpinion(HBState data, long stateTimestamp,
            HBResult result, String newState) {
        String jsonData = makeStringOfMyOpinion(data, stateTimestamp, newState);
        try {
            return jsonData.getBytes(config.getCharset());
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(config.getCharset() + " is unknown.");
        }
    }
    
    private String makeStringOfMyOpinion(HBState data, long stateTimestamp, String newState) {
        HBState.ZKData zkData = data.getZkData(); 
        return MessageFormatter.arrayFormat(OPINION_FORMAT,
            new Object[] { config.getIp() + ":" + config.getPort(),
                            newState, zkData.version, stateTimestamp, System.currentTimeMillis() });
    }
    
}

