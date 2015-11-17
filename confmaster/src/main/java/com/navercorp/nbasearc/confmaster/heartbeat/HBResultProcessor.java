package com.navercorp.nbasearc.confmaster.heartbeat;

import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.OPINION_PUBLISH;

import java.io.UnsupportedEncodingException;

import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;

@Component
public class HBResultProcessor {
    
    @Autowired
    private Config config;
    
    @Autowired
    private ZooKeeperHolder zookeeper;

    @Autowired 
    private WorkflowExecutor workflowExecutor;
    
    public static String OPINION_FORMAT = 
            "{\"name\":\"{}\",\"opinion\":\"{}\",\"version\":{},\"state_timestamp\":{}}";

    public String proc(HBResult result, boolean putOpinion)
            throws NodeExistsException, MgmtZooKeeperException {
        HeartbeatTarget target = result.getTarget();
        HBRefData data = target.getRefData();
        
        if (target.getHB().equals(Constant.HB_MONITOR_NO)) {
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
    
    private void pgs(HBRefData refData, HBResult result, boolean putOpinion) 
            throws MgmtZooKeeperException, NodeExistsException {
        String newState = result.getState();
        long stateTimestamp = Constant.DEFAULT_STATE_TIMESTAMP;
        HBRefData.ZKData zkData = refData.getZkData(); 
        
        if (result.getState().equals(Constant.SERVER_STATE_NORMAL)) {
            String[] resAry = result.getResponse().split(" ");
            if (resAry.length < 3) {
                Logger.error("Invalid response. from: {}, response: {}", 
                        result.getRemoteIP() + ":" + result.getRemoteIP(),
                        result.getResponse());
                newState = Constant.SERVER_STATE_FAILURE;
            }
            
            if (resAry[1].equals("1")) {
                newState = Constant.SERVER_STATE_LCONN;
            }
            else if (resAry[1].equals("0")) {
                newState = Constant.SERVER_STATE_FAILURE;
            }
            
            stateTimestamp = Long.parseLong(resAry[2]);
        } else if (result.getState().equals(Constant.SERVER_STATE_FAILURE)) {
            stateTimestamp = zkData.stateTimestamp + 1;
        }
        
        if (stateTimestamp == zkData.stateTimestamp && newState.equals(zkData.state)) {
            if (refData.isSubmitMyOpinion()) {
                if (putOpinion) {
                    removeMyOpinion(refData, result, newState);
                } else {
                    workflowExecutor.perform(OPINION_PUBLISH, result);
                }
            }
        } else if (newState.equals(zkData.state) && 
                 newState.equals(Constant.SERVER_STATE_FAILURE) && 
                 stateTimestamp - 1 == zkData.stateTimestamp) {
            if (refData.isSubmitMyOpinion()) {
                if (putOpinion) {
                    removeMyOpinion(refData, result, newState);
                } else {
                    workflowExecutor.perform(OPINION_PUBLISH, result);
                }
            }
        }
        /**
         * HBC assumes that dead PGS`s version is zk_version + 1, because Dead
         * PGS`s version(timestamp) is 0. so this routine is located at the
         * bottom of these conditional statements. Otherwise, if PGS was in
         * dead, then this function increases PGS`s version and puts an opinion
         * to the ZooKeeper repeatedly.
         */
        else if (!newState.equals(refData.getLastState()) || 
                 stateTimestamp != refData.getLastStateTimestamp()) {
            if (putOpinion) {
                if (refData.isSubmitMyOpinion()) {
                    removeMyOpinion(refData, result, newState);
                }
                putMyOpinion(stateTimestamp, refData, result, newState);
            } else {
                workflowExecutor.perform(OPINION_PUBLISH, result);
            }
        }
    }
    
    private void common(HBRefData refData, HBResult result, boolean putOpinion)
            throws MgmtZooKeeperException, NodeExistsException {
        HBRefData.ZKData zkData = refData.getZkData();
        
        if (zkData.state.equals(result.getState()) && refData.isSubmitMyOpinion())  {
            if (putOpinion) {
                removeMyOpinion(refData, result, result.getState());
            } else {
                workflowExecutor.perform(OPINION_PUBLISH, result);
            }
        } else if (!result.getState().equals(refData.getLastState()) && 
                 !result.getState().equals(zkData.state))  {
            if (putOpinion) {
                if (refData.isSubmitMyOpinion()) 
                    removeMyOpinion(refData, result, result.getState());
                putMyOpinion(0, refData, result, result.getState());
            } else {
                workflowExecutor.perform(OPINION_PUBLISH, result);
            }
        }
    }

    private void putMyOpinion(long stateTimestamp, HBRefData refData,
            HBResult result, String newState) throws MgmtZooKeeperException,
            NodeExistsException {
        String path = makePathOfMyOpinion(result.getTarget().getTargetOfHeartbeatPath());
        byte[] data;
        
        data = makeDataOfMyOpinion(refData, stateTimestamp, result, newState);
        
        try {
            zookeeper.createEphemeralZNode(path, data);
        } catch (NodeExistsException e) {
            Logger.error("Put my opinion fail. path: {}, opinion: {}", 
                    path, makeStringOfMyOpinion(refData, stateTimestamp, newState), e);
            throw e;
        } catch (MgmtZooKeeperException e) {
            Logger.error("Put my opinion fail. path: {}, opinion: {}", 
                    path, makeStringOfMyOpinion(refData, stateTimestamp, newState), e);
            throw e;
        }
        
        Logger.info(result.toString());
        
        refData.setLastState(newState);
        refData.setLastStateTimestamp(stateTimestamp);
        refData.setSubmitMyOpinion(true);
    }
    
    private void removeMyOpinion(HBRefData refData, HBResult result,
            String newState) throws MgmtZooKeeperException {
        String path = makePathOfMyOpinion(result.getTarget().getTargetOfHeartbeatPath());

        zookeeper.deleteZNode(path, -1);

        Logger.info(result.toString());

        refData.setLastState(Constant.SERVER_STATE_UNKNOWN);
        refData.setLastStateTimestamp(0L);
        refData.setSubmitMyOpinion(false);
    }
    
    private String makePathOfMyOpinion(String targetPath) {
        String path = targetPath + "/" + config.getIp() + ":" + config.getPort();
        return path;
    }

    private byte[] makeDataOfMyOpinion(HBRefData data, long stateTimestamp,
            HBResult result, String newState) {
        String jsonData = makeStringOfMyOpinion(data, stateTimestamp, newState);
        try {
            return jsonData.getBytes(config.getCharset());
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(config.getCharset() + " is unknown.");
        }
    }
    
    private String makeStringOfMyOpinion(HBRefData data, long stateTimestamp, String newState) {
        HBRefData.ZKData zkData = data.getZkData(); 
        return MessageFormatter.arrayFormat(OPINION_FORMAT,
            new Object[] { config.getIp() + ":" + config.getPort(),
                            newState, zkData.version, stateTimestamp });
    }
    
}

