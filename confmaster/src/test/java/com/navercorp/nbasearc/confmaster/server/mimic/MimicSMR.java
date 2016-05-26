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

package com.navercorp.nbasearc.confmaster.server.mimic;

import static com.navercorp.nbasearc.confmaster.Constant.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

// Leaky-spec of smr-replicator, mimicking some behaviors for test.
public class MimicSMR implements Answer<String>, IMimic<String, String>,
        IInjectable<String, String> {
    int quorum;
    long min, commit, max, besent;
    long timestamp;
    String role;
    boolean unstable;
    Random rand = new Random(System.currentTimeMillis());
    volatile IInjector<String, String> injector = null;
    
    public long getTimestamp() {
        return timestamp;
    }

    public String getSeqLogReply() {
        return "+OK log min:" + min + " commit:" + commit + " max:" + max
                + " be_sent:" + besent;
    }
    
    public void init() {
        quorum = 0;
        min = commit = max = besent = 0;
        timestamp = System.currentTimeMillis();
        role = PGS_ROLE_LCONN;
        timestamp++;
    }

    public void setUnstable(boolean unstable) {
        this.unstable = unstable;
    }
    
    public void setSeqLog(long min, long commit, long max, long besent) {
        this.min = min;
        this.commit = commit;
        this.max = max;
        this.besent = besent;
    }
    
    @Override
    public String answer(InvocationOnMock invocation) throws IOException {
        return execute((String) invocation.getArguments()[0]);
    }
    
    @Override
    public synchronized String execute(String cmd) {
        String[] args = cmd.split(" ");
        
        if (unstable) {
            int r = rand.nextInt(10);
            if (r < 5) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                return "";
            } else if (r < 8) {
                return "";
            }
        } else if (injector != null) {
            String resp = injector.inject(cmd);
            if (resp != null) {
                return resp;
            }
        }
        
        if (args[0].equals("getquorum")) {
            return getQuorum();
        } else if (args[0].equals("setquorum")) {
            return setQuorum(Arrays.copyOfRange(args, 1, args.length));
        } else if (args[0].equals("getseq") && args[1].equals("log")) {
            return getSeqlog();
        } else if (args[0].equals(PGS_PING)) {
            return ping();
        } else if (args[0].equals(REDIS_PING)) {
            return bping();
        } else if (args[0].equals("role")) {
            if (args[1].equals("master")) {
                if (args.length != 3+2) {
                    return "-ERR need <nid> <quorum policy> <rewind cseq>";
                }
                return roleMaster(Arrays.copyOfRange(args, 2, args.length));
            } else if (args[1].equals("slave")) {
                if (args.length != 3+2 && args.length != 4+2) {
                    return "-ERR <nid> <host> <base port> [<rewind cseq>]";
                }
                return roleSlave(Arrays.copyOfRange(args, 2, args.length));
            } else if (args[1].equals("lconn")) {
                return roleLconn();
            } else if (args[1].equals("none")) {
                return roleNone();
            } else if (args[1].equals("null")) {
                return roleNull();
            }
        }
        return "-ERR bad request: unsupported cmd " + cmd;
    }
    
    private String getQuorum() {
        if (!role.equals(PGS_ROLE_MASTER)) {
            return "-ERR bad state:" + PartitionGroupServer.roleToNumber(role);
        }
        return String.valueOf(quorum);
    }
    
    private String setQuorum(String[] args) {
        if (!role.equals(PGS_ROLE_MASTER)) {
            return "-ERR bad state:" + PartitionGroupServer.roleToNumber(role);
        }
        quorum = Integer.parseInt(args[0]);
        return S2C_OK;
    }
    
    private String getSeqlog() {
        return getSeqLogReply();
    }
    
    private String ping() {
        if (role != null) {
            return "+OK " + PartitionGroupServer.roleToNumber(role) + " " + timestamp;
        } 
        return "";
    }
    
    public String bping() {
        if (role != null) {
            return REDIS_PONG;
        } 
        return "";
    }
    
    private String roleMaster(String[] args) {
        if (role.equals(PGS_ROLE_LCONN)) {
            role = PGS_ROLE_MASTER;
            quorum = Integer.valueOf(args[1]);
            timestamp++;
            return S2C_OK;
        }
        return "-ERR do_role_master failed";
    }

    private String roleSlave(String[] args) {
        if (role.equals(PGS_ROLE_LCONN)) {
            role = PGS_ROLE_SLAVE;
            timestamp++;
            return S2C_OK;
        }
        return "-ERR do_role_slave failed";
    }

    private String roleLconn() {
        if (role.equals(PGS_ROLE_LCONN)
                || role.equals(PGS_ROLE_MASTER) 
                || role.equals(PGS_ROLE_SLAVE)) {
            role = PGS_ROLE_LCONN;
            timestamp++;
            return S2C_OK;
        }
        return "-ERR failed";
    }
    
    private String roleNone() {
        role = PGS_ROLE_NONE;
        timestamp++;
        return S2C_OK;
    }
    
    private String roleNull() {
        role = null;
        timestamp++;
        return S2C_OK;
    }

    @Override
    public synchronized void setInjector(IInjector<String, String> injector) {
        this.injector = injector; 
    }

}