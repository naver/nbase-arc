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

public class Constant {
    
    public static final byte[] ZERO_BYTE = new byte[0];
    
    public static final String CLUSTER_PHASE_INIT = "I";
    public static final String CLUSTER_PHASE_RUNNING = "R";

    public static final String PGS_ROLE_NONE = "N";
    public static final String PGS_ROLE_MASTER = "M";
    public static final String PGS_ROLE_SLAVE = "S";
    
    public static final String PGS_ROLE_NONE_IN_PONG = "0";
    public static final String PGS_ROLE_LCONN_IN_PONG = "1"; 
    public static final String PGS_ROLE_MASTER_IN_PONG = "2";
    public static final String PGS_ROLE_SLAVE_IN_PONG = "3";

    public static final String SERVER_STATE_UNKNOWN = "?";
    public static final String SERVER_STATE_NORMAL = "N";
    public static final String SERVER_STATE_LCONN = "L";
    public static final String SERVER_STATE_FAILURE = "F";

    public static final String HB_MONITOR_YES = "Y";
    public static final String HB_MONITOR_NO = "N";
    
    public static final String EXCEPTIONMSG_INTERNAL_ERROR = "Internal server error.";
    public static final String EXCEPTIONMSG_ZOOKEEPER = "-ERR zookeeper exception";
    public static final String EXCEPTIONMSG_WRONG_NUMBER_ARGUMENTS = "-ERR wrong number of arguments";
    public static final String EXCEPTIONMSG_INCONSISTENCE = "-ERR zookeeper znode structure is inconsistent with memory.";
    public static final String EXCEPTIONMSG_JSON_FORMATTING_ERROR = "-ERR internal json formatting error.";
    public static final String EXCEPTIONMSG_COMMAND_NOT_FOUND = "-ERR command not found";

    public static final String EXCEPTIONMSG_PHYSICAL_MACHINE_DOES_NOT_EXIST = "-ERR pm does not exist. ";
    public static final String EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST = "-ERR cluster does not exist. ";    
    public static final String EXCEPTIONMSG_GATEWAY_DOES_NOT_EXIST = "-ERR gateway does not exist. ";
    public static final String EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST = "-ERR pg does not exist. ";
    public static final String EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST = "-ERR pgs does not exist. ";
    
    public static final String EXCEPTIONMSG_PHYSICAL_MACHINE_CLUSTER_DOES_NOT_EXIST = "-ERR cluster does not in pm. ";
    public static final String EXCEPTIONMSG_PG_NOT_EMPTY = "-ERR the pg has a pgs or more. " ;
    public static final String EXCEPTIONMSG_CLUSTER_HAS_GW = "-ERR the cluster has a gw or more. ";
    public static final String EXCEPTIONMSG_CLUSTER_HAS_PG = "-ERR the cluster has a pg or more. ";
    public static final String EXCEPTIONMSG_CLUSTER_HAS_PGS = "-ERR the cluster has a pgs or more. ";
    
    public static final String NEED_TO_RECOVER_CONSISTENCE = "-ERR have to recover the zookeeper and consistence. If you want more detail information, show log messages.";
    
    public static final int SETQUORUM_ADMITTABLE_RANGE = 1024 * 1024 * 5;
    public static final int SETQUORUM_SCHEDULE_INTERVAL = 1000;
    public static final int TERMINATION_SCHEDULE_INTERVAL = 200;

    public static final String ERROR = "-ERR";
    public static final String S2C_OK = "+OK";
    
    public static final String PGS_RESPONSE_OK = S2C_OK;
    
    public static final String GW_RESPONSE_OK = S2C_OK;
    public static final String GW_PING = "ping";
    public static final String GW_PONG = "+PONG";
    public static final String REDIS_PING = "bping";
    public static final String REDIS_REPL_PING = "bping";
    public static final String REDIS_PONG = "+PONG";
    public static final String REDIS_REP_PING = "ping";
    public static final String PGS_PING = "ping";

    public static final String ALL = "all";
    public static final String ALL_IN_PG = "all_in_pg";
    
    public static final Integer KEY_SPACE_SIZE = 8192;

    public static final String SEVERITY_BLOCKER = "BLOCKER";
    public static final String SEVERITY_CRITICAL = "CRITICAL";
    public static final String SEVERITY_MAJOR = "MAJOR";
    public static final String SEVERITY_MODERATE = "MODERATE";
    public static final String SEVERITY_MINOR = "MINOR";
    
    public static final String LOG_TYPE_COMMAND = "COMMAND";
    public static final String LOG_TYPE_WORKFLOW = "WORKFLOW";

    public static final Object TEMP_ZNODE_NAME_FOR_CHILDEVENT = "temp_child_event_for_master_election";

    public static final long DEFAULT_STATE_TIMESTAMP = 0L;
    
    public static final long GW_QUERY_TIMEOUT = 3000L;

    public static final String APPDATA_TYPE_BACKUP = "backup";

    public static final char AFFINITY_TYPE_ALL = 'A';
    public static final char AFFINITY_TYPE_WRITE = 'W';
    public static final char AFFINITY_TYPE_READ = 'R';
    public static final char AFFINITY_TYPE_NONE = 'N';
    public static final String AFFINITY_ZNODE_INITIAL_DATA = "[]"; 

    public static final String ZK_ROOT_PATH = "";

    public static final String MSG_MAX_CLIENT_REACHED = "-ERR max client reached, connection closed";

}
