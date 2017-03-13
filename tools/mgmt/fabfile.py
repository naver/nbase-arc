#
# Copyright 2015 Naver Corp.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import importlib
import telnetlib
import json
import time
import logging
import logging.handlers
import threading
import fileinput
import sys
import os
import ast
import glob
import random
import string
from fabric.api import *
from fabric.colors import *
from fabric.contrib.console import *
from fabric.contrib.files import *
from redis_cmd import *
import remote
import cm
import show_info
import util
from gw_cmd import *
from error import *

config = None

def check_local_binary_exist(bins=None):
    print magenta("\n[localhost] Check Local Binary")
    if bins == None:
        with settings(warn_only=True):
            if local('test -e %s/redis-arc-%s' % (config.LOCAL_BINARY_PATH, config.REDIS_VERSION)).failed: return False
            if local('test -e %s/cluster-util-%s' % (config.LOCAL_BINARY_PATH, config.REDIS_VERSION)).failed: return False
            if local('test -e %s/redis-gateway-%s' % (config.LOCAL_BINARY_PATH, config.GW_VERSION)).failed: return False
            if local('test -e %s/smr-replicator-%s' % (config.LOCAL_BINARY_PATH, config.SMR_VERSION)).failed: return False
            if local('test -e %s/smr-logutil-%s' % (config.LOCAL_BINARY_PATH, config.SMR_VERSION)).failed: return False
    else:
        for bin in bins:
            if bin == 'redis':
                if local('test -e %s/redis-arc-%s' % (config.LOCAL_BINARY_PATH, config.REDIS_VERSION)).failed: return False
            elif bin == 'cluster-util':
                if local('test -e %s/cluster-util-%s' % (config.LOCAL_BINARY_PATH, config.REDIS_VERSION)).failed: return False
            elif bin == 'gateway':
                if local('test -e %s/redis-gateway-%s' % (config.LOCAL_BINARY_PATH, config.GW_VERSION)).failed: return False
            elif bin == 'smr':
                if local('test -e %s/smr-replicator-%s' % (config.LOCAL_BINARY_PATH, config.SMR_VERSION)).failed: return False
                if local('test -e %s/smr-logutil-%s' % (config.LOCAL_BINARY_PATH, config.SMR_VERSION)).failed: return False
    print green("[localhost] Check Local Binary Success\n")
    return True

def menu_add_replication():
    # Confirm mode ?
    config.confirm_mode = confirm(cyan("Confirm Mode?"))

    # Command input method
    from_prompt = confirm(cyan("Read PGS information from command line(Y) or a file(n)."))

    if from_prompt:
        # Input pgs information from command line
        s = prompt(cyan("PGS information(CLUSTER PGS_ID PG_ID PM_NAME PM_IP PORT CRONSAVE_NUM)"))
        if ' ' not in s:
            warn(red('PGS information Must be specified. Aborting...'))
            return
        if call_add_replication(s) == False: return False
    else:
        # Get input file name
        name = prompt(cyan("Input file name:"))
        for s in fileinput.FileInput(name):
            s = s.strip()
            if len(s) == 0: continue
            if s[0] == '#': continue
            if ' ' not in s:
                warn(red('PGS information Must be specified. Aborting...'))
                return
            if call_add_replication(s) == False: return False

def call_add_replication(args):
    cluster_name = args.split(' ')[0]
    pgs_id = int(args.split(' ')[1])
    pg_id = int(args.split(' ')[2])
    pm_name = args.split(' ')[3]
    pm_ip = args.split(' ')[4]
    smr_base_port = int(args.split(' ')[5])
    redis_port = smr_base_port + 9
    cronsave_num = int(args.split(' ')[6])
    log_delete_delay = 60 * 60 * 24 / cronsave_num 

    if install_pgs(cluster_name, pgs_id, pg_id, pm_name, pm_ip, smr_base_port, redis_port, cronsave_num, log_delete_delay, True) == OK:
        return True
    else:
        return False

def copy_checkpoint_from_master(cluster_name, pg_id, pm_ip, smr_base_port):
    # Set host
    host = config.USERNAME + '@' + pm_ip.encode('ascii')
    env.hosts = [host]

    # Get master PGS
    master = cm.get_master_pgs(cluster_name, pg_id)
    if master == None:
        warn(red("[%s] Get master PGS fail. CLUSTER_NAME:%s, PG_ID:%d" % (host, cluster_name, pg_id)))
        return False
    
    if config.confirm_mode and not confirm(cyan('[%s] Master PGS has IP:%s, PORT:%s, SMR_ROLE:%s. Continue?' % (host, master['ip'], master['smr_base_port'], master['smr_role']))):
        warn("Aborting at user request.")
        return False

    # Get checkpoint from master
    range = '0-8191'
    if execute(remote.get_checkpoint, master['ip'], master['redis_port'], 'dump.rdb', range , smr_base_port)[host] == False:
        warn(red("[%s] Get checkpoint fail, SRC_IP:%s, SRC_PORT:%d, FILE_NAME:%s, PN_PG_MAP:%s" % (host, master['ip'], master['redis_port'], 'dump.rdb', range)))
        return False

    return True

def menu_leave_replication():
    # Confirm mode ?
    config.confirm_mode = confirm(cyan("Confirm Mode?"))

    # Command input method
    from_prompt = confirm(cyan("Read PGS information from command line(Y) or a file(n)."))

    if from_prompt:
        # Input pgs information from command line
        s = prompt(cyan("PGS information(CLUSTER PGS_ID)"))
        if ' ' not in s:
            warn(red('PGS information Must be specified. Aborting...'))
            return
        if call_leave_replication(s) == False: return False
    else:
        # Get input file name
        name = prompt(cyan("Input file name:"))
        for s in fileinput.FileInput(name):
            s = s.strip()
            if len(s) == 0: continue
            if s[0] == '#': continue
            if ' ' not in s:
                warn(red('PGS information Must be specified. Aborting...'))
                return
            if call_leave_replication(s) == False: return False

def call_leave_replication(args):
    cluster_name = args.split(' ')[0]
    pgs_id = int(args.split(' ')[1])

    return uninstall_pgs(cluster_name, pgs_id, True, remain_mgmt_conf=True)

def deploy_pgs_binary(cluster_name, pgs_id, pm_ip, smr_base_port, redis_port, cronsave_num):
    # Check local binary
    if check_local_binary_exist() == False:
        warn(red("Local binary for upgrade doesn't exist"))
        return False

    # Set host
    host = config.USERNAME + '@' + pm_ip.encode('ascii')
    env.hosts = [host]

    # Copy Binary
    print green('[%s] copy_binary begin' % host)
    if execute(remote.copy_binary)[host] == False:
        warn(red("[%s] Copy binary fail" % host))
        return False
    print green('[%s] copy_binary end' % host)

    # Check smr directory
    smr_path = util.make_smr_path_str(smr_base_port)
    if execute(remote.path_exist, smr_path)[host] == True:
        warn(red("[%s] smr directory already exists. PATH:%s" % (host, smr_path)))
        return False

    # Check redis directory
    redis_path = '%s/%d/redis' % (config.REMOTE_PGS_DIR, smr_base_port)
    if execute(remote.path_exist, redis_path)[host] == True:
        warn(red("[%s] redis directory already exists. PATH:%s" % (host, redis_path)))
        return False

    # Make smr directory
    smr_path = util.make_smr_log_path_str(smr_base_port)
    if execute(remote.make_remote_path, smr_path)[host] == False:
        return False
    print green('[%s] Make smr path %s success' % (host, smr_path))

    # Make redis directory
    if execute(remote.make_remote_path, redis_path)[host] == False:
        return False
    print green('[%s] Make redis path %s success' % (host, redis_path))

    # Apply PGS conf
    if execute(remote.apply_pgs_conf, cluster_name, smr_base_port, redis_port, cronsave_num)[host] == False:
        warn(red("[%s] Apply pgs conf fail, CLUSTER_NAME:%s, SMR_BASE_PORT:%d, REDIS_PORT:%d" % (host, cluster_name, smr_base_port, redis_port)))
        return False

    return True

def start_pgs_proces(cluster_name, pgs_id, pm_ip, smr_base_port, redis_port, log_delete_delay):
    # Set host
    host = config.USERNAME + '@' + pm_ip.encode('ascii')
    env.hosts = [host]

    # Start SMR process
    if execute(remote.start_smr_process, pm_ip, smr_base_port, log_delete_delay)[host] != True:
        warn(red("[%s] Start SMR fail, PGS_ID:%d, PORT:%d" % (host, pgs_id, smr_base_port)))
        return False

    # Start Redis process
    if execute(remote.start_redis_process, pm_ip, smr_base_port, redis_port, cluster_name)[host] != True:
        warn(red("[%s] Start Redis fail, PGS_ID:%d, PORT:%d" % (host, pgs_id, redis_port)))
        return False

    return True

def start_heartbeat_to_pgs(cluster_name, pgs_id, pg_id, pm_name, pm_ip, smr_base_port, redis_port):
    # Set host
    host = config.USERNAME + '@' + pm_ip.encode('ascii')
    env.hosts = [host]

    # PGS Join 
    if cm.pgs_join(cluster_name, pgs_id, pm_ip, smr_base_port, host) != True:
        warn(red("[%s] Join PGS fail, PGS_ID:%d, PORT:%d" % (host, pgs_id, smr_base_port)))
        return False

    # Check redis state
    if util.check_redis_state(pm_ip, redis_port) != True:
        warn(red("[%s] Check Redis state fail. PGS_ID:%d, PORT:%d" % (host, pgs_id, redis_port))) 
        return False

    return True

"""
Install a PGS.

Args:
    cluster_name : cluster_name
    pgs_id : partition group server id
    pg_id : partition group id
    pm_name : physical machine name
    pm_ip : physical machine ip
    smr_base_port : base port of SMR(State Machine Replicator)
    redis_port : port of redis
    cronsave_num : number of cronsave per a day
    log_delete_delay : log_delete_delay option of SMR. Normally, this value is (86400 / cronsave_num).
    chpt : if true then new PGS gets checkpoint from the existing master. 

Returns:
    if success then install_pgs() returns OK, otherwise it returns an ERROR_CODE.
    possible ERROR_CODEs : 
        ERROR_INTERRUPT
        ERROR_PGS_EXIST
        ERROR_PGS_ADD
        ERROR_DEPLOY_PGS_BIN
        ERROR_GET_CKPT
        ERROR_START_PGS
        ERROR_START_HB
"""
def install_pgs(cluster_name, pgs_id, pg_id, pm_name, pm_ip, smr_base_port, redis_port, cronsave_num, log_delete_delay, ckpt = False):
    print yellow('\n #### INSTALL BEGIN PGSID:%d IP:%s Port:%d #### \n' % (pgs_id, pm_ip, smr_base_port))

    # Continue?
    if util.cont() == False:
        return ERROR_INTERRUPT

    # Set host
    host = config.USERNAME + '@' + pm_ip.encode('ascii')
    env.hosts = [host]

    # Check if pgs exists
    pgs = cm.pgs_info(cluster_name, pgs_id)
    if pgs != None:
        print "EXISTING PGS =", pgs
        warn(red("PGS '%d' already exists in %s CLUSTER" % (pgs_id, cluster_name)))
        return ERROR_PGS_EXIST

    # Deploy binaries of PGS
    if deploy_pgs_binary(cluster_name, pgs_id, pm_ip, smr_base_port, redis_port, cronsave_num) == False:
        return ERROR_DEPLOY_PGS_BIN

    # Add pgs information to mgmt-cc for management
    if cm.pgs_add(cluster_name, pgs_id, pg_id, pm_name, pm_ip, smr_base_port, host) != True: 
        warn(red("[%s] PGS Add fail, CLUSTER:%s, PGS_ID:%d, PG_ID:%d, PM_NAME:%s, PM_IP:%s, SMR_BASE_PORT:%d" % (host, cluster_name, pgs_id, pg_id, pm_name, pm_ip, smr_base_port)))
        return ERROR_PGS_ADD

    # Get checkpoint from amster
    if ckpt:
        if copy_checkpoint_from_master(cluster_name, pg_id, pm_ip, smr_base_port) == False:
            return ERROR_GET_CKPT

    # Start pgs processes
    if start_pgs_proces(cluster_name, pgs_id, pm_ip, smr_base_port, redis_port, log_delete_delay) == False:
        return ERROR_START_PGS

    # Start heartbeat
    if start_heartbeat_to_pgs(cluster_name, pgs_id, pg_id, pm_name, pm_ip, smr_base_port, redis_port) == False:
        return ERROR_START_HB

    print green('\n #### INSTALL SUCCESS PGSID:%d IP:%s Port:%d #### \n' % (pgs_id, pm_ip, smr_base_port))
    print '===================================================================='
    return OK

def uninstall_pgs(cluster_name, pgs_id, remain_data, remain_mgmt_conf):
    if remain_mgmt_conf == True:
        print yellow('\n #### LEAVE REPLICATION BEGIN CLUSTER:%s PGSID:%d REMAIN_DATA:%s REMAIN_MGMT_CONF:%s  #### \n' % (cluster_name, pgs_id, remain_data, remain_mgmt_conf))
    else:
        print yellow('\n #### UNINSTALL BEGIN CLUSTER:%s PGSID:%d REMAIN_DATA:%s REMAIN_MGMT_CONF:%s  #### \n' % (cluster_name, pgs_id, remain_data, remain_mgmt_conf))

    # Continue?
    if util.cont() == False:
        return False

    # Get cluster info from Conf Master
    json_data = cm.cluster_info(cluster_name)
    if json_data == None: return False

    # Get pgs info
    pgs_data = cm.pgs_info(cluster_name, pgs_id)
    if pgs_data == None:
        warn(red("PGS '%d' doesn't exist in %s CLUSTER." % (pgs_id, cluster_name)))
        return False
    pgs_data = pgs_data['data']
    print "EXISTING PGS =", pgs_data

    ip = pgs_data['pm_IP']
    smr_base_port = pgs_data['replicator_Port_Of_SMR']
    redis_port = pgs_data['backend_Port_Of_Redis']

    # Show pg info
    pg_id = pgs_data['pg_ID']
    show_info.show_pgs_list(cluster_name, pg_id, True)
    if config.confirm_mode and not confirm(cyan('PGS uninstall, Continue?')):
        warn("Aborting at user request.")
        return False

    # Set host
    host = config.USERNAME + '@' + ip.encode('ascii')
    env.hosts = [host]

    # Check redis and smr are running
    if execute(remote.is_redis_process_exist, smr_base_port)[host] == False:
        warn(red("[%s] Check redis process fail. Aborting..." % host))
        return False

    if execute(remote.is_smr_process_exist, smr_base_port)[host] == False:
        warn(red("[%s] Check smr process fail. Aborting..." % host))
        return False

    # Check pgs
    path = config.REMOTE_PGS_DIR + '/' + str(smr_base_port) + '/redis'
    if execute(remote.path_exist, path)[host] != True:
        warn(red("[%s] '%s' doesn't exist." % (host, path)))
        return False

    path = config.REMOTE_PGS_DIR + '/' + str(smr_base_port) + '/smr/log'
    if execute(remote.path_exist, path)[host] != True:
        warn(red("[%s] '%s' doesn't exist." % (host, path)))
        return False

    # Change master
    pgs_count = cm.get_joined_pgs_count(cluster_name, pg_id)
    if pgs_count > 1 and pgs_data['smr_role'] == 'M':
        if change_master(cluster_name, pg_id, host) != True:
            warn(red("[%s] Change master fail. PGS_ID:%d, PG_ID:%d" % (host, pgs_id, pg_id)))
            return

    # PGS Leave
    if remain_data == False and remain_mgmt_conf == False:
        forced_leave = True
    else:
        forced_leave = False

    if cm.pgs_leave(cluster_name, pgs_id, ip, redis_port, host, forced=forced_leave) != True:
        warn(red("[%s] PGS Leave fail, PGS_ID:%d, PORT:%d" % (host, pgs_id, smr_base_port)))
        return False

    if remain_data == True:
        # BGSAVE
        if execute(remote.bgsave_redis_server, ip, redis_port)[host] != True:
            warn(red("[%s] BGSAVE fail, PGS_ID:%d, IP:%s, PORT:%d" % (host, pgs_id, ip, redis_port)))
            return False
        
    # Get Redis PID
    ret, pid = execute(remote.get_redis_pid, ip, redis_port)[host]
    if ret != True:
        warn(red("[%s] Get Redis PID fail, PGS_ID:%d, IP:%s, PORT:%d" % (host, pgs_id, ip, redis_port)))
        return False
    else:
        redis_pid = pid

    # PGS lconn
    if cm.pgs_lconn(cluster_name, pgs_id, ip, smr_base_port, host) != True:
        warn(red("[%s] PGS lconn fail, PGS_ID:%d, PORT:%d" % (host, pgs_id, smr_base_port)))
        return False
    
    # show pg info
    target_is_lconn = False
    while target_is_lconn == False:
        pgs_list = cm.get_pgs_list(cluster_name, pg_id)
        for id, data in pgs_list.items():
            if pgs_id != id:
                continue

            active_role = util.get_role_of_smr(data['ip'], data['mgmt_port'])
            if active_role == '?':
                warn(red('Invalid PGS role. Aborting...'))
                return  False

            if active_role == 'L':
                target_is_lconn = True
                show_info.show_pgs_list(cluster_name, pg_id, True, skip_warn=True)
                print green('PGS state became LCONN')
                break 
            else:
                time.sleep(0.5)

    # check master election
    if len(pgs_list) != 1:
        master_election = False
        while master_election == False:
            time.sleep(0.5)

            pgs_list = cm.get_pgs_list(cluster_name, pg_id)
            for id, data in pgs_list.items():
                if data['smr_role'] == 'M':
                    master_election = True
                    show_info.show_pgs_list(cluster_name, pg_id, True, skip_warn=True)
                    print green('New master has been eleected.')
                    break 

    # Stop redis process
    if execute(remote.stop_redis_process, ip, redis_port, redis_pid, smr_base_port)[host] != True:
        warn(red("[%s] Stop Redis fail, PGS_ID:%d, IP:%s, PORT:%d" % (host, pgs_id, ip, redis_port)))
        return False

    # Stop SMR process
    if execute(remote.stop_smr_process, smr_base_port)[host] != True:
        warn(red("[%s] Stop SMR fail, PGS_ID:%d, IP:%s, PORT:%d" % (host, pgs_id, ip, smr_base_port)))
        return False

    if remain_mgmt_conf == False:
        # PGS Del
        if cm.pgs_del(cluster_name, pgs_id, host) != True:
            warn(red("[%s] PGS Del fail, CLUSTER:%s, PGS_ID:%d" % (host, cluster_name, pgs_id)))
            return False

    if remain_data == False:
        # Remove smr memlog
        if execute(remote.exist_smr_memlog, smr_base_port)[host] == True:
            if execute(remote.remove_smr_memlog, smr_base_port)[host] == False:
                return False;

        # Remove directory
        path = config.REMOTE_PGS_DIR + '/' + str(smr_base_port)
        if execute(remote.remove_remote_path, path)[host] != True:
            warn(red("[%s] Remove pgs directory fail, PATH:%s" % (host, path)))
            return False
        print green("[%s] Remove pgs directory success, PATH:%s" % (host, path))
    else:
        print "[%s] Skip removing pgs directory, PATH:%s" % (host, path)

    print green('\n #### UNINSTALL SUCCESS PGSID:%d IP:%s Port:%d #### \n' % (pgs_id, ip, smr_base_port))
    print '===================================================================='
    return True

def menu_install_gw():
    # Confirm mode ?
    config.confirm_mode = confirm(cyan("Confirm Mode?"))

    # Command input method
    from_prompt = confirm(cyan("Read GW information from command line(Y) or a file(n)."))

    if from_prompt:
        # Input pgs information from command line
        s = prompt(cyan("GW information(CLUSTER GW_ID PM_NAME PM_IP PORT)"))
        if ' ' not in s:
            warn(red('GW information Must be specified. Aborting...'))
            return
        if call_install_gw(s) == False: return False
    else:
        # Get input file name
        name = prompt(cyan("Input file name:"))
        for s in fileinput.FileInput(name):
            s = s.strip()
            if len(s) == 0: continue
            if s[0] == '#': continue
            if ' ' not in s:
                warn(red('PGS information Must be specified. Aborting...'))
                return
            if call_install_gw(s) == False: return False

def call_install_gw(args):
    cluster_name = args.split(' ')[0]
    gw_id = int(args.split(' ')[1])
    pm_name = args.split(' ')[2]
    pm_ip = args.split(' ')[3]
    port = int(args.split(' ')[4])

    return install_gw(cluster_name, gw_id, pm_name, pm_ip, port)

def install_gw(cluster_name, gw_id, pm_name, pm_ip, port):
    print yellow('\n #### INSTALL BEGIN CLUSTER:%s GWID:%d IP:%s Port:%d #### \n' % (cluster_name, gw_id, pm_ip, port))

    # Continue?
    if util.cont() == False:
        return False

    # Check if GW exists
    gw = cm.gw_info(cluster_name, gw_id)
    if gw != None:
        print "EXISTING GW =", gw 
        warn(red("GW '%d' already exists in %s CLUSTER" % (gw_id, cluster_name)))
        return False

    # Check local binary
    if check_local_binary_exist() == False:
        warn(red("Local binary for upgrade doesn't exist"))
        return False

    # Copy Binary
    host = config.USERNAME + '@' + pm_ip.encode('ascii')
    env.hosts = [host]

    print green('[%s] copy_binary begin' % host)
    if execute(remote.copy_binary)[host] == False:
        warn(red("[%s] Copy binary fail" % host))
        return False
    print green('[%s] copy_binary end' % host)

    # Check GW directory
    gw_path = '%s/%d' % (config.REMOTE_GW_DIR, port)
    if execute(remote.path_exist, gw_path)[host] == True:
        warn(red("[%s] GW directory already exists. PATH:%s" % (host, gw_path)))
        return False

    # Check GW are running
    if execute(remote.is_gw_process_exist, cluster_name, port)[host] == True:
        warn(red("[%s] GW is already running. Aborting..." % host))
        return False

    # Make GW directory
    if execute(remote.make_remote_path, gw_path)[host] == False:
        return False
    print green('[%s] Make GW path %s success' % (host, gw_path))

    # Start gateway
    if execute(remote.start_gateway, cluster_name, pm_ip, port, config.CONF_MASTER_IP, config.CONF_MASTER_PORT)[host] != True:
        warn(red("[%s] Start GW fail, GW_ID:%d, PORT:%d" % (host, gw_id, port)))
        return False

    # Check gateway connection to redis
    if util.check_gw_inactive_connections(pm_ip, port) == False:
        warn("[%s:%d] Gateway check inactive connections fail" % (pm_ip, port))
        return False

    # Configure GW information to mgmt-cc
    if cm.gw_add(cluster_name, gw_id, pm_name, pm_ip, port) != True:
        warn(red("[%s] GW Add fail, GW_ID:%d, PORT:%d" % (host, gw_id, port)))
        return False

    print green('\n #### INSTALL SUCCESS GWID:%d IP:%s Port:%d #### \n' % (gw_id, pm_ip, port))
    print '===================================================================='
    return True

def menu_uninstall_gw():
    # Confirm mode ?
    config.confirm_mode = confirm(cyan("Confirm Mode?"))

    # Command input method
    from_prompt = confirm(cyan("Read GW information from command line(Y) or a file(n)."))

    if from_prompt:
        # Input pgs information from command line
        s = prompt(cyan("GW information(CLUSTER GW_ID)"))
        if ' ' not in s:
            warn(red('GW information Must be specified. Aborting...'))
            return
        if call_uninstall_gw(s) == False: return False
    else:
        # Get input file name
        name = prompt(cyan("Input file name:"))
        for s in fileinput.FileInput(name):
            s = s.strip()
            if len(s) == 0: continue
            if s[0] == '#': continue
            if ' ' not in s:
                warn(red('GW information Must be specified. Aborting...'))
                return
            if call_uninstall_gw(s) == False: return False

def call_uninstall_gw(args):
    cluster_name = args.split(' ')[0]
    gw_id = int(args.split(' ')[1])
    
    return uninstall_gw(cluster_name, gw_id)

def uninstall_gw(cluster_name, gw_id):
    print yellow('\n #### UNINSTALL BEGIN GWID:%d #### \n' % (gw_id))

    # Continue?
    if util.cont() == False:
        return False

    # Check if GW exists
    gw = cm.gw_info(cluster_name, gw_id)
    if gw == None:
        warn(red("GW '%d' doesn't exist in %s CLUSTER" % (gw_id, cluster_name)))
        return False

    pm_ip = gw['data']['pm_IP']
    port = gw['data']['port']

    # Set host
    host = config.USERNAME + '@' + pm_ip.encode('ascii')
    env.hosts = [host]

    # Check GW are running
    if execute(remote.is_gw_process_exist, cluster_name, port)[host] == False:
        warn(red("[%s] Check GW process fail. Aborting..." % host))
        return False

    # Check GW directory
    gw_path = '%s/%d' % (config.REMOTE_GW_DIR, port)
    if execute(remote.path_exist, gw_path)[host] == False:
        warn(red("[%s] GW directory doesn't exist. PATH:%s" % (host, gw_path)))
        return False

    # Delete GW information in mgmt-cc
    if cm.gw_del(cluster_name, gw_id, pm_ip, port) != True:
        warn(red("[%s] GW Del fail, GW_ID:%d, PORT:%d" % (host, gw_id, port)))
        return

    # Stop gateway
    if execute(remote.stop_gateway, cluster_name, pm_ip, port)[host] != True:
        warn(red("[%s] Stop GW fail, GW_ID:%d, PORT:%d" % (host, gw_id, port)))
        return

    if execute(remote.remove_remote_path, gw_path)[host] != True:
        warn(red("[%s] Remove GW directory fail, PATH:%s" % (host, gw_path)))
        return False
    print green("[%s] Remove GW directory success, PATH:%s" % (host, gw_path))

    print green('\n #### UNINSTALL SUCCESS GWID:%d IP:%s Port:%d #### \n' % (gw_id, pm_ip, port))
    print '===================================================================='
    return True

def menu_upgrade_pgs():
    # Check local binary
    if check_local_binary_exist() == False:
        warn(red("Local binary for upgrade doesn't exist"))
        return

    config.confirm_mode = confirm(cyan("Confirm Mode?"))
    # Get cronsave number
    new_cronsave_num = prompt(cyan("Input cronsave number (CRONSAVE_NUM), If you don't want to modify cronsave number then input 0."))
    if new_cronsave_num == '': new_cronsave_num = 0
    else: new_cronsave_num = int(new_cronsave_num)
    cluster_unit = confirm(cyan("Upgrade Cluster units(Y) or PGS units(n)"))
    if cluster_unit:
        target_cluster = prompt(cyan("Input Cluster ('cluster name' or all)"))

        if target_cluster == 'all':
            json_data = cm.cluster_ls()
            if json_data == None:
                return False
            
            cluster_name_list = json_data['data']['list']
        
            if len(cluster_name_list) == 0:
                print yellow('There is no cluster.')
                return True
        
            cluster_name_list.sort()
            for cluster_name in cluster_name_list:
                cluster_name = cluster_name.encode('ascii')
                if 'java_client_test' in cluster_name:
                    continue

                print yellow('\n #### UPGRADE PGS BEGIN CLUSTER:%s #### \n' % cluster_name)
                if upgrade_all_pgs_in_cluster(cluster_name, new_cronsave_num) == False:
                    return False
        else:
            if upgrade_all_pgs_in_cluster(target_cluster, new_cronsave_num) == False:
                return False
    else:
        # Command input method
        from_prompt = confirm(cyan("Read PGS information from command line(Y) or a file(n)."))

        if from_prompt:
            # Input pgs information from command line
            s = prompt(cyan("PGS information(CLUSTER PGS_ID)"))
            if ' ' not in s:
                warn(red('PGS information Must be specified. Aborting...'))
                return
            target_cluster = s.split(' ')[0]
            pgs_id = int(s.split(' ')[1])
            if upgrade_pgs(target_cluster, pgs_id, new_cronsave_num) == False:
                return False
        else:
            # Get input file name
            name = prompt(cyan("Input file name:"))
            for s in fileinput.FileInput(name):
                s = s.strip()
                if len(s) == 0: continue
                if s[0] == '#': continue
                if ' ' not in s:
                    warn(red('PGS information Must be specified. Aborting...'))
                    return
                target_cluster = s.split(' ')[0]
                pgs_id = int(s.split(' ')[1])
                if upgrade_pgs(target_cluster, pgs_id, new_cronsave_num) == False:
                    return False

    return True

def upgrade_all_pgs_in_cluster(cluster_name, new_cronsave_num):
    # Get cluster info from Conf Master
    json_data = cm.cluster_info(cluster_name)
    if json_data == None: 
        return False

    # Get PG and PGS info
    pg_list = {}
    for pg_data in json_data['data']['pg_list']:
        pg_id = pg_data['pg_id']
        pg_list[pg_id] = {}
        for pgs_id in pg_data['pg_data']['pgs_ID_List']:
            
            pgs_json_data = cm.pgs_info(cluster_name, pgs_id)
            if pgs_json_data == None: 
                warn(red("PGS '%s' doesn't exist." % pgs_id))
                return False

            pg_list[pg_id][pgs_id] = {}
            pg_list[pg_id][pgs_id]['ip'] = pgs_json_data['data']['pm_IP']
            pg_list[pg_id][pgs_id]['redis_port'] = pgs_json_data['data']['backend_Port_Of_Redis']
            pg_list[pg_id][pgs_id]['smr_base_port'] = pgs_json_data['data']['replicator_Port_Of_SMR']
            pg_list[pg_id][pgs_id]['smr_role'] = pgs_json_data['data']['smr_Role'].encode('ascii')

    # Upgrade loop
    for pg_id, pg_data in pg_list.items():
        for pgs_id, pgs_data in sorted(pg_data.items(), key=lambda x: x[1]['smr_role'], reverse=True):
            if upgrade_pgs(cluster_name, pgs_id, new_cronsave_num) == False:
                return False

    return True

"""
Get supplment physical machine information.

Returns:
    tuple(string, string): string: physical machine name
                           string: physical machine ip
"""
def get_supplement_pm():
    while True:
        try:
            input = prompt(cyan("Input supplement host information(PM_NAME IP)")).split(" ")
            suppl_pm_name = input[0].encode('ascii')
            suppl_pm_ip = input[1].encode('ascii')

            # Check pm_info in mgmt-cc
            pm = cm.pm_info(suppl_pm_name)
            if pm == None:
                warn(red('%s is not in mgmt-cc. Please check pm_name or add pm information to mgmt-cc') % (suppl_pm_name))
                continue

            if suppl_pm_ip != pm['pm_info']['ip']:
                warn(red('IP does not match. input:%s, mgmt-cc:%s' % (suppl_pm_ip, pm['pm_info']['ip'])))
                continue
        except:
            warn(red('Invalid arguments'))
            continue
        break

    return (suppl_pm_name, suppl_pm_ip)

class PortAllocator():
    def __init__(self, host, path, start_port):
        self.host = host
        self.path = path
        self.port = start_port

        self.index = 0
        ports = execute(remote.get_ports, self.path, hosts=[self.host])[self.host]
        if len(ports) == 0:
            self.max_port = start_port - 10
        else:
            self.max_port = max(ports)

        if self.max_port < start_port:
            self.max_port = start_port - 10

    def set_max_port(self, port):
        self.max_port = port

    def get_max_port(self):
        return self.max_port

    def next(self):
        self.max_port += 10
        return self.max_port

"""
Preface supplement pgs information.
This function looks for smr_base_port and pgs_id, Add pgs info to mgmt-cc for preoccupancy.

Returns:
    dict: {"pgs_id" : <int>, "smr_base_port" : <int>, "redis_port" : <int>}
"""
def prepare_suppl_pgsinfo(cluster_name, pg_id, pm_name, pm_ip):
    # Set host
    host = config.USERNAME + '@' + pm_ip.encode('ascii')
    env.hosts = [host]

    # Get port
    port = PortAllocator(host, config.REMOTE_PGS_DIR, config.PGS_BASE_PORT).next()

    # Get pgs_id
    pm = cm.pm_info(pm_name)
    if pm != None:
        pgs_id = -1
        for cluster in pm['cluster_list']:
            for name, data in cluster.items():
                if name == cluster_name:
                    pgs_id = max(int(x) for x in data['pgs_ID_List']) + 1
                    print yellow("[%s] Use max pgs_id in cluster. PGS_ID:%d, PM_NAME:%s, PM_IP:%s" % 
                            (host, pgs_id, pm_name, pm_ip))
                    break

    if pgs_id == -1:
        pgs_ids = cm.pgs_ls(cluster_name) 
        if pgs_ids == None:
            return None 
        pgs_id = max(int(x) for x in pgs_ids['data']['list'])
        pgs_id += 1
        print yellow("[%s] Use max pgs_id in cluster. PGS_ID:%d, PM_NAME:%s, PM_IP:%s" % 
                (host, pgs_id, pm_name, pm_ip))

    return {"pgs_id" : pgs_id, "smr_base_port" : port, "redis_port" : port + 9}

"""
Add a PGS to a PG in order to make a 1-copy replication a 2-copy replication.
When a PG is already 2-copy or more, this function should not be called.

Returns:
    tuple(boolean, dict): boolean: return True if successful or False
                           dict: {"pgs_id" : <int>, "smr_base_port" : <int>, "redis_port" : <int>}
"""
def supplement_pgs(cluster_name, pg_id, pm_name, pm_ip, redis_port, cronsave_num, log_delete_delay, gw_list):
    print magenta("\n[%s] Add supplement pgs because there is no replication pgs, PG:%d\n" % (pm_ip, pg_id))

    # Get machine for supplement pgs 
    with RedisCmd(pm_ip, redis_port) as redis_cmd:
        suppl_pm_name = pm_name.encode('ascii')
        suppl_pm_ip = pm_ip.encode('ascii')
        while True:
            suppl_host = config.USERNAME + '@' + suppl_pm_ip

            # Get memory info
            required_mem = (redis_cmd.info_rss() / 1024 + 1024) * 2.2   # supplementary pgs + bgsave upgrade taget pgs
            ret, avail_mem = execute(remote.available_mem, host=suppl_host)[suppl_host]
            if ret == False:
                warn(red("[%s] Failed to get available mem. Check host status, HOST:%s" % (suppl_host, suppl_pm_ip)))
                suppl_pm_name, suppl_pm_ip = get_supplement_pm()
                continue

            print yellow("[%s] memory, required:%d, available:%d" % (pm_ip, required_mem, avail_mem))

            if avail_mem > required_mem:
                break

            warn(red("[%s] Not enough memory, REQUIRED_MEM:%d, AVAILABLE_MEM:%d" % (suppl_host, required_mem, avail_mem)))

            # Get new supplement physical machine
            suppl_pm_name, suppl_pm_ip = get_supplement_pm()

    # Prepare PGS info
    suppl_pgs = prepare_suppl_pgsinfo(cluster_name, pg_id, suppl_pm_name, suppl_pm_ip)
    if suppl_pgs == None:
        warn(red("[%s] Get supplement pgs info fail. PG:%d" % (suppl_host, pg_id)))
        return False, None

    # Install PGS
    while suppl_pgs['pgs_id'] < 0x7FFF:
        ret = install_pgs(cluster_name, suppl_pgs['pgs_id'], pg_id, suppl_pm_name, suppl_pm_ip, 
                suppl_pgs['smr_base_port'], suppl_pgs['redis_port'], 
                cronsave_num, log_delete_delay, True) 
        if ret == OK:
            break
        else:
            if ret == ERROR_PGS_ADD:
                print yellow('[%s] PGS_ID duplicated, retry. PGSID:%d IP:%s Port:%d #### \n' % 
                        (suppl_pm_ip,  suppl_pgs['pgs_id'], suppl_pm_ip, suppl_pgs['smr_base_port']))
                time.sleep(0.5)
                suppl_pgs['pgs_id'] += 1
                continue
            else:
                warn(red('[%s] Failed to add pgs. ERROR:%d PGSID:%d IP:%s Port:%d #### \n' % 
                        (suppl_pm_ip, ret, suppl_pgs['pgs_id'], suppl_pm_ip, suppl_pgs['smr_base_port'])))
                return False, None

    # Check all connections from gateway has done.
    if util.check_gw_inactive_connections_par(gw_list) == False:
        warn("[%s:%d] Check gateway's inactive connections fail" % (pm_ip, port))
        return False, None

    # Check pgs
    pgs_list = cm.get_pgs_list(cluster_name, pg_id)
    if len(pgs_list) < 2:
        warn(red('Not enough PGS. It must be more than 2, but %d. Aborting...' % len(pgs_list)))
        return False, None

    print magenta("\n[%s] Add supplement pgs success, PG:%d\n" % (pm_ip, pg_id))
    return True, suppl_pgs

def upgrade_pgs(cluster_name, pgs_id, new_cronsave_num):
    pgs_id = int(pgs_id)
    new_cronsave_num= int(new_cronsave_num)

    print yellow('\n #### UPGRADE BEGIN CLUSTER:%s PGSID:%d #### \n' % (cluster_name, pgs_id))

    # Continue?
    if util.cont() == False:
        return False

    # Get cluster info from Conf Master
    cluster_json = cm.cluster_info(cluster_name)
    if cluster_json == None: return

    # Check if pgs exists
    pgs = cm.pgs_info(cluster_name, pgs_id)
    if pgs == None:
        warn(red("PGS '%d' doesn't exist in %s CLUSTER" % (pgs_id, cluster_name)))
        return False

    pgs_json = pgs['data']
    pg_id = pgs_json['pg_id']

    # Get quorum policy
    ret = cluster_json['data']['cluster_info']['Quorum_Policy']
    ret = ret.strip()[1:-1]

    # Get quorum
    pg_info = cm.pg_info(cluster_name, pg_id)
    if pg_info == None:
        warn(red("[%s] get PG '%d' information from confmaster fail." % (env.host_string, pg_id)))
        return False
    quorum_value = pg_info['quorum']

    # Get PG and PGS info from Json
    pgs_list = cm.get_pgs_list(cluster_name, pg_id)
    if len(pgs_list) == 0:
        warn(red('PGS list is empty. Aborting...'))
        return False

    # Get gateway list
    gw_list = {}
    for gw_id in cluster_json['data']['gw_id_list']:

        gw_list[gw_id] = {}
        gw_json_data = cm.gw_info(cluster_name, gw_id)
        if gw_json_data == None:
            warn(red("GW '%s' doesn't exist." % gw_id))
            return False

        gw_list[gw_id]['ip'] = gw_json_data['data']['pm_IP']
        gw_list[gw_id]['pm_name'] = gw_json_data['data']['pm_Name']
        gw_list[gw_id]['port'] = gw_json_data['data']['port']

    # Set remote host
    host = config.USERNAME + '@' + pgs_json['ip'].encode('ascii')
    env.hosts = [host]

    # Copy binary
    if execute(remote.copy_binary)[host] != True:
        warn(red("[%s] copy binary fail" % host))
        return False

    # Check pgs path
    path = config.REMOTE_PGS_DIR + '/' + str(pgs_json['smr_base_port']) + '/redis'
    if execute(remote.path_exist, path)[host] != True:
        warn(red("[%s] '%s' doesn't exist." % (host, path)))
        return False

    path = config.REMOTE_PGS_DIR + '/' + str(pgs_json['smr_base_port']) + '/smr/log'
    if execute(remote.path_exist, path)[host] != True:
        warn(red("[%s] '%s' doesn't exist." % (host, path)))
        return False

    # Set arguments
    ip = pgs_json['ip'].encode('ascii')
    pm_name = pgs_json['pm_name'].encode('ascii')
    redis_port = pgs_json['redis_port']
    smr_base_port = pgs_json['smr_base_port']
    role = pgs_json['smr_role']

    # Get cronsave num
    cronsave_num = None
    if new_cronsave_num == 0:
        cronsave_num = execute(remote.get_cronsave_num, smr_base_port)[host]
        if cronsave_num == -1:
            warn(red("[%s] Get cronsave num fail, PGS_ID:%d, IP:%s, PORT:%d" % (host, pgs_id, ip, smr_base_port)))
            return False
    else:
        cronsave_num = new_cronsave_num
    log_delete_delay = 60 * 60 * 24 / cronsave_num 
    print yellow("[%s] >>> cronsave num : %d" % (host, cronsave_num))
    print yellow("[%s] >>> log_delete_delay : %d" % (host, log_delete_delay))

    org_pgs_count = cm.get_joined_pgs_count(cluster_name, pg_id)

    # Add supplement replication. 
    # Reason: while it is upgrading 1-copy-cluster, there is no pgs.
    if org_pgs_count == 1:
        ret, suppl_pgs = supplement_pgs(cluster_name, pg_id, pm_name, ip, redis_port, cronsave_num, log_delete_delay, gw_list)
        if ret == False:
            warn(red("[%s] Add supplement pgs fail, PG_ID:%d, PGS_ID:%d, IP:%s, PORT:%d" % (host, pg_id, pgs_id, ip, smr_base_port)))
            return False

        # Update pgs list
        pgs_list = cm.get_pgs_list(cluster_name, pg_id)
        if len(pgs_list) == 0:
            warn(red('PGS list is empty. Aborting...'))
            return False

        # Restore host
        host = config.USERNAME + '@' + ip.encode('ascii')
        env.hosts = [host]

    # Check replication count
    pgs_cnt = len(cm.get_pgs_list(cluster_name, pg_id))
    if pgs_cnt == 0:
        warn(red('PGS list is empty. Aborting...'))
        return False
    elif pgs_cnt < 2:
        warn(red('There are not enough PGSes'))
        return False

    # Change master
    if role == 'M':
        # Temporary code. send replication ping to all redis in the PG to escape hang
        ping_workers = []
        for temp_pgs_id, temp_pgs_json in pgs_list.items():
            ping_workers.append(util.PingpongWorker(temp_pgs_json['ip'], temp_pgs_json['redis_port'], 'ping', None, try_cnt=3))

        # Role change
        if change_master(cluster_name, pg_id, host) != True:
            warn(red("[%s] Change master fail. PGS:%d, PG:%d" % (host, pgs_id, pg_id)))
            return False

        # Temporary code. send replication ping to all redis in the PG to escape hang
        for w in ping_workers:
            w.start()

        for w in ping_workers:
            w.join()

        for w in ping_workers:
            reply = w.get_last_reply()
            if reply != '+PONG':
                warn(red("[%s:%d] Replication ping to redis fail. UPGRADE_PGS:%d, PG:%d, REPLY:'%s'" % (w.get_ip(), w.get_port(), pg_id, pgs_id, reply)))
                return False

        print green("Replication ping to all redis in PG '%d' success." % pg_id)

    # PGS Leave
    if cm.pgs_leave(cluster_name, pgs_id, ip, redis_port, host) != True:
        warn(red("[%s] PGS Leave fail, PGS_ID:%d, PORT:%d" % (host, pgs_id, smr_base_port)))
        return False
        
    # BGSAVE
    if execute(remote.bgsave_redis_server, ip, redis_port)[host] != True:
        warn(red("[%s] BGSAVE fail, PGS_ID:%d, IP:%s, PORT:%d" % (host, pgs_id, ip, redis_port)))
        return False

    # Get Redis PID
    ret, pid = execute(remote.get_redis_pid, ip, redis_port)[host]
    if ret != True:
        warn(red("[%s] Get Redis PID fail, PGS_ID:%d, IP:%s, PORT:%d" % (host, pgs_id, ip, redis_port)))
        return False
    else:
        redis_pid = pid

    # PGS lconn
    if cm.pgs_lconn(cluster_name, pgs_id, ip, smr_base_port, host) != True:
        warn(red("[%s] PGS lconn fail, PGS_ID:%d, PORT:%d" % (host, pgs_id, smr_base_port)))
        return False

    # Stop redis process
    if execute(remote.stop_redis_process, ip, redis_port, redis_pid, smr_base_port)[host] != True:
        warn(red("[%s] Stop Redis fail, PGS_ID:%d, IP:%s, PORT:%d" % (host, pgs_id, ip, redis_port)))
        return False

    # Stop SMR process
    if execute(remote.stop_smr_process, smr_base_port)[host] != True:
        warn(red("[%s] Stop SMR fail, PGS_ID:%d, IP:%s, PORT:%d" % (host, pgs_id, ip, smr_base_port)))
        return False

    # Apply pgs conf
    if execute(remote.apply_pgs_conf, cluster_name, smr_base_port, redis_port, cronsave_num)[host] == False:
        warn(red("[%s] Apply pgs conf fail, CLUSTER_NAME:%s, SMR_BASE_PORT:%d, REDIS_PORT:%d" % (host, cluster_name, smr_base_port, redis_port)))
        return False

    # Start SMR process
    if execute(remote.start_smr_process, ip, smr_base_port, log_delete_delay)[host] != True:
        warn(red("[%s] Start SMR fail, PGS_ID:%d, PORT:%d" % (host, pgs_id, smr_base_port)))
        return False

    # Start Redis process
    if execute(remote.start_redis_process, ip, smr_base_port, redis_port, cluster_name)[host] != True:
        warn(red("[%s] Start Redis fail, PGS_ID:%d, PORT:%d" % (host, pgs_id, redis_port)))
        return False

    # PGS Join 
    if cm.pgs_join(cluster_name, pgs_id, ip, smr_base_port, host) != True:
        warn(red("[%s] Join PGS fail, PGS_ID:%d, PORT:%d" % (host, pgs_id, smr_base_port)))
        return False

    # Check redis state
    if util.check_redis_state(ip, redis_port) != True:
        warn(red("[%s] Check Redis state fail. PGS_ID:%d, PORT:%d" % (host, pgs_id, redis_port))) 
        return False

    # Check all connections from gateway has done.
    if util.check_gw_inactive_connections_par(gw_list) == False:
        warn("[%s:%d] Check gateway's inactive connections fail" % (ip, port))
        return False

    # Remove supplement pgs, only if target is 1-copy-cluster
    if org_pgs_count == 1:
        print magenta("\n[%s] Delete supplement pgs. SPGS:%d, SPORT:%d, PG:%d\n" % (host, suppl_pgs['pgs_id'], suppl_pgs['smr_base_port'], pg_id))

        # Role change
        if change_master(cluster_name, pg_id, host) != True:
            warn(red("[%s] Change master fail. PGS:%d, PG:%d" % (host, suppl_pgs['pgs_id'], pg_id)))
            return False

        # Uninstall PGS
        if uninstall_pgs(cluster_name, suppl_pgs['pgs_id'], remain_data=False, remain_mgmt_conf=False) == False:
            warn(red("Uninstall supplement pgs fail. SPGS:%d, SPORT:%d, PG:%d" %  
                (suppl_pgs['pgs_id'], suppl_pgs['smr_base_port'], pg_id)))
            return False

        print magenta("\n[%s] Delete supplement pgs success. SPGS:%d, SPORT:%d, PG:%d\n" % 
                (host, suppl_pgs['pgs_id'], suppl_pgs['smr_base_port'], pg_id))

        # Update pgs list
        pgs_list = cm.get_pgs_list(cluster_name, pg_id)
        if len(pgs_list) == 0:
            warn(red('PGS list is empty. Aborting...'))
            return False

        # Restore host
        host = config.USERNAME + '@' + ip.encode('ascii')
        env.hosts = [host]

    # Check Quorum
    if execute(remote.check_quorum, pgs_list, quorum_value)[host] != True:
        warn("[%s] Check quorum fail, pgs_id:%d, port:%d" % (host, pgs_id, smr_base_port))
        return False

    print green('\n #### UPGRADE SUCCESS CLUSTER:%s PGSID:%d IP:%s Port:%d #### \n' % (cluster_name, pgs_id, ip, smr_base_port))
    print '===================================================================='

    return True

# On success, change_master() returns pgs_id of new master; on error, it returns -1.
def change_master(cluster_name, pg_id, host):
    print magenta("\n[%s] Change master, PG:%d" % (host ,pg_id))

    # Get candidates for master
    candidates = []

    pgs_list = cm.get_pgs_list(cluster_name, pg_id)
    for id, data in pgs_list.items():
        if data['smr_role'] == 'S' and data['hb'] == 'Y':
            active_role = util.get_role_of_smr(data['ip'], data['mgmt_port'], verbose=False)
            if active_role != data['smr_role']:
                warn(red("[%s] Change master fail, PGS '%d' has invalid state. %s(%s)" % 
                          (host, id, data['smr_role'], active_role )))
                return False

            candidates.append(data)

    # Check candidates
    if len(candidates) == 0:
        warn(red("[%s] Change master fail, PG '%d' doesn't have any slave." % (host, pg_id)))
        return False

    pgs_id = random.choice(candidates)['pgs_id']

    master_id = cm.role_change(cluster_name, pgs_id, host)
    if master_id == -1:
        warn(red("[%s] Change master fail." % host))
        return False

    print green("[%s] Change master success, PG:%d, MASTER:%d" % (host, pg_id, master_id))
    return True

def menu_upgrade_gw():
    # Check local binary
    if check_local_binary_exist() == False:
        warn(red("Local binary for upgrade doesn't exist"))
        return False

    config.confirm_mode = confirm(cyan("Confirm Mode?"))
    cluster_unit = confirm(cyan("Upgrade Cluster units(Y) or GW units(n)"))
    if cluster_unit:
        # Get cluster info
        cluster_name = prompt(cyan("Input Cluster ('cluster name' or all)"))
        if cluster_name == 'all':
            json_data = cm.cluster_ls()
            if json_data == None:
                return False
            
            cluster_name_list = json_data['data']['list']
        
            if len(cluster_name_list) == 0:
                print yellow('There is no cluster.')
                return True
        
            cluster_name_list.sort()
            for name in cluster_name_list:
                name = name.encode('ascii')
                if 'java_client_test' in name:
                    continue
                print yellow('\n #### UPGRADE GW BEGIN CLUSTER:%s #### \n' % name)
                if upgrade_all_gw_in_cluster(name) == False:
                    return False
        else:
            if upgrade_all_gw_in_cluster(cluster_name) == False:
                return False
    else:
        # Command input method
        from_prompt = confirm(cyan("Read GW information from command line(Y) or a file(n)."))

        if from_prompt:
            # Input pgs information from command line
            s = prompt(cyan("GW information(CLUSTER GW_ID)"))
            if ' ' not in s:
                warn(red('PGS information Must be specified. Aborting...'))
                return
            target_cluster = s.split(' ')[0]
            gw_id = int(s.split(' ')[1])
            if upgrade_gw(target_cluster, gw_id) == False:
                return False
        else:
            # Get input file name
            name = prompt(cyan("Input file name:"))
            for s in fileinput.FileInput(name):
                s = s.strip()
                if len(s) == 0: continue
                if s[0] == '#': continue
                if ' ' not in s:
                    warn(red('PGS information Must be specified. Aborting...'))
                    return
                target_cluster = s.split(' ')[0]
                gw_id = int(s.split(' ')[1])
                if upgrade_gw(target_cluster, gw_id) == False:
                    return False

def upgrade_all_gw_in_cluster(cluster_name):
    json_data = cm.cluster_info(cluster_name)
    if json_data == None: 
        return False

    # Upgrade loop
    for gw_id in json_data['data']['gw_id_list']:
        if upgrade_gw(cluster_name, gw_id) == False:
            return False

    return True

def upgrade_gw(cluster_name, gw_id):
    if type(gw_id) == str: gw_id = int(gw_id)

    print yellow('\n #### UPGRADE BEGIN CLUSTER:%s GWID:%d #### \n' % (cluster_name, gw_id))

    # Continue?
    if util.cont() == False:
        return False

    # Get gateway info
    gw_json_data = cm.gw_info(cluster_name, gw_id)
    if gw_json_data == None:
        warn(red("GW '%s' doesn't exist." % gw_id))
        return False

    ip = gw_json_data['data']['pm_IP'].encode('ascii')
    pm_name = gw_json_data['data']['pm_Name'].encode('ascii')
    port = int(gw_json_data['data']['port'])

    # Copy Binary
    host = config.USERNAME + '@' + ip.encode('ascii')
    env.hosts = [host]

    if execute(remote.copy_binary)[host] != True:
        warn(red("[%s] Copy binary fail" % host))
        return False

    # Check GW path
    path = config.REMOTE_GW_DIR + '/' + str(port)
    if execute(remote.path_exist, path)[host] != True:
        warn(red("[%s] '%s' doesn't exist." % (host, path)))
        return False

    # GW Del
    if cm.gw_del(cluster_name, gw_id, ip, port) != True:
        warn(red("[%s] GW Del fail, GW_ID:%d, PORT:%d" % (host, gw_id, port)))
        return False

    # Stop gateway
    if execute(remote.stop_gateway, cluster_name, ip, port)[host] != True:
        warn(red("[%s] Stop gateway fail, GW_ID:%d, PORT:%d" % (host, gw_id, port)))
        return False

    # Start gateway
    if execute(remote.start_gateway, cluster_name, ip, port, config.CONF_MASTER_IP, config.CONF_MASTER_PORT)[host] != True:
        warn(red("[%s] Start gateway fail, GW_ID:%d, PORT:%d" % (host, gw_id, port)))
        return False

    # Check gateway connection to redis
    if util.check_gw_inactive_connections(ip, port) == False:
        warn("[%s:%d] Gateway check inactive connections fail" % (ip, port))
        return False

    # Configure GW information to mgmt-cc
    if cm.gw_add(cluster_name, gw_id, pm_name, ip, port, additional_clnt=config.NUM_CLNT_MIN) != True:
        warn(red("[%s] GW Add fail, GW_ID:%d, PORT:%d" % (host, gw_id, port)))
        return False

    print green('\n #### UPGRADE SUCCESS CLUSTER:%s GWID:%d IP:%s Port:%d #### \n' % (cluster_name, gw_id, ip, port))
    print '===================================================================='

    return True

def menu_recover_cluster():
    # Confirm mode ?
    config.confirm_mode = confirm(cyan("Confirm Mode?"))
    cluster_name = prompt(cyan("Cluster information (CLUSTER)"))
    cronsave_num = int(prompt(cyan("Cronsave number")).strip())
    log_delete_delay = 60 * 60 * 24 / cronsave_num 

    try:
        # Recover Gateway
        show_info.menu_show_gw_list(cluster_name)
        s = prompt(cyan("GW information ([GW_ID or all])"))
        if s != '':
            gw_list = cm.get_gw_list(cluster_name)
            if None != gw_list:
                for gw_id, gw in sorted(gw_list.items(), key=lambda x: int(x[0])):
                    if gw['state'] == 'N':
                        print yellow("\n #### Gateway '%s' is running ####\n" % gw_id)
                        continue

                    if s.lower() != 'all' and int(s) != gw_id:
                        continue

                    if execute_gw(cluster_name, int(gw_id), gw['pm_IP'].encode('ascii'), int(gw['port'])) == False:
                        warn(red("Recover gateway '%d' fail" % gw_id))

        # Recover PG
        show_info.menu_show_all(cluster_name)
        s = prompt(cyan("PG information ([PG_ID or all])"))
        if s != '':
            # Try to recover all PG
            if s.lower() == 'all':
                json_data = cm.cluster_info(cluster_name)
                if json_data == None: 
                    warn(red("Cluster '%s' is not exist" % cluster_name))
                    return False
                
                for pg in sorted(json_data['data']['pg_list'], key=lambda x: int(x['pg_id'])):
                    pg_id = int(pg['pg_id'])
                    if recover_pg(cluster_name, pg_id, cronsave_num, log_delete_delay) == False: 
                        return False
            else:
                pg_id = int(s)

                cluster_json = cm.cluster_info(cluster_name)
                if cluster_json == None: 
                    warn(red("Cluster '%s' is not exist" % cluster_name))
                    return False
                
                pg_info = filter(lambda x: int(x['pg_id']) == pg_id, cluster_json['data']['pg_list'])
                if len(pg_info) == 0:
                    warn(red("PG '%d' is not exist" % pg_id))
                    return False
                elif len(pg_info) != 1:
                    warn(red("There are too many PG '%d'" % pg_id))
                    return False

                pg_info = pg_info[0]
                if recover_pg(cluster_name, pg_id, cronsave_num, log_delete_delay) == False: 
                    return False

    except ValueError as e:
        warn(red('Invalid input'))
        return False

def recover_pg(cluster_name, pg_id, cronsave_num, log_delete_delay):
    print yellow("\n #### REPAIR PG '%d' BEGIN #### \n" % pg_id)

    pg = cm.pg_info(cluster_name, pg_id)
    if pg == None:
        warn(red("PG '%d' is not exist" % pg_id))
        return False

    recover_list = []
    for pgs_id in pg['pgs_ID_List']:
        pgs = cm.pgs_info(cluster_name, pgs_id)
        if pgs == None:
            warn(red("PGS '%d' is not exist" % pgs_id))
            return False

        pgs = pgs['data']
        smr_base_port = pgs['smr_base_port']

        host = config.USERNAME + '@' + pgs['ip'].encode('ascii')
        env.hosts = [host]

        # Check smr directory
        smr_path = '%s/%d/smr' % (config.REMOTE_PGS_DIR, smr_base_port)
        if execute(remote.path_exist, smr_path)[host] == False:
            warn(red("[%s] smr directory doesn't exist. PATH:%s" % (host, smr_path)))
            return False

        # Check redis directory
        redis_path = '%s/%d/redis' % (config.REMOTE_PGS_DIR, smr_base_port)
        if execute(remote.path_exist, redis_path)[host] == False:
            warn(red("[%s] redis directory doesn't exist. PATH:%s" % (host, redis_path)))
            return False

        # Need to recover smr?
        if execute(remote.is_smr_process_exist, smr_base_port)[host] == False:
            recover_list.append(pgs)
            continue

        # Need to recover redis?
        if execute(remote.is_redis_process_exist, smr_base_port)[host] == False:
            recover_list.append(pgs)
            continue

    # Execute SMR and Redis
    for pgs in recover_list:
        pgs_id = pgs['pgs_id']
        ip = pgs['ip']
        smr_base_port = pgs['smr_base_port']
        redis_port = pgs['redis_port']

        host = config.USERNAME + '@' + pgs['ip'].encode('ascii')
        env.hosts = [host]

        # Apply PGS conf
        if execute(remote.apply_pgs_conf, cluster_name, smr_base_port, redis_port, cronsave_num)[host] == False:
            warn(red("[%s] Apply pgs conf fail, CLUSTER_NAME:%s, SMR_BASE_PORT:%d, REDIS_PORT:%d" % (host, cluster_name, smr_base_port, redis_port)))
            return False

        # Start SMR process
        if execute(remote.is_smr_process_exist, smr_base_port)[host] == False:
            if execute(remote.start_smr_process, ip, smr_base_port, log_delete_delay)[host] != True:
                warn(red("[%s] Start SMR fail, PGS_ID:%d, PORT:%d" % (host, pgs_id, smr_base_port)))
                return False

        # Start Redis process
        if execute(remote.is_redis_process_exist, smr_base_port)[host] == False:
            expected = ['+OK 1', '+OK 2', '+OK 3']
            if execute(remote.start_redis_process, ip, smr_base_port, redis_port, cluster_name, expected)[host] != True:
                warn(red("[%s] Start Redis fail, PGS_ID:%d, PORT:%d" % (host, pgs_id, redis_port)))
                return False

    # Check state
    for pgs in recover_list:
        pgs_id = pgs['pgs_id']
        ip = pgs['ip']
        smr_base_port = pgs['smr_base_port']
        redis_port = pgs['redis_port']

        host = config.USERNAME + '@' + pgs['ip'].encode('ascii')
        env.hosts = [host]

        # Check redis state
        if util.check_redis_state(ip, redis_port) != True:
            warn(red("[%s] Check Redis state fail. PGS_ID:%d, PORT:%d" % (host, pgs_id, redis_port))) 
            return False

    print green("\n #### REPAIR PG '%d' SUCCESS #### \n" % (pg_id))
    print '===================================================================='
    return True

def execute_gw(cluster_name, gw_id, pm_ip, port):
    print yellow('\n #### EXECUTE GATEWAY BEGIN GWID:%d IP:%s Port:%d #### \n' % (gw_id, pm_ip, port))

    # Check local binary
    if check_local_binary_exist() == False:
        warn(red("Local binary for upgrade doesn't exist"))
        return False

    # Copy Binary
    host = config.USERNAME + '@' + pm_ip.encode('ascii')
    env.hosts = [host]

    print green('[%s] copy_binary begin' % host)
    if execute(remote.copy_binary)[host] == False:
        warn(red("[%s] Copy binary fail" % host))
        return False
    print green('[%s] copy_binary end' % host)

    # Check GW directory
    gw_path = '%s/%d' % (config.REMOTE_GW_DIR, port)
    if execute(remote.path_exist, gw_path)[host] == False:
        warn(red("[%s] GW directory does not exist. PATH:%s" % (host, gw_path)))
        return False

    # Check GW are running
    if execute(remote.is_gw_process_exist, cluster_name, port)[host] == True:
        warn(red("[%s] GW is already running. Aborting..." % host))
        return False

    # Start gateway
    if execute(remote.start_gateway, cluster_name, pm_ip, port, config.CONF_MASTER_IP, config.CONF_MASTER_PORT)[host] != True:
        warn(red("[%s] Start GW fail, GW_ID:%d, PORT:%d" % (host, gw_id, port)))
        return False

    # Check gateway connection to redis
    if util.check_gw_inactive_connections(pm_ip, port) == False:
        warn("[%s:%d] Gateway check inactive connections fail" % (pm_ip, port))
        return False

    print green('\n #### EXECUTE GATEWAY SUCCESS GWID:%d IP:%s Port:%d #### \n' % (gw_id, pm_ip, port))
    print '===================================================================='
    return True

def menu_install_cluster():
    # Check local binary
    if check_local_binary_exist(['cluster-util']) == False:
        warn(red("Local binary for upgrade doesn't exist"))
        return False

    # Input arguments
    cluster_name = prompt(cyan("Cluster name : "))
    quorum_policy = "0:1"
    pg_count = int(prompt(cyan("PG count : ")))
    rep_num = int(prompt(cyan("Replication number : ")))
    pm_list_str = prompt(cyan('PGS Physical Machine list([["PM_NAME PM_IP", "PM_NAME PM_IP"], ["PM_NAME PM_IP", "PM_NAME PM_IP"], ...])')).strip()
    gw_list_str = prompt(cyan("Gateway Physical Machine list([PM_NAME PM_IP, PM_NAME PM_IP, ...])")).strip()
    cronsave_num = int(prompt(cyan("Cronsave number")).strip())
    log_delete_delay = 60 * 60 * 24 / cronsave_num 

    return install_cluster(cluster_name, quorum_policy, pg_count, rep_num, pm_list_str, gw_list_str, cronsave_num, log_delete_delay)

def install_cluster(cluster_name, quorum_policy, pg_count, rep_num, pm_list_str, gw_list_str, cronsave_num, log_delete_delay):
    # These are constant variables, do not modify them.
    NAME = 0
    IP = 1

    pm_group_list = ast.literal_eval(pm_list_str)
    gw_pm_list = ast.literal_eval(gw_list_str)

    for i in range(len(pm_group_list)):
        for j in range(len(pm_group_list[i])):
            pm_group_list[i][j] = pm_group_list[i][j].split(' ')

    for i in range(len(gw_pm_list)):
        gw_pm_list[i] = gw_pm_list[i].split(' ')

    # Check arguments
    all_pms = [pm for pm_group in pm_group_list for pm in pm_group]
    all_pms += gw_pm_list

    for pm_group in pm_group_list:
        for j in range(len(pm_group) - 1):
            if pm_group[j][0] == pm_group[j+1][0] or pm_group[j][1] == pm_group[j+1][1]:
                warn(red("Check the group of PMs %s" % (pm_group)))
                return False

    for pm in all_pms:
        for rhs in all_pms:
            # rhs : right hand side
            if pm[NAME] == rhs[NAME] and pm[IP] != rhs[IP]:
                warn(red("Check IP of PM, NAME:%s, IP:%s" % (pm[NAME], pm[IP])))
                return False
            if pm[IP] == rhs[IP] and pm[NAME] != rhs[NAME]:
                warn(red("Check NAME of PM, NAME:%s, IP:%s" % (pm[NAME], pm[IP])))
                return False

    for pm_group in pm_group_list:
        if rep_num != len(pm_group):
            warn(red("Check pair of Physical Machine list, PM_LIST:%s" % pm_group))
            return False

    # Check cluster
    with settings(hide('warnings')):
        cluster = cm.cluster_info(cluster_name)
    if cluster != None:
        warn(red("Cluster '%s' aleady exists" % cluster_name))
        return False

    # Map PG to PM
    mean = pg_count / len(pm_group_list)
    remainder = pg_count % len(pm_group_list)

    pg_pm_counts = [mean for i in range(len(pm_group_list))]
    slot_idx = 0
    for i in range(remainder):
        pg_pm_counts[slot_idx % len(pg_pm_counts)] += 1
        slot_idx += 1

    # Make PGS Port Allocators
    pgs_max_port = 0
    pgs_port_allocators = {}
    gw_port_allocators = {}
    temp_pm_group_list = []
    for count in pg_pm_counts:
        pm_group = pm_group_list.pop(0)
        temp_pm_group_list.append(pm_group)

        for i in range(count):
            pgs_id_base = 0
            for pm in pm_group:
                # Make PGS port allocator
                host = config.USERNAME + '@' + pm[IP].encode('ascii')
                if pgs_port_allocators.has_key(host) == False:
                    pgs_port_allocators[host] = PortAllocator(host, config.REMOTE_PGS_DIR, config.PGS_BASE_PORT)
                    if pgs_max_port < pgs_port_allocators[host].get_max_port():
                        pgs_max_port = pgs_port_allocators[host].get_max_port()

    # Make GW Port Allocators
    gw_max_port = 0
    for gw_pm in gw_pm_list:
        host = config.USERNAME + '@' + gw_pm[IP].encode('ascii')
        if gw_port_allocators.has_key(host) == False:
            gw_port_allocators[host] = PortAllocator(host, config.REMOTE_GW_DIR, config.GW_BASE_PORT)
            if gw_max_port < gw_port_allocators[host].get_max_port():
                gw_max_port = gw_port_allocators[host].get_max_port()


    # Set Max Port
    if pgs_max_port - config.PGS_BASE_PORT > gw_max_port - config.GW_BASE_PORT:
        max_port = pgs_max_port - config.PGS_BASE_PORT
    else:
        max_port = gw_max_port - config.GW_BASE_PORT

    for host, port_allocator in pgs_port_allocators.items():
        port_allocator.set_max_port(max_port + config.PGS_BASE_PORT)

    for host, port_allocator in gw_port_allocators.items():
        port_allocator.set_max_port(max_port + config.GW_BASE_PORT)

    # Make PGS configuration
    pm_group_list = temp_pm_group_list

    pgs_list = []
    pg_id_base = 0
    for count in pg_pm_counts:
        pm_group = pm_group_list.pop(0)

        for i in range(count):
            pgs_id_base = 0
            for pm in pm_group:
                # Make PGS port allocator
                host = config.USERNAME + '@' + pm[IP].encode('ascii')

                pgs = {
                        'pg_id' : pg_id_base + i,
                        'pgs_id' : pgs_id_base + pg_id_base + i,
                        'pm' : pm,
                        'port' : pgs_port_allocators[host].next(),
                      }
                pgs_list.append(pgs)
                if rep_num == 2:
                    pgs_id_base += 50
                elif rep_num == 3:
                    pgs_id_base += 30
                else:
                    pgs_id_base += 20

        pg_id_base += config.ID_GAP

    # Map partition numbers to partition groups
    mean = 8192 / pg_count
    remainder = 8192 % pg_count
    pn_pg_map = [mean for i in range(pg_count)]

    slot_idx = 0
    for i in range(remainder):
        pn_pg_map[slot_idx % pg_count] += 1
        slot_idx += 1

    # Make PG configuration
    pg_list = []
    slot_no = 0
    pg_id_base = 0
    pn_pg_map_idx = 0

    for count in pg_pm_counts:
        for i in range(count):
            pg = {
                    'id'            : pg_id_base + i, 
                    'range_from'    : slot_no, 
                    'range_to'      : slot_no + pn_pg_map[pn_pg_map_idx] - 1 
                 }
            pg_list.append(pg)
            slot_no += pn_pg_map[pn_pg_map_idx]

            pn_pg_map_idx += 1
        pg_id_base += config.ID_GAP
    pg_list.sort(key=lambda x: x['id'])

    # Make GW configuration
    gw_list = []
    for gw_pm in gw_pm_list:
        host = config.USERNAME + '@' + gw_pm[IP].encode('ascii')
        gw = {
                'gw_id'     : len(gw_list) + 1,
                'pm'        : gw_pm,
                'port'      : gw_port_allocators[host].next(),
             }
        gw_list.append(gw)

    # Print script
    if confirm(cyan('\nPrint script?')):
        util.print_script(cluster_name, quorum_policy, pg_list, pgs_list, gw_list)

    print_conf = confirm(cyan('\nPrint configuration?'))

    if print_conf:
        # Show GW information
        print yellow("\n[GW INFORMATION]")
        print yellow("+-------+------------------------------------------+--------+") 
        print yellow("| GW_ID |                    PM                    |  PORT  |")
        print yellow("+-------+------------------------------------------+--------+") 
        for gw in gw_list:
            print yellow("| %(gw_id)5d | %(pm)40s | %(port)6d |" % gw)
        print yellow("+-------+------------------------------------------+--------+\n") 

        # Show PG information
        print yellow("\n[PG INFORMATION]")
        print yellow("+-------+--------------+-----------+")
        print yellow("| PG_ID |     SLOT     | SLOT SIZE |")
        print yellow("+-------+--------------+-----------+") 
        for pg in pg_list:
            pg['slot_size'] = pg['range_to'] - pg['range_from'] + 1
            print yellow("| %(id)5d | %(range_from)6d%(range_to)6d | %(slot_size)9d |" % pg)
        print yellow("+-------+--------------+-----------+") 

        # Show PGS information
        print yellow("\n[PGS INFORMATION]")
        print yellow("+-------+--------+------------------------------------------+--------+") 
        print yellow("| PG_ID | PGS_ID |                    PM                    |  PORT  |")
        print yellow("+-------+--------+------------------------------------------+--------+") 
        for pgs in pgs_list:
            print yellow("| %(pg_id)5d | %(pgs_id)6d | %(pm)40s | %(port)6d |" % pgs)
        print yellow("+-------+--------+------------------------------------------+--------+\n") 

    # Arrange masters and slaves
    temp_pgs_list = []
    j = 0
    for i in range(pg_count):
        for k in range(j%rep_num):
            temp_pgs_list.append(pgs_list[i*rep_num + k])
        j += 1

    for pgs in temp_pgs_list:
        pgs_list.remove(pgs)
        pgs_list.append(pgs)

    if print_conf:
        # Master location information
        print yellow("\n[PGS INFORMATION, After Arranging locations of masters]")
        print yellow("+-------+--------+------------------------------------------+--------+") 
        print yellow("| PG_ID | PGS_ID |                    PM                    |  PORT  |")
        print yellow("+-------+--------+------------------------------------------+--------+") 
        slave_list = []
        master_cnt_list = {}
        for pgs in pgs_list:
            if pgs['pg_id'] in slave_list:
                print "| %(pg_id)5d | %(pgs_id)6d | %(pm)40s | %(port)6d |" % pgs
            else:
                print yellow("| %(pg_id)5d | %(pgs_id)6d | %(pm)40s | %(port)6d |" % pgs)
                if master_cnt_list.get(pgs['pm'][IP]) == None:
                    master_cnt_list[pgs['pm'][IP]] = 1
                else:
                    master_cnt_list[pgs['pm'][IP]] += 1
            slave_list.append(pgs['pg_id'])
        print yellow("+-------+--------+------------------------------------------+--------+\n") 

        print yellow("[MASTER COUNT]")
        for ip, cnt in master_cnt_list.items():
            print yellow('%s : %d' % (ip, cnt))

    if not confirm(cyan('\nCreate PGS, Continue?')):
        warn(red("Aborting at user request."))
        return False

    config.confirm_mode = confirm(cyan("Confirm Mode?"))

    # Check pm
    existing_pm_list = cm.pm_ls()['list']

    # Add pm
    for pm in all_pms:
        if pm[NAME] in existing_pm_list:
            pm_json = cm.pm_info(pm[NAME])
            if pm[IP] != pm_json['pm_info']['ip']:
                warn(red("PM information isn't the same information in mgmt-cc, PM_NAME:%s, PM_IP:%s" % (pm[NAME], pm[IP])))
                return False
            continue

        if cm.pm_add(pm[NAME], pm[IP]) == False:
            return False

        existing_pm_list.append(pm[NAME])

    # Add cluster
    if cm.cluster_add(cluster_name, quorum_policy) == False: 
        warn(red('Add clsuter fail. PM_NAME:%s, PM_IP:%s' % (pm[NAME], pm[IP])))
        return False

    # Make PG configuration
    for pg in pg_list:
        pg_id = pg['id']
        range_from = pg['range_from']
        range_to = pg['range_to']
        # Add PG
        if cm.pg_add(cluster_name, pg_id) == False:
            warn(red("Add PG fail. PG_ID:%s, RANGE:%s:%s" % (pg_id, range_from, range_to)))
            return

        # Set slot to PG
        if cm.slot_set_pg(cluster_name, pg_id, range_from, range_to) == False:
            warn(red('Set slot fail. PG_ID:%s, RANGE:%s:%s' % (pg_id, range_from, range_to)))
            return

    # Install PGS
    for pgs in pgs_list:
        ret = install_pgs(cluster_name, pgs['pgs_id'], pgs['pg_id'], pgs['pm'][NAME], pgs['pm'][IP], pgs['port'], pgs['port'] + 9, cronsave_num, log_delete_delay)
        if ret != OK:
            warn(red('Install PGS fail. ERROR:%d, CLUSTER:%s, PGS_ID:%d,' % (ret, cluster_name, pgs['pgs_id'])))
            return False

    # Install GW
    for gw in gw_list:
        pm_name = gw['pm'][NAME]
        pm_ip = gw['pm'][IP]
        host = config.USERNAME + '@' + pm_ip.encode('ascii')

        # Install GW configuration
        if install_gw(cluster_name, gw['gw_id'], pm_name, pm_ip, gw['port']) == False:
            return False

    return True

def menu_uninstall_cluster():
    # Confirm mode ?
    config.confirm_mode = confirm(cyan("Confirm Mode?"))

    # Command input method
    cluster_name = prompt(cyan("Cluster information(CLUSTER)"))
    
    uninstall_cluster(cluster_name)

# primitive
def uninstall_cluster(cluster_name):
    auth_str = ''.join(random.choice(string.digits) for _ in range(5))
    s = prompt(cyan("When is your birthday? (%s)" % auth_str))
    if auth_str != s:
        warn(red("Incorrect input value. Aborting..."))
        return False

    cluster_json_data = cm.cluster_info(cluster_name)
    if cluster_json_data == None:
        warn(red("Cluster does not exist. Aborting..."))
        return False
    cluster_json_data = cluster_json_data['data']

    # Uninstall GW
    for gw_id in cluster_json_data['gw_id_list']:
        if uninstall_gw(cluster_name, gw_id) == False:
            warn(red("Uninstall gateway '%d' fail. Aborting..." % gw_id))
            return False

    # Uninstall PGS
    for pg in cluster_json_data['pg_list']:
        for pgs_id in pg['pg_data']['pgs_ID_List']:
            if uninstall_pgs(cluster_name, pgs_id, False, False) == False:
                warn(red("Uninstall PGS '%d' fail. Aborting..." % pgs_id))
                return False

    # Delete PG configuration
    if config.confirm_mode and not confirm(cyan('Delete PG. Continue?')):
        warn("Aborting at user request.")
        return False

    for pg_json in cluster_json_data['pg_list']:
        pg_id = int(pg_json['pg_id'])
        if cm.pg_del(cluster_name, pg_id) == False:
            warn(red("Delete PG '%d' configuration fail. Aborting..." % pg_id))
            return False
        else:
            print green("Delete PG '%d' success" % pg_id)

    # Delete Cluster configuration
    if config.confirm_mode and not confirm(cyan('Delete Cluster. Continue?')):
        warn("Aborting at user request.")
        return False
    
    if cm.cluster_del(cluster_name) == False:
        warn(red("Delete cluster configuration fail. Aborting..."))
        return False
    else:
        print green('\n #### UNINSTALL SUCCESS CLUSTER:%s #### \n' % (cluster_name))

def call_uninstall_gw(args):
    cluster_name = args.split(' ')[0]
    gw_id = int(args.split(' ')[1])
    
    return uninstall_gw(cluster_name, gw_id)


def menu_install_pgs():
    # Confirm mode ?
    config.confirm_mode = confirm(cyan("Confirm Mode?"))

    # Command input method
    from_prompt = confirm(cyan("Read PGS information from command line(Y) or a file(n)."))

    if from_prompt:
        # Input pgs information from command line
        s = prompt(cyan("PGS information(CLUSTER PGS_ID PG_ID PM_NAME PM_IP PORT CRONSAVE_NUM)"))
        if ' ' not in s:
            warn(red('PGS information Must be specified. Aborting...'))
            return
        if call_install_pgs(s) == False: return False
    else:
        # Get input file name
        name = prompt(cyan("Input file name:"))
        for s in fileinput.FileInput(name):
            s = s.strip()
            if len(s) == 0: continue
            if s[0] == '#': continue
            if ' ' not in s:
                warn(red('PGS information Must be specified. Aborting...'))
                return
            if call_install_pgs(s) == False: return False

def call_install_pgs(args):
    cluster_name = args.split(' ')[0]
    pgs_id = int(args.split(' ')[1])
    pg_id = int(args.split(' ')[2])
    pm_name = args.split(' ')[3]
    pm_ip = args.split(' ')[4]
    smr_base_port = int(args.split(' ')[5])
    redis_port = smr_base_port + 9
    cronsave_num = int(args.split(' ')[6])
    log_delete_delay = 60 * 60 * 24 / cronsave_num 

    if install_pgs(cluster_name, pgs_id, pg_id, pm_name, pm_ip, smr_base_port, redis_port, cronsave_num, log_delete_delay) == OK:
        return True
    else:
        return False

def menu_uninstall_pgs():
    # Confirm mode ?
    config.confirm_mode = confirm(cyan("Confirm Mode?"))

    # Delete smr-logs and rdb files after uninstall
    remain_data = confirm(cyan("Remain PGS directories in order to backup data?"))

    # Command input method
    from_prompt = confirm(cyan("Read PGS information from command line(Y) or a file(n)."))

    if from_prompt:
        # Input pgs information from command line
        s = prompt(cyan("PGS information(CLUSTER PGS_ID)"))
        if ' ' not in s:
            warn(red('PGS information Must be specified. Aborting...'))
            return
        if call_uninstall_pgs(s, remain_data) == False: return False
    else:
        # Get input file name
        name = prompt(cyan("Input file name:"))
        for s in fileinput.FileInput(name):
            s = s.strip()
            if len(s) == 0: continue
            if s[0] == '#': continue
            if ' ' not in s:
                warn(red('PGS information Must be specified. Aborting...'))
                return
            if call_uninstall_pgs(s, remain_data) == False: return False

def call_uninstall_pgs(args, remain_data):
    cluster_name = args.split(' ')[0]
    pgs_id = int(args.split(' ')[1])
    
    return uninstall_pgs(cluster_name, pgs_id, remain_data, False)
    
def menu_migration():
    config.confirm_mode = confirm(cyan("Confirm Mode?"))

    # Command input method
    from_prompt = confirm(cyan("Read Migration information from command line(Y) or a file(n)."))

    if from_prompt:
        cluster_name = prompt(cyan("Cluster name"))

        # Show PG info
        show_info.menu_show_pg_list(cluster_name)
        print ''

        # Input arguments
        s = prompt(cyan("Migration information(SRC_PG_ID DST_PG_ID RANGE_FROM RANGE_TO TPS)"))
        if ' ' not in s:
            warn(red('Migration information Must be specified. Aborting...'))
            return
       
        src_pg_id = int(s.split(' ')[0])
        dst_pg_id = int(s.split(' ')[1])
        range_from = int(s.split(' ')[2])
        range_to = int(s.split(' ')[3])
        tps = int(s.split(' ')[4])

        return call_migration(cluster_name, src_pg_id, dst_pg_id, range_from, range_to, tps)
    else:
        # Get input file name
        name = prompt(cyan("Input file name:"))
        for s in fileinput.FileInput(name):
            s = s.strip()
            if len(s) == 0: continue
            if s[0] == '#': continue
            if ' ' not in s:
                warn(red('PGS information Must be specified. Aborting...'))
                return

            cluster_name = s.split(' ')[0]
            src_pg_id = int(s.split(' ')[1])
            dst_pg_id = int(s.split(' ')[2])
            range_from = int(s.split(' ')[3])
            range_to = int(s.split(' ')[4])
            tps = int(s.split(' ')[5])

            # Show PG info
            show_info.menu_show_pg_list(cluster_name)
            print ''

            if call_migration(cluster_name, src_pg_id, dst_pg_id, range_from, range_to, tps) == False:
                return False

def call_migration(cluster_name, src_pg_id, dst_pg_id, range_from, range_to, tps):
    # Check local binary
    if check_local_binary_exist(['cluster-util']) == False:
        warn(red("Local binary for upgrade doesn't exist"))
        return False
    
    # Get cluster info from Conf Master
    cluster_json_data = cm.cluster_info(cluster_name)
    if cluster_json_data == None: return False

    # Get masters of source and destination 
    pg_list = {}
    for pg_data in cluster_json_data['data']['pg_list']:
        pg_id = int(pg_data['pg_id'])

        if pg_id != src_pg_id and pg_id != dst_pg_id:
            continue

        for pgs_id in pg_data['pg_data']['pgs_ID_List']:
            pgs_json_data = cm.pgs_info(cluster_name, pgs_id)
            if pgs_json_data == None: 
                warn(red("PGS '%s' doesn't exist." % pgs_id))
                return False

            if pg_id == src_pg_id:
                if 'M' == pgs_json_data['data']['smr_Role'].encode('ascii'):
                    src_ip = pgs_json_data['data']['pm_IP'].encode('ascii')
                    src_port = int(pgs_json_data['data']['replicator_Port_Of_SMR']) 
                    break

            if pg_id == dst_pg_id:
                if 'M' == pgs_json_data['data']['smr_Role'].encode('ascii'):
                    dst_ip = pgs_json_data['data']['pm_IP'].encode('ascii')
                    dst_port = int(pgs_json_data['data']['replicator_Port_Of_SMR'])

                    host = config.USERNAME + '@' + pgs_json_data['data']['pm_IP'].encode('ascii')
                    env.hosts = [host]
                    break

    # Copy Binary
    print green('[%s] copy_binary begin' % host)
    if execute(remote.copy_binary)[host] == False:
        warn(red("[%s] Copy binary fail" % host))
        return False
    print green('[%s] copy_binary end' % host)

    # Check TPS
    active_tps = util.redis_tps(src_ip, src_port + 9)
    if tps > 50000 or tps < active_tps:
        warn(red("TPS should be in range %d~50000" % active_tps))
        return False

    if tps < active_tps * 1.1:
        if confirm(yellow("Current service TPS is %d, thus migration can take too much time. Abort?" % active_tps)) == True:
            return False

    return migration(host, cluster_name, src_pg_id, dst_pg_id, src_ip, src_port, dst_ip, dst_port, range_from, range_to, tps)

def migration(host, cluster_name, src_pg_id, dst_pg_id, src_ip, src_port, dst_ip, dst_port, range_from, range_to, tps):
    # Check arguments
    if src_ip == None or src_port == None:
        warn(red("source master does not exist."))
        return False

    if dst_ip == None or dst_port == None:
        warn(red("destination master does not exist."))
        return False
        
    if tps < 1000:
        warn(red("TPS is larger than 1000."))
        return False

    if range_from < 0:
        warn(red("range msut start with 0"))
        return False

    if range_to > 8191:
        warn(red("range msut be less than 8192"))
        return False

    src_redis_port = src_port + 9
    dst_redis_port = dst_port + 9
    src_mgmt_port = src_port + 3

    # Check if src slot contains specified range
    cluster_json_data = cm.cluster_info(cluster_name)
    if cluster_json_data == None:
        warn(red("Get cluster info fail. Aborting..."))
        return False

    print yellow("\nPN PG MAP: %s" % cluster_json_data['data']['cluster_info']['PN_PG_Map'])

    pn_pg = cluster_json_data['data']['cluster_info']['PN_PG_Map'].split()
    slots = []
    i = 0
    while i < len(pn_pg):
        pg_id = int(pn_pg[i])
        count = int(pn_pg[i+1])
        i = i + 2
        for j in range(count):
            slots.append(pg_id)

    for slot in range(range_from, range_to+1):
        if slots[slot] != src_pg_id:
            warn(red("Invalid range. %d is not in SRC_PG '%d'" % (slot, slots[slot])))
            return False
    
    # Show keyspace sizes of src and dst, before migration
    with RedisCmd(src_ip, src_redis_port) as redis_cmd:
        before_src_kcnt = redis_cmd.info_key_count()
    with RedisCmd(dst_ip, dst_redis_port) as redis_cmd:
        before_dst_kcnt = redis_cmd.info_key_count()
    print yellow("+-------------------+---------------------------+---------------------------+")
    print yellow("| \t\t\t| SRC %-21s | DST %-21s |" % ("%s:%d" % (src_ip, src_redis_port), "%s:%d" % (dst_ip, dst_redis_port)))
    print yellow("+-------------------+---------------------------+---------------------------+")
    print yellow("| KEY COUNT(before)\t| %-25d | %-25d |" % (before_src_kcnt, before_dst_kcnt))
    print yellow("+-------------------+---------------------------+---------------------------+\n")

    if config.confirm_mode and not confirm(cyan('Check key count above. Continue?')):
        warn("Aborting at user request.")
        return False

    # TODO Check all gateways are in NORMAL state.

    # Notify migstart to dest redis
    print cyan('Command : migstart %d-%d' % (range_from, range_to))
    ret = util.command(dst_ip, dst_redis_port, 'migstart %d-%d' % (range_from, range_to))
    if ret != '+OK\r\n':
        warn(red("Migstart error. ret:%s" % ret))
        return False

    # Remote partial checkpoint
    ret, seq = execute(remote.checkpoint_and_play, src_ip, src_redis_port, dst_ip, dst_redis_port, range_from, range_to, tps)[host]
    if ret == False:
        warn(red("[%s] Copy checkpoint from source to destination fail, SRC:%s:%d, DEST:%s:%d, RANGE:%d-%d, TPS:%d" % (host, src_ip, src_redis_port, dst_ip, dst_redis_port, range_from, range_to, tps)))
        return False
    print cyan('Sequence : %d' % seq)

    # Remote catchup
    num_part = 8192
    if range_from == 0 and range_to == num_part-1:
        rle = '%d %d' % (1, num_part)
    elif range_from == 0:
        rle = '%d %d %d %d' % (1, range_to+1, 0, num_part-1-range_to)
    elif range_to == num_part-1:
        rle = '%d %d %d %d' % (0, range_from, 1, range_to-range_from+1)
    else:
        rle = '%d %d %d %d %d %d' % (0, range_from, 1, range_to-range_from+1,
                                     0, num_part-1-range_to)

    cmd = 'migrate start %s %d %d %d %d %s' % (dst_ip, dst_port, seq, tps, num_part, rle)
    print cyan('Command : ' + cmd)
    reply = util.command(src_ip, src_mgmt_port, cmd)
    if reply.find('+OK') == -1:
        warn(red('migrate start fail. REPLY:"%s"' % reply))
        return False

    time.sleep(1)

    # Migrate, wait until remote catchup done 
    # and have gateways forward operations to migration target pg.
    migseq_vector = []
    while True:
        try_mig2pc = False

        cmd = 'migrate info'
        reply = util.command(src_ip, src_mgmt_port, cmd).strip()
        seqs = reply.split()
        logseq = int(seqs[1].split(':')[1])
        migseq = int(seqs[2].split(':')[1])
        diff = logseq - migseq
        migseq_vector.append(migseq)

        totmig_rate = 0
        avgmig_rate = 0
        if len(migseq_vector) > 1:
            midx = 0
            while midx < len(migseq_vector) - 1:
                mig_amount = migseq_vector[midx + 1] - migseq_vector[midx]
                totmig_rate +=  mig_amount
                midx += 1
        avgmig_rate = totmig_rate / len(migseq_vector)
        print yellow('Mig Info(%s diff:%d), Rate(tot:%d avg:%d)' % (reply, diff, totmig_rate, avgmig_rate))

        if config.confirm_mode:
            # Manual checking
            if confirm(yellow('Wait?')) == False:
                if diff <= avgmig_rate * config.MIN_TIME_TO_ATTEMPT_MIG2PC:    # Migration seems to be finished in 0.1 second.
                    try_mig2pc = True
                else:
                    red('too many remaining logs:%d, wait...' % diff)
        else:
            # Auto checking
            if diff <= avgmig_rate * config.MIN_TIME_TO_ATTEMPT_MIG2PC:    # Migration seems to be finished in 0.1 second.
                try_mig2pc = True
            else:
                time.sleep(1)

        # Mig2PC (confirm mode)
        if try_mig2pc:
            print yellow("Try mig2pc")
            if config.confirm_mode and not confirm(cyan('Continue?')):
                warn("Aborting at user request.")
                return False

            ret = cm.mig2pc(cluster_name, src_pg_id, dst_pg_id, range_from, range_to)
            if ret == True: break;
            if confirm(cyan('Retry mig2pc? (if type n, you have to do a manual migration job without this tool.)')) == False:
                warn("Aborting at user request.")
                return False

    print green('mig2pc success.')

    # Finish migration
    cmd = 'migrate interrupt'
    print cyan('Command : ' + cmd)
    reply = util.command(src_ip, src_mgmt_port, cmd)
    if reply.find('+OK') == -1:
        warn(red('migrate start fail. REPLY:"%s"' % reply))
        return False

    # Notify migend to dest redis
    print cyan('Command : migend')
    ret = util.command(dst_ip, dst_redis_port, 'migend')
    if ret != '+OK\r\n':
        warn(red("Migend error. ret:%s" % ret))
        return False

    # Clean up migrated partition of source
    if execute(remote.rangedel, src_ip, src_redis_port, range_from, range_to, tps)[host] == False:
        warn(red("[%s] Rangedel fail, SRC:%s:%d, RANGE:%d-%d, TPS:%d" % (host, src_ip, src_redis_port, range_from, range_to, tps)))
        return False
    print cyan('[%s] Rangedel success. SRC:%s:%d, RANGE:%d-%d, TPS:%d' % (host, src_ip, src_redis_port, range_from, range_to, tps))

    # Show variations of keyspace sizes of src and dst and PN PG MAP
    cluster_json_data = cm.cluster_info(cluster_name)
    if cluster_json_data == None: 
        warn(red("Get cluster info fail. Aborting..."))
        return False
    print yellow("\nPN PG MAP: %s" % cluster_json_data['data']['cluster_info']['PN_PG_Map'])

    with RedisCmd(src_ip, src_redis_port) as redis_cmd:
        after_src_kcnt = redis_cmd.info_key_count()
    with RedisCmd(dst_ip, dst_redis_port) as redis_cmd:
        after_dst_kcnt = redis_cmd.info_key_count()
    print yellow("+-------------------+---------------------------+---------------------------+")
    print yellow("| \t\t\t| SRC %-21s | DST %-21s |" % ("%s:%d" % (src_ip, src_redis_port), "%s:%d" % (dst_ip, dst_redis_port)))
    print yellow("+-------------------+---------------------------+---------------------------+")
    print yellow("| KEY COUNT(before)\t| %-25d | %-25d |" % (before_src_kcnt, before_dst_kcnt))
    print yellow("| KEY COUNT(after)\t| %-25d | %-25d |" % (after_src_kcnt, after_dst_kcnt))
    print yellow("| KEY COUNT(diff)\t| %-25d | %-25d |" % (after_src_kcnt - before_src_kcnt, after_dst_kcnt - before_dst_kcnt ))
    print yellow("+-------------------+---------------------------+---------------------------+\n")

    return True

def menu_deploy_bash_profile():
    # ip list example : 127.0.0.100 127.0.0.101 127.0.0.102
    pm_ip = prompt(cyan("PM information('ip list' or all)"))
    if pm_ip == 'all':
        # Get pm
        pm_list = cm.pm_ls()['list']
        if pm_list == None:
            warn(red('Get pm list fail. Aborting...'))
            return False

        # Copy ARC_BASH_PROFILE to all PMs
        for pm_name in pm_list:
            pm_name = pm_name.encode('utf-8')
            pm_json = cm.pm_info(pm_name)

            if pm_json == None:
                warn(red('Get pm info fail. Aborting...'))
                return False

            ip = pm_json['pm_info']['ip']
            host = config.USERNAME + '@' + ip
            env.hosts = [host]
            if execute(remote.deploy_arc_bash_profile, ip)[host] == False:
                return False
    else:
        for ip in pm_ip.split(" "):
            host = config.USERNAME + '@' + ip
            env.hosts = [host]
            if execute(remote.deploy_arc_bash_profile, ip)[host] == False:
                return False

    return True

def init_config(config_path):
    global config

    # Load config
    sys.path.insert(0, '.')
    config = importlib.import_module(config_path)

    env.shell = config.SHELL

    # Check config
    if config.check_config() == False: 
        warn(red("Configuration is not correct."))
        return None

    # Init modules
    cm.set_config(config)
    util.set_config(config)
    remote.set_config(config)
    show_info.set_config(config)

    if cm.init() == False: 
        return None

    return config

def main(config_path="config"):
    if init_config(config_path) == None:
        return False

    # Menu
    menu = [
                ["Show cluster information", show_info.main],
                ["Upgrade PGS", menu_upgrade_pgs],
                ["Upgrade GW", menu_upgrade_gw],
                ["Install Cluster", menu_install_cluster],
                ["Install PGS", menu_install_pgs],
                ["Install GW", menu_install_gw],
                ["Uninstall Cluster", menu_uninstall_cluster],
                ["Uninstall PGS", menu_uninstall_pgs],
                ["Uninstall GW", menu_uninstall_gw],
                ["Add replication", menu_add_replication],
                ["Leave replication", menu_leave_replication],
                ["Migration", menu_migration],
                ["Repair Cluster (Cluster already installed)", menu_recover_cluster],
                ["Deploy bash_profile", menu_deploy_bash_profile],
           ]

    while True:
        print "======================================="
        for i in range(0, len(menu)):
            print yellow("%d. %s" % (i + 1, menu[i][0]))
        print yellow("x. Exit")
        print "======================================="
        try:
            s = prompt(">>")
            if s.lower() == 'x':
                break

            menu_no = int(s.split(' ')[0])
        except:
            try:
                warn(red('Invalid input', menu_no))
            except:
                pass
            continue

        if menu_no < 1 or menu_no > len(menu):
           warn(red('Invalid menu'))
           continue
        
        if menu[menu_no - 1][1] == show_info.main:
            menu[menu_no - 1][1](s)
        else:
            menu[menu_no - 1][1]()

