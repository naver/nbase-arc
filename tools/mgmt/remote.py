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

import json
import time
import telnetlib
import socket
import glob
import traceback
import sys
from fabric.api import *
from fabric.colors import *
from fabric.contrib.console import *
from fabric.contrib.files import *
import util

config = util.config

def set_config(config_module):
    global config
    config = config_module

def check_shell():
    ret = run('echo $SHELL')
    if ret.failed:
        warn(red("Examine shell environment. Aborting..."))
        return False
    if ret != '/bin/bash':
        warn(red("Bash shell is required. Aborting..."))
        return False
    return True

def shell(exec_str):
    return run(exec_str, pty=False)

def path_exist(path):
    return exists(path)

def get_ports(path):
    print magenta("\n[%s] Get using ports" % env.host_string)

    if exists(path) == False:
        return []

    ports = []
    output = run('ls %s' % path)
    for dir in output.split():
        try:
            port = int(dir)
            ports.append(port)
        except ValueError:
            pass

    return ports

def remove_remote_path(remote_path):
    if exists(remote_path) == False:
        warn("Abortin at user request. path doesn't exist")
        return False
    run('rm -rf %s' % remote_path)
    return True

def make_remote_path(remote_path):
    if exists(remote_path) == False:
        if config.confirm_mode and not confirm(cyan('[%s] Create directory, %s. Continue?' % (env.host_string, remote_path))):
            warn("Aborting at user request.")
            return False
        run('mkdir -p %s' % remote_path)
    return True

def deploy_redis_conf(cluster_name, smr_base_port, redis_port, cronsave_num=1):
    cronsave_min = (smr_base_port - config.BGSAVE_BASE) % 60
    cronsave_hour = float((3 + int((smr_base_port - config.BGSAVE_BASE) / 60)) % 24)

    remote_path = '%s/%d/redis' % (config.REMOTE_PGS_DIR, smr_base_port)
    remote_file_path = '%s/%d/redis/%d.%s.conf' % (config.REMOTE_PGS_DIR, smr_base_port, smr_base_port, cluster_name)

    if exists(remote_path) == False:
        if config.confirm_mode and not confirm(cyan('[%s] Create directory, %s. Continue?' % (env.host_string, remote_path))):
            warn("Aborting at user request.")
            return False
        run('mkdir -p %s' % remote_path)

    cronsave_opt = ''
    for i in range(cronsave_num):
        cronsave_opt += 'cronsave %d %d\\n' % (cronsave_min, int(cronsave_hour))
        cronsave_hour += 24.0 / cronsave_num
        cronsave_hour = cronsave_hour % 24

    # Make redis configuration
    redis_conf = config.make_redis_conf(
            cluster_name, smr_base_port, redis_port, config.CRONSAVE_BASE_HOUR, config.CRONSAVE_BASE_MIN, cronsave_num)
    if redis_conf == None:
        warn("[%s:%d] Make redis config fail." % (env.host_string, smr_base_port))
        return False

    conf_file_path = config.make_redis_conf_file(redis_conf)
    if conf_file_path == None:
        warn("[%s:%d] Make redis config file fail." % (env.host_string, smr_base_port))
        return False

    # Copy redis config to remote host
    put(conf_file_path, remote_file_path)
    local('rm -f %s' % conf_file_path)

    return True

def deploy_arc_bash_profile(ip):
    old_shell = env.shell
    env.shell = '/bin/bash -l -c'
    try:
        print yellow('\n #### SETUP ARC_BASH_PROFILE BEGIN. IP:%s #### \n' % ip)

        config.confirm_mode = False
        
        # Make ARC_BASH_PROFILE
        bash_profile_path = config.make_bash_profile_file(config.REMOTE_NBASE_ARC)
        if bash_profile_path  == None:
            warn("[%s:%d] Make bash profile fail." % host)
            return False

        # Copy ARC_BASH_PROFILE to remote machine
        if copy(bash_profile_path, '~/.%s' % config.ARC_BASH_PROFILE) == False:
            warn(red('[%s] Failed to write ARC_BASH_PROFILE. Aborting...' % host))
            return False
        local('rm -f %s' % bash_profile_path)

        # Have .bash_profile contain ARC_BASH_PROFILE
        with settings(warn_only=True):
            if run('cat ~/.bashrc | grep %s' % config.ARC_BASH_PROFILE).failed:
                # Write ARC_BASH_PROFILE
                bash_script = os.linesep.join([
                    'if [ -f ~/.%s ]; then' % config.ARC_BASH_PROFILE,
                    '   . ~/.%s' % config.ARC_BASH_PROFILE,
                    'fi'])
                if run('echo "%s" >> ~/.bashrc' % bash_script).failed:
                    warn(red('[%s] Failed to write ~/.bash_profile. Aborting...' % host))
                    return False

            # Check NBASE_ARC_HOME
            env.shell = old_shell 
            if run('test -n "$NBASE_ARC_HOME"').failed:
                warn(red('[%s] $NBASE_ARC_HOME is empty. Aborting...' % host))
                return False

        print green('\n #### SETUP ARC_BASH_PROFILE SUCCESS. IP:%s #### \n' % ip)
        return True

    finally:
        # Restore env.shell
        env.shell = old_shell 

def copy(local_path, remote_path):
    if config.confirm_mode and not confirm('Copy %s to [%s]%s. Continue?' % (local_path, env.host, remote_path)):
        warn("Aborting at user request.")
        return False
    put(local_path, remote_path)
    return True

def copy_binary():
    local_redis_path = config.LOCAL_BINARY_PATH + '/*-' + config.REDIS_VERSION
    local_gw_path = config.LOCAL_BINARY_PATH + '/*-' + config.GW_VERSION
    local_smr_path = config.LOCAL_BINARY_PATH + '/*-' + config.SMR_VERSION
    remote_path = config.REMOTE_BIN_DIR

    if exists(remote_path) == False:
        if config.confirm_mode and not confirm('[%s] Create directory, %s. Continue?' % (env.host_string, remote_path)):
            warn("Aborting at user request.")
            return False
        run('mkdir -p %s' % remote_path)

    if exists(remote_path + '/redis-gateway-' + config.GW_VERSION) == False:
        run('ls %s' % remote_path)
        if config.confirm_mode and not confirm('Copy NBase-ARC-GW ' + cyan(config.GW_VERSION) + ' to [%s]%s. Continue?' % (env.host, remote_path)):
            warn("Aborting at user request.")
            return False
        put(local_gw_path, remote_path)
        run('chmod +x %s/*' % remote_path)

    if exists(remote_path + '/redis-arc-' + config.REDIS_VERSION) == False:
        run('ls %s' % remote_path)
        if config.confirm_mode and not confirm('Copy NBase-ARC ' + cyan(config.REDIS_VERSION) + ' to [%s]%s. Continue?' % (env.host, remote_path)):
            warn("Aborting at user request.")
            return False
        put(local_redis_path, remote_path)
        run('chmod +x %s/*' % remote_path)

    if exists(remote_path + '/smr-replicator-' + config.SMR_VERSION) == False:
        run('ls %s' % remote_path)
        if config.confirm_mode and not confirm('Copy SMR ' + cyan(config.SMR_VERSION) + ' to [%s]%s. Continue?' % (env.host, remote_path)):
            warn("Aborting at user request.")
            return False
        put(local_smr_path, remote_path)
        run('chmod +x %s/*' % remote_path)

    return True

def is_redis_process_exist(smr_base_port):
    print magenta("\n[%s] Is redis exist?" % env.host_string)
    return is_exist('ps -ef | grep redis-arc | grep " %s\." | grep -v grep' % smr_base_port)

def is_smr_process_exist(smr_base_port):
    print magenta("\n[%s] Is SMR exist?" % env.host_string)
    return is_exist('ps -ef | grep smr-replicator | grep " -b %s" | grep -v grep' % smr_base_port)

def is_gw_process_exist(cluster_name, port):
    print magenta("\n[%s] Is GW exist?" % env.host_string)
    return is_exist('ps -ef | grep redis-gateway | grep "%s" | grep " -p %s" | grep -v grep' % (cluster_name, port))

def is_exist(cmd):
    with settings(warn_only=True):
        out = run(cmd)

    if out == '':
        return False
    else:
        return True

def check_process_stopped(max_auto_retry, f, *args):
    i = 0
    while True:
        i += 1

        if f(*args) == False:
            return True

        if i > max_auto_retry:
            if confirm(cyan("Process is running yet. Wait more until process goes down(Y) or abort(n).")) == False:
                return False

        time.sleep(0.5)

# return -1 if failed, otherwise return cronsave num.
def get_cronsave_num(smr_base_port):
    print magenta("\n[%s] Get cronsave number." % env.host_string)
    with settings(warn_only=True):
        out = run('ps -ef | grep smr-replicator | grep " -b %s" | grep -v grep' % smr_base_port)

    if out == '':
        return -1

    toks = out.split(' ')
    if '-x' not in toks:
        return 1

    idx = toks.index('-x')
    return int(86400 / int(toks[idx + 1]))

def stop_redis_process(ip, port, pid, smr_base_port):
    print magenta("\n[%s] Stop Redis" % env.host_string)
    run('ps -ef | grep %d | grep redis-arc | grep -v grep' % pid)

    if config.confirm_mode and not confirm(cyan('[%s] Stop Redis. PID:%d, %s:%d. Continue?' % (env.host_string, pid, ip, port))):
        conn.close()
        warn("Aborting at user request.")
        return False

    with settings(warn_only=True):
        if run('kill %d' % pid).failed:
            conn.close()
            return False

    if check_process_stopped(10, is_redis_process_exist, smr_base_port) == False:
        print warn(red('[%s] Stop Redis fail' % env.host_string))
        return False

    print green('[%s] Stop Redis success' % env.host_string)
    return True
        
def stop_smr_process(port):
    print magenta("\n[%s] Stop SMR" % env.host_string)
    pid = int(run("ps -ef | grep smr-replicator | grep '\-b %d' | grep -v grep | awk '{print $2}'" % port))
    run("ps -ef | grep smr-replicator | grep %d | grep -v grep" % pid)

    if config.confirm_mode and not confirm(cyan('[%s] Stop SMR. PID:%d, %s:%d. Continue?' % (env.host_string, pid, env.host, port))):
        warn("Aborting at user request.")
        return False

    with settings(warn_only=True):
        if run('kill %d' % pid).failed:
            return False

    if check_process_stopped(10, is_smr_process_exist, port) == False:
        print warn(red('[%s] Stop SMR fail' % env.host_string))
        return False

    print green('[%s] Stop SMR success' % env.host_string)
    return True

def get_checkpoint(src_ip, src_port, file_name, pn_pg_map, smr_base_port):
    path = config.REMOTE_PGS_DIR + '/' + str(smr_base_port) + '/redis'
    exec_str = "cluster-util-%s --getdump %s %d %s %s" % (config.REDIS_VERSION, src_ip, src_port, file_name, pn_pg_map)

    if config.confirm_mode and not confirm(cyan('[%s] Get checkpoint from %s:%d. Continue?' % (env.host_string, src_ip, src_port))):
        warn("Aborting at user request.")
        return False

    with cd(path):
        with settings(warn_only=True):
            if run(exec_str).failed: return False

    file_path = path + "/" + file_name
    if exists(file_path) == False:
        warn(red("%s doesn't exist." % file_path))
        return False

    with cd(path):
        run("ls -al %s" % file_path)
        new_dump_file_name = run("find . -mmin 1 | grep %s" % file_name)
        if new_dump_file_name == None or new_dump_file_name[2:] != file_name:
            if not confirm(cyan('[%s] Have to check time of above file. Correct?' % (env.host_string))):
                warn("Aborting at user request.")
                return False

    return True

def checkpoint_and_play(src_ip, src_port, dst_ip, dst_port, range_from, range_to, tps):
    seq = -1
    exec_str = "cluster-util-%s --getandplay %s %d %s %d %d-%d %d" % (config.REDIS_VERSION, src_ip, src_port, dst_ip, dst_port, range_from, range_to, tps)
    print cyan(exec_str)
    if config.confirm_mode and not confirm(cyan('[%s] Copy checkpoint from [%s:%d] to [%s:%d]. Continue?' % (env.host_string, src_ip, src_port, dst_ip, dst_port))):
        warn("Aborting at user request.")
        return False, seq

    with settings(warn_only=True):
        out = run(exec_str)
        if out.failed: return False, seq

    lines = out.split('\n')
    for line in lines:
        if line.find("Checkpoint Sequence Number:") != -1:
            seq = int(line[line.rfind(":")+1:])

    return True, seq

def rangedel(src_ip, src_port, range_from, range_to, tps):
    exec_str = "cluster-util-%s --rangedel %s %d %d-%d %d" % (config.REDIS_VERSION, src_ip, src_port, range_from, range_to, tps)
    print cyan(exec_str)
    if config.confirm_mode and not confirm(cyan('[%s] Rangedel, PGS:%s:%d, RANGE:%d-%d, Continue?' % (env.host_string, src_ip, src_port, range_from, range_to))):
        warn("Aborting at user request.")
        return False

    with settings(warn_only=True):
        if run(exec_str).failed: return Falseq

    return True

def start_smr_process(ip, smr_base_port, log_delete_delay=86400):
    print magenta("\n[%s] Start SMR" % env.host_string)
    path = config.REMOTE_PGS_DIR + '/' + str(smr_base_port) + '/smr'
    exec_str = 'smr-replicator-' + config.SMR_VERSION + ' -D -d log -l smrlog -b ' + str(smr_base_port) + ' -x ' + str(log_delete_delay)
    run('ls -al %s' % path)

    if config.confirm_mode and not confirm(cyan('[%s] Start SMR. %s:%d. Continue?' % (env.host_string, env.host, smr_base_port))):
        warn("Aborting at user request.")
        return False

    with cd(path):
        with settings(warn_only=True):
            if run(exec_str, pty=False).failed: return False

    # Check SMR State
    while True:
        print '[%s] Check SMR State. ADDR=%s:%d' % (env.host_string, env.host, smr_base_port)
        try:
            conn = telnetlib.Telnet(ip, smr_base_port + 3)
            conn.write('ping\r\n')
            ret = conn.read_until('\r\n', 1)
            conn.close()

            print yellow('[%s] >>> %s' % (env.host_string, ret.strip()))
            if '+OK 0' in ret: break

        except socket.error as e:
            print e

        time.sleep(0.5)

    print green('[%s] Start SMR success' % env.host_string)
    return True

def start_redis_process(ip, smr_base_port, redis_port, cluster_name, expected_pongs=['+OK 1']):
    print magenta("\n[%s] Start Redis" % env.host_string)
    path = config.REMOTE_PGS_DIR + '/' + str(smr_base_port) + '/redis'
    exec_str = 'redis-arc-%s %d.%s.conf' % (config.REDIS_VERSION, smr_base_port, cluster_name)
    run('ls -al %s' % path)

    if config.confirm_mode and not confirm(cyan('[%s] Start Redis. %s:%d. Continue?' % (env.host_string, env.host, redis_port))):
        warn("Aborting at user request.")
        return False

    with cd(path):
        with settings(warn_only=True):
            if run(exec_str, pty=False).failed: return False

    # Check SMR State
    while True:
        print '[%s] Check SMR State. %s:%d.' % (env.host_string, env.host, smr_base_port)
        try:
            conn = telnetlib.Telnet(ip, smr_base_port + 3)
            conn.write('ping\r\n')
            ret = conn.read_until('\r\n', 1)
            conn.close()

            print yellow('[%s] >>> %s' % (env.host_string, ret.strip()))
            ok = False
            for pong in expected_pongs:
                if pong in ret: 
                    ok = True
                    break

            if ok == True: break

        except socket.error as e:
            print e

        time.sleep(0.5)

    print green('[%s] Start Redis success' % env.host_string)
    return True

def redis_ping_check(ip, redis_port, num_gateway):
    print magenta("\n[%s] Redis ping test" % env.host_string)

    # Check Redis client connection
    while True:
        try:
            num_connected = util.get_redis_client_connection_count(ip, redis_port)
            print yellow("redis '%s:%d' client connection count:%d" % (ip, redis_port, num_connected))

            if (num_connected == num_gateway * config.NUM_WORKERS_PER_GATEWAY + config.CONF_MASTER_MGMT_CONS): 
                # Check consistency of num_connected while 1 seconds
                ok = True 
                for i in range(5):
                    cnt = util.get_redis_client_connection_count(ip, redis_port)
                    print yellow(" >> redis '%s:%d' client connection count:%d" % (ip, redis_port, cnt))
                    if cnt != num_connected:
                        ok = False
                        break
                    time.sleep(0.2)

                if ok:
                    break
            time.sleep(0.5)
        except:
            continue

    print green('[%s] Redis ping test success' % env.host_string)
    return True

def get_redis_pid(ip, port):
    try:
        conn = telnetlib.Telnet(ip, port)

        conn.write('info server\r\n')
        while True:
            ret = conn.read_until('\r\n', 1)
            if ret == '\r\n': break;
            if ret.find('process_id') != -1:
                pid = int(ret[11:].strip())
        
    except:
        warn(red('[%s] Get Redis PID fail, Can not connect to Redis server. %s:%d' % (env.host_string, ip, port)))
        conn.close()
        return False, 0

    conn.close()
    return True, pid

def bgsave_redis_server(ip, port):
    print magenta("\n[%s] BGSAVE" % env.host_string)
    if config.confirm_mode and not confirm(cyan('[%s] BGSAVE. %s:%d. Continue?' % (env.host_string, ip, port))):
        warn("Aborting at user request.")
        return False

    try:
        conn = telnetlib.Telnet(ip, port)

        conn.write('time\r\n')
        conn.read_until('\r\n', 1)
        conn.read_until('\r\n', 1)
        ret = conn.read_until('\r\n', 1)
        redis_server_time = int(ret.strip())
        conn.read_until('\r\n', 1)
        conn.read_until('\r\n', 1)

        conn.write('bgsave\r\n')
        ret = conn.read_until('\r\n', 1)
        if ret != '+Background saving started\r\n':
            warn(red('[%s] BGSAVE fail, Can not connect to Redis server. %s:%d' % (env.host_string, ip, port)))
            conn.close()
            return False
        conn.close()

        conn = telnetlib.Telnet(ip, port)
        while True:
            try:
                print yellow('[%s] >>> Bgsaving ...' % (env.host_string))
                conn.write('lastsave\r\n')
                ret = conn.read_until('\r\n', 1)
                lastsave_time = int(ret[1:].strip())
                if lastsave_time >= redis_server_time: break
            except:
                warn(red('[%s] BGSAVE status check fail, Can not read from Redis server. %s:%d' % (env.host_string, ip, port)))
                conn.close()
                conn = telnetlib.Telnet(ip, port)
            finally:
                time.sleep(0.5)
    except:
        warn(red('[%s] BGSAVE fail, Can not connect to Redis server. %s:%d' % (env.host_string, ip, port)))
        return False

    print green('[%s] BGSAVE success' % env.host_string)
    conn.close()
    return True

def get_quorum(master):
    conn = None
    ip = master['ip'].encode('ascii')
    port = master['mgmt_port']
    try:            
        conn = telnetlib.Telnet(ip, port)
        conn.write('getquorum\r\n')
        ret = conn.read_until('\r\n', 1)
        return int(ret.strip())
    except:
        warn(red('[%s] Get quorum fail, Can not connect to SMR replicator. %s:%d' % (env.host_string, ip, port)))
        return -1
    finally:
        if conn != None:
            conn.close()


def check_quorum(pg_data, quorum):
    print magenta("\n[%s] Check quorum value" % env.host_string)
    master_found = False

    try:
        # Find master
        for pgs_id, pgs_data in pg_data.items():
            ip = pgs_data['ip'].encode('ascii')
            smr_base_port = pgs_data['smr_base_port']
            
            conn = telnetlib.Telnet(ip, smr_base_port+3)
            conn.write('ping\r\n')
            ret = conn.read_until('\r\n', 1)
            if '+OK 2' in ret: 
                master_found = True
                break;
            conn.close()
        if master_found == False:
            warn(red('[%s] Check quorum fail, Can not find master smr-replicator.' % (env.host_string)))
            return False

        # Check Quroum
        while True:
            conn.write('getquorum\r\n')
            ret = conn.read_until('\r\n', 1)
            print yellow('[%s] >>> quorum:%s' % (env.host_string, ret.strip()))
            if int(ret.strip()) == quorum: 
                conn.close()
                break
            time.sleep(0.5)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
        warn(red('[%s] Check quorum fail, Can not connect to SMR replicator. %s:%d' % (env.host_string, ip, smr_base_port)))
        return False

    print green('[%s] Check quorum value success' % env.host_string)
    return True

@parallel()
def iostat():
    cmd = 'iostat -mx -d 1 2'
    with settings(warn_only=True):
        with hide('running', 'stdout', 'stderr'):
            out = run(cmd)
            if out.failed:
                warn(red("[%s] %s fail." % (env.host_string, cmd)))
                return None

    lines = out.split('\n')
    for i in range(len(lines)):
        lines[i] = lines[i].replace('\r', '')

    for line in lines:
        if line.find('Device:') != -1:
            columns = line.split(' ')
            while '' in columns:
                columns.remove('')

    iostat_map = {}

    for line in reversed(lines):
        if line.find('Device:') != -1:
            break

        if len(line) == 0:
            continue

        items = line.split(' ')
        while '' in items:
            items.remove('')

        iostat_map[items[0]] = {}
        for idx in range(len(items)):
            iostat_map[items[0]][columns[idx]] = items[idx]

    return iostat_map

def start_gateway(cluster_name, ip, port, cmip, cmport):
    print magenta("\n[%s] Start Gateway" % env.host_string)
    path = config.REMOTE_GW_DIR + '/' + str(port)
    add_opt = config.get_gw_additional_option()
    exec_str = 'redis-gateway-%s -D -l gwlog -c %s -b %d -w %d -p %d -n %s %s' % (config.GW_VERSION, cmip, cmport, config.NUM_WORKERS_PER_GATEWAY, port, cluster_name, add_opt)
    run('ls -al %s' % path)

    if config.confirm_mode and not confirm(cyan('[%s] Start Gateway. %s:%d. Continue?' % (env.host_string, env.host, port))):
        warn("Aborting at user request.")
        return False

    with cd(path):
        with settings(warn_only=True):
            if run(exec_str, pty=False).failed: return False

    # Check Gateway state
    while True:
        try:
            conn = telnetlib.Telnet(ip, port)
            conn.write('ping\r\n')
            ret = conn.read_until('\r\n', 1)
            if ret.find('+PONG') != -1:
                break
        except:
            continue

    conn.close()
    print green('[%s] Start Gateway success' % env.host_string)
    return True

def stop_gateway(cluster_name, ip, port):
    print magenta("\n[%s] Stop Gateway" % env.host_string)
    pid = int(run("ps -ef | grep redis-gateway | grep '\-p %d' | grep '\-n %s' | grep -v grep | awk '{print $2}'" % (port, cluster_name)))
    run("ps -ef | grep redis-gateway | grep %d | grep -v grep" % pid)

    if config.confirm_mode and not confirm(cyan('[%s] Stop Gateway. PID:%d, %s:%d. Continue?' % (env.host_string, pid, env.host, port))):
        warn("Aborting at user request.")
        return False

    with settings(warn_only=True):
        if run('kill %d' % pid).failed:
            return False

    time.sleep(1)

    if check_process_stopped(10, is_gw_process_exist, cluster_name, port) == False:
        print warn(red('[%s] Stop Gateway fail' % env.host_string))
        return False

    print green('[%s] Stop Gateway success' % env.host_string)
    return True
    
"""
Returns 
    int: available memory(Kb)
"""
def available_mem():
    mem = run("""free | grep "\-\/+ buffers\/cache:" | awk '{print $4}'""")
    if mem == '':
        return False, 0
    else:
        return True, int(mem)

