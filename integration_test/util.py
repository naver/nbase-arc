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

import os
import subprocess
import stat
import config
import constant as c
import telnetlib
import socket
import random
import signal
import smr_mgmt
import redis_mgmt
import gateway_mgmt
import sys
import time
import datetime
import telnet
import json
import inspect
import testbase
import load_generator
import numbers
import traceback
import string
import process_mgmt
import shutil
import exceptions
from arcci.arcci import *

proc_mgmt = process_mgmt.ProcessMgmt()

class Output(str):
    @property
    def stdout(self):
        return str(self)


class IntOutput(numbers.Integral):
    def __init__(self):
        pass


def log( msg ):
    d = datetime.datetime.now()
    log_msg = '[%s] %s' % (strtime(), msg)
    print log_msg


g_process_logfile_prefix = ""
def set_process_logfile_prefix( prefix ):
    global g_process_logfile_prefix
    g_process_logfile_prefix = prefix
    return 0


def get_process_logfile_prefix():
    global g_process_logfile_prefix
    return g_process_logfile_prefix


def open_process_logfile( id, name ):
    log_file = open( '%s/%s_%s_%d_%s' %
        (c.logdir, get_process_logfile_prefix(), name, id, strtime()), 'w' )
    return log_file


def pingpong( ip, port, timeout=1, logging=True ):
    res = None
    try:
        t = telnetlib.Telnet( ip, port, timeout )
        t.write( 'ping\r\n' )
        res = t.read_until( '\r\n', timeout )
        t.close()
        if logging:
            log( '>>> [%s:%d] %s' % (ip, port, res[:-2]) )
    except IOError as e:
        if logging:
            log( '>>> [%s:%d] %s' % (ip, port, e) )
    return res


def getseq_log( s, getlogseq=False, logging=False ):
    smr = smr_mgmt.SMR( s['id'] )
    try:
        ret = smr.connect( s['ip'], s['smr_mgmt_port'] )
        if ret != 0:
            return

        smr.write( 'getseq log\r\n' )
        response = smr.read_until( '\r\n', 1 )
        if logging:
            log('getseq log (pgs%d) = %s' % (s['id'], response[:-2]))
        smr.disconnect()

        if getlogseq:
            logseq = {}
            if response != None and response != '':
                tokens = response.split(' ')

                logseq['min'] = int(tokens[2].split(':')[1])
                logseq['commit'] = int(tokens[3].split(':')[1])
                logseq['max'] = int(tokens[4].split(':')[1])
                logseq['be'] = 0

                if response.find('be_sent'):
                    logseq['be'] = int(tokens[5].split(':')[1])
            return 0, logseq
        return 0
    except IOError:
        if getlogseq:
            return -1, None
        return -1


def log_server_state( cluster ):
    for server in cluster['servers']:
        pingpong( server['ip'], server['smr_mgmt_port'] )


def print_frame(postfix='', fname=None):
    callerframerecord = inspect.stack()[1]
    frame = callerframerecord[0]
    info = inspect.getframeinfo(frame)
    if fname == None:
        log( 'run %s - %s' % (info.function, postfix) )
    else:
        log( 'run %s - %s' % (fname, postfix) )


# return port number, but return -1 if failed
def get_unused_port( start, end ):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as msg:
        s = None
        return -1

    for port in range( start, end + 1 ):
        print port
        try:
            s.bind( ("", port) )
        except socket.error as msg:
            if port == end:
                s.close()
                return -1;
            continue
        break

    s.close()
    return port


def shell_cmd_sync( cmd, expected_exit_status ):
    ret = os.system( cmd )
    if ret <> expected_exit_status:
        print "ret of cmd [%s] is %d" % (cmd, ret)
        return -1
    return 0


devnull = open( os.devnull, 'w' )
def exec_proc_async( working_dir,
                     args,
                     is_shell,
                     in_handle=devnull,
                     out_handle=1,
                     err_handle=2 ):
    old_cwd = os.path.abspath( os.getcwd() )
    os.chdir( working_dir )
    p = subprocess.Popen( args,
                          preexec_fn=os.setsid,
                          shell=is_shell,
                          stdout=out_handle,
                          stderr=err_handle,
                          close_fds=True )
    os.chdir( old_cwd )

    return p;


def local(cmd_arg, shell=None, ok_ret_codes=[0]):
    log('>>> execute "%s"' % cmd_arg)

    out_stream = subprocess.PIPE
    err_stream = subprocess.PIPE

    if shell is not None:
        p = subprocess.Popen(cmd_arg, shell=True, stdout=out_stream,
                             stderr=err_stream, executable=shell)
    else:
        p = subprocess.Popen(cmd_arg, shell=True, stdout=out_stream,
                             stderr=err_stream)
    (stdout, stderr) = p.communicate()

    out = Output(stdout.strip() if stdout else "")
    err = Output(stderr.strip() if stderr else "")
    out.return_code = p.returncode
    out.stderr = err
    if p.returncode not in ok_ret_codes:
        out.succeeded = False
        log(">>> local() encountered an error (return code %s) while executing '%s'" % (p.returncode, cmd_arg))
    else:
        out.succeeded = True

    return out


def sudo(cmd_arg):
    return local('ssh %s@localhost "sudo %s"' % (config.sudoer, cmd_arg))


## Add virtual network interface
#  @param vni_name name of virtual network interface
#  @param ip virtual ip of virtual network interface
#  @param netmask netmask of virtual network interface
#  @return If no error occurs, it returns True. Otherwise, it returns False.
def nic_add(vni_name, ip, netmask='255.255.255.0'):
    out = sudo('/sbin/ifconfig %s %s netmask %s' % (vni_name, ip, netmask))
    if out.succeeded == False:
        log('failed to add NIC. name:%s, ip:%s, netmask:%s' % (vni_name, ip, netmask))
        return False

    return True


## Delete virtual network interface
#  @param vni_name name of virtual network interface
#  @return If no error occurs, it returns True. Otherwise, it returns False.
def nic_del(vni_name):
    out = sudo('/sbin/ifconfig %s down' % (vni_name))
    if out.succeeded == False:
        log('failed to delete NIC. name:%s' % (vni_name))
        return False

    return True


def kill_proc( popen ):
    os.killpg( popen.pid, signal.SIGTERM )
    #popen.wait()
    popen.communicate()
    popen.poll()


def _killps_y( name ):
    pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]

    for pid in pids:
        try:
            cmdline = open(os.path.join('/proc', pid, 'cmdline'), 'rb').read()
            if name in cmdline:
                os.kill(int(pid), signal.SIGKILL)
        except IOError:
            continue

def write_executable_file( data, dir, file_name ):
    if not os.path.exists( dir ):
        os.mkdir( dir )

    path = '%s/%s' % (dir, file_name)
    try:
        f = open( path, 'wb' )
        f.write( data )
        f.close()
    except IOError as e:
        log( e )
        log( '[error] could not open a file(%s/%s)' % (dir, file_name) )
        return -1

    os.chmod( path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )
    return 0


def cm_command( ip, port, cmd, line=1 ):
    t = telnetlib.Telnet( ip, port )
    t.write( cmd + '\n' )
    ret = ''
    i = 0
    while i < line:
        ret += t.read_until('\r\n')
        i += 1
    t.close()
    return ret


def cm_success(reply):
    try:
        jsonObj = json.loads(reply, encoding='ascii')
        if jsonObj['state'] != 'success':
            log('REPLY:%s' % reply[:-1])
            return False, None
        if jsonObj.has_key('data'):
            return True, jsonObj['data']
        return True, None
    except ValueError as e:
        log('reply: "%s", e: "%s"' %(reply.strip(), str(e)))
        return False, None


def cluster_info(mgmt_ip, mgmt_port, cluster_name):
    reply = cm_command(mgmt_ip, mgmt_port, 'cluster_info %s' % cluster_name)
    log('cluster_info ret : %s' % reply)
    ret = json.loads(reply)
    if ret == None:
        log('cluster_info fail, cluster_name:%s' % cluster_name)
        return None

    state = ret['state']
    if 'success' == state:
        return ret['data']
    else:
        return None


def pg_info(mgmt_ip, mgmt_port, cluster_name, pg_id):
    reply = cm_command(mgmt_ip, mgmt_port, 'pg_info %s %d' % (cluster_name, pg_id))
    log('pg_info ret : %s' % reply)
    ret = json.loads(reply)
    if ret == None:
        log('cluster_info fail, cluster_name:%s, pg_id:%d' % (cluster_name, pg_id))
        return None

    state = ret['state']
    if 'success' == state:
        return ret['data']
    else:
        return None


def get_pgs_info_closure(mgmt_ip, mgmt_port, cluster_name, pgs_id):
    def get_pgs_info_f():
        return get_pgs_info(mgmt_ip, mgmt_port, cluster_name, pgs_id)
    return get_pgs_info_f


def get_pgs_info(mgmt_ip, mgmt_port, cluster_name, pgs_id):
    reply = cm_command(mgmt_ip, mgmt_port, 'pgs_info %s %s\r\n' % (cluster_name, pgs_id))
    ret = json.loads(reply)
    pgs_info = ret['data']
    pgs_info['pgs_id'] = pgs_id
    return pgs_info


def get_pgs_info_list(mgmt_ip, mgmt_port, cluster):
    infoList= []
    for s in cluster['servers']:
        info = get_pgs_info(mgmt_ip, mgmt_port, cluster['cluster_name'], s['id'])
        infoList.append(info)
    return infoList


def get_pgs_info_all(mgmt_ip, mgmt_port, cluster_name, pgs_id):
    reply = cm_command(mgmt_ip, mgmt_port, 'pgs_info_all %s %s\r\n' % (cluster_name, pgs_id))
    ret = json.loads(reply)
    pgs_info = ret['data']
    return pgs_info


def check_cluster_closure(cluster_name, mgmt_ip, mgmt_port, state=None, check_quorum=False):
    def check_cluster_f():
        return check_cluster(cluster_name, mgmt_ip, mgmt_port, state, check_quorum)
    return check_cluster_f

def check_cluster(cluster_name, mgmt_ip, mgmt_port, state=None, check_quorum=False):
    log('')
    log('===================================================================')
    log('CLUSTER %s' % cluster_name)

    ok = True
    cluster = cluster_info(mgmt_ip, mgmt_port, cluster_name)
    for pg in sorted(cluster['pg_list'], key=lambda k: int(k['pg_id'])):
        pg['pg_id']
        log('')
        log('PG %s' % pg['pg_id'])

        master_count = 0
        slave_count = 0
        quorum = -1
        expected_quorum = -1
        pgs_id_list = pg['pg_data']['pgs_ID_List']
        for pgs_id in pgs_id_list:
            pgs_info = get_pgs_info(mgmt_ip, mgmt_port, cluster_name, pgs_id)
            ip = pgs_info['pm_IP']
            port = pgs_info['management_Port_Of_SMR']
            role = smr_role(ip, port, False)
            msg = '%s:%s a%s m%s %s %d' % (ip, port, role, pgs_info['smr_Role'], pgs_info['color'][:1], role.ts)
            if role == pgs_info['smr_Role']:
                log('+%s' % msg)
                if role == 'M':
                    master_count = master_count + 1
                    master = {'id':pgs_id,'ip':ip,'smr_mgmt_port':port}
                    quorum = get_quorum(master)
                elif role == 'S':
                    slave_count = slave_count + 1
            else:
                log('@%s' % msg)

            if state != None:
                state.append({'pgs_id':pgs_id, 'ip':ip, 'port':port, 'mgmt_role':pgs_info['smr_Role'], 'active_role':role, 'active_ts':role.ts,'quorum':quorum,'color':pgs_info['color']})

        expected_slave_count = 1
        if len(pgs_id_list) == 3:
            expected_slave_count = 2

        if check_quorum:
            expected_quorum = slave_count
            if False == (master_count == 1 and slave_count == expected_slave_count and quorum == expected_quorum):
                ok = False

            log('+master_count=%d, slave_count=%d, quorum=%s, expected_quorum=%d' %
                    (master_count, slave_count, quorum, expected_quorum))
        else:
            if False == (master_count == 1 and slave_count == expected_slave_count):
                ok = False

            log('+master_count=%d, slave_count=%d, quorum=%s' %
                    (master_count, slave_count, quorum))

    return ok


def check_quorum( cluster_name, mgmt_ip, mgmt_port ):
    ok = True
    cluster = cluster_info(mgmt_ip, mgmt_port, cluster_name)
    for pg in sorted(cluster['pg_list'], key=lambda k: int(k['pg_id'])):
        pg_id = pg['pg_id']
        log('')
        log('PG %s' % pg_id)

        master_count = 0
        slave_count = 0
        quorum = -1
        expected_quorum = -1
        pgs_id_list = pg['pg_data']['pgs_ID_List']
        for pgs_id in pgs_id_list:
            pgs_info = get_pgs_info(mgmt_ip, mgmt_port, cluster_name, pgs_id)
            ip = pgs_info['pm_IP']
            port = pgs_info['management_Port_Of_SMR']
            role = smr_role(ip, port, False)
            msg = '%s:%s a(%s) m(%s) %d' % (ip, port, role, pgs_info['smr_Role'], role.ts)
            if role == pgs_info['smr_Role']:
                log('+%s' % msg)
                if role == 'M':
                    master_count = master_count + 1
                    master = {'id':pgs_id,'ip':ip,'smr_mgmt_port':port}
                    quorum = get_quorum(master)
                elif role == 'S':
                    slave_count = slave_count + 1
            else:
                log('@%s' % msg)

        expected_slave_count = 1
        if len(pgs_id_list) == 3:
            expected_slave_count = 2

        qp = json.loads(cluster['cluster_info']['Quorum_Policy'])
        if len(qp) >= master_count + slave_count:
            expected_quorum = int(qp[master_count + slave_count - 1])
        else:
            expected_quorum = int(qp[len(qp) - 1])

        if quorum != expected_quorum:
            ok = False

        log('+master_count=%d, slave_count=%d, quorum=%d, expected_quorum=%d' %
                (master_count, slave_count, quorum, expected_quorum))
    return ok


def get_rand_gateway( cluster ):
    server = random.choice( cluster['servers'] )
    return server['ip'], server['gateway_port']


def deploy_gateway(id):
    try:
        copy_gw( id )

    except IOError as e:
        log(e)
        log('Error: can not find file or read data')
        return False

    return True


def deploy_pgs(id):
    try:
        log('copy binaries, server_id=%d' % id)
        copy_smrreplicator( id )
        copy_redis_server( id )
        copy_cluster_util( id )
        copy_dump_util( id )
        copy_dump_util_plugin( id )
        copy_log_util( id )

    except IOError as e:
        log(e)
        log('Error: can not find file or read data')
        return False

    return True


def check_gateway_state( cluster_name, leader_cm, server ):
    cmd = 'gw_info %s %d' % (cluster_name, server['id'])
    reply = cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
    if reply == None:
        log('Check gateway state : confmaster return None, cmd="%s"' % cmd)
        return False

    try:
        jobj = json.loads(reply)
        if jobj['state'] != 'success':
            log('Check gateway state : unexpected return from confmaster, cmd="%s", ret="%s"' % (cmd, reply))
            return False
    except:
        log('Check gateway state : unexpected return from confmaster, cmd="%s", ret="%s"' % (cmd, reply))
        return False

    if jobj['data']['state'] == 'F':
        log('Check gateway state : gateway state is \'F\'.')
        return False

    if 0 != check_if_gateway_is_running_properly( server['ip'], server['gateway_port'] ):
        log('Check gateway state : ping to gateway fail.')
        return False

    return True


def get_cm_by_role( servers, target_role ):
    for server in servers:
        cmd = 'not_existing_cmd'
        res = cm_command( server['ip'], server['cm_port'], cmd )
        jobj = json.loads( res )
        if target_role == c.CC_LEADER and jobj['state'] == 'error':
            return server
        elif target_role == c.CC_FOLLOWER and jobj['state'] == 'redirect':
            return server
    return None


"""@param target_role master | slave"""
def get_server_by_role( servers, target_role ):
    for server in servers:
        role = get_role_of_server( server )
        if target_role == 'master' and role == c.ROLE_MASTER:
            return server
        elif target_role == 'slave' and role == c.ROLE_SLAVE:
            return server

    return None


def get_server_by_role_and_pg( servers, target_role, pg_id ):
    attr = 'curr_role'
    attr_len = len( attr )

    for server in servers:
        if server['pg_id'] != pg_id:
            continue

        role = get_role_of_server( server )
        if target_role == 'master' and role == c.ROLE_MASTER:
            return server
        elif target_role == 'slave' and role == c.ROLE_SLAVE:
            return server

    return None


# return PGD ID  of new master, if error occurs, return -1
def role_change( cc, cluster_name, pgs_id ):
    cmd = 'role_change %s %d' % ( cluster_name, pgs_id )
    reply = cm_command( cc['ip'], cc['cm_port'], cmd )
    log('cmd: "%s", reply: "%s"' % (cmd, reply))
    jobj = json.loads(reply)
    if jobj['state'] != 'success':
        log( 'Change role fail. CMD:%s, REPLY:%s' % ( cmd, reply[:-1] ) )
        return -1

    if jobj.get( 'data' ) == None:
        return pgs_id
    else:
        return int(jobj['data']['master'])

def pg_dq(cm, cluster_name, pg_id):
    cmd = 'pg_dq %s %d' % (cluster_name, pg_id)
    reply = cm_command(cm['ip'], cm['cm_port'], cmd)
    jsonObj = json.loads(reply)
    if jsonObj['state'] != 'success':
        log('pg_dq fail. CMD:%s, REPLY:%s' % (cmd, reply[:-1]))
        return False
    return True

def check_if_smr_is_running_properly( ip, mgmt_port, timeout=1 ):
    max_try = 100
    error = False
    for try_count in range( 0, max_try ):
        time.sleep( 0.2 )

        error = False
        try:
            t = telnetlib.Telnet( ip, mgmt_port )
            t.write( 'ping\r\n' )
            response = t.read_until( '\r\n', timeout )
            t.close()
            if response.find('+OK') is -1:
                error = True
        except socket.error as e:
            log(e)
            error = True

        if not error:
            break;

    if error:
        log('smr-replicator is not running properly.')
        return -1

    return 0


def check_if_redis_is_running_properly( redis_port, timeout=1, max_try=30 ):
    error = False
    for try_count in range( 0, max_try ):
        time.sleep(1.0)

        error = False
        try:
            t = telnetlib.Telnet( 'localhost', redis_port)
            t.write( 'bping\r\n' )
            response = t.read_until( '\r\n', timeout )
            t.close()
            log('response from redis : %s' % (response))
            if response.find('+PONG') is -1:
                error = True
        except socket.error as e:
            log(e)
            error = True

        if not error:
            break

    if error:
        log('redis-arc is not running properly.')
        return -1

    return 0


def check_if_gateway_is_running_properly( ip, gw_port, timeout=1 ):
    max_try = 20
    error = False
    for try_count in range( 0, max_try ):
        time.sleep( 0.2 )
        response = ''

        error = False
        try:
            t = telnetlib.Telnet( ip, gw_port )
            t.write( 'ping\r\n' )
            response = t.read_until( '\r\n', timeout )
            t.close()
            if response.find('+PONG') is -1:
                error = True
        except socket.error as e:
            log(e)
            error = True

        if not error:
            break

    if error:
        log('gateway is not running properly. response:%s' % response)
        return -1

    return 0


def get_tps(ip, port, type='gw'):
    con = None
    try:
        con = telnetlib.Telnet(ip, port)

        if type == 'gw':
            con.write('info gateway\r\n')
        elif type == 'redis':
            con.write('info\r\n')

        reply = con.read_until('\r\n')
        size = int(reply[1:])

        readlen = 0
        while readlen <= size:
            reply = con.read_until('\r\n')
            readlen += len(reply)
            reply.strip()
            if reply.find(':'):
                tokens = reply.split(':')
                if type == 'gw':
                    if tokens[0] == 'gateway_instantaneous_ops_per_sec':
                        tps = int(tokens[1])
                        return tps
                elif type == 'redis':
                    if tokens[0] == 'instantaneous_replied_ops_per_sec':
                        tps = int(tokens[1])
                        return tps

    except IOError as e:
        log(e)
        return -1
    except TypeError as e:
        log(e)
        return -1
    finally:
        if con != None:
            con.close()


## Check ops of all redis or all gateway in servers
#  @param servers The target servers to check ops
#  @param type It indicates the type of target.
#  @param condition. It applies to all, in order to check ops. ex: (lambda s: (s['ops'] > 50))
#  @return If success, it returns True. Otherwise, it returns False.
def check_ops(servers, type, condition):
    assert (type == 'gw' or type == 'redis')

    ops_list = map(lambda s: {'id':s['id'], 'ops':get_tps(s['ip'], s['gateway_port'], type)}, servers)
    return len(filter(condition, ops_list)) == len(servers)


def pm_add(name, ip, mgmt_ip, mgmt_port):
    cmd = 'pm_add %s %s' % (name, ip)
    result = cm_command( mgmt_ip, mgmt_port, cmd )
    jobj = json.loads(result)
    if jobj['state'] != 'success':
        log('failed to execute. cmd:%s, result:%s' % (cmd, result))
        return False
    return True


def pg_del(cluster, servers, leader_cm, stop_gw=True):
    for server in servers:
        if testbase.finalize_info_of_cm_about_pgs(cluster, server, leader_cm) is not 0:
            log('scale in : failed to finalize cc_info, pgs_del command faile')
            return False

    cmd = 'pg_del %s %d' % (cluster['cluster_name'], servers[0]['pg_id'])
    result = cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
    jobj = json.loads(result)
    if jobj['state'] != 'success':
        log('failed to execute. cmd:%s, result:%s' % (cmd, result))
        return False

    for server in servers:
        if testbase.request_to_shutdown_redis(server) is not 0:
            log('scale in : failed to request_to_shutdown_redis')
            return False

    for server in servers:
        if testbase.request_to_shutdown_smr(server) is not 0:
            log('scale in : failed to request_to_shutdown_smr')
            return False

    if stop_gw:
        for server in servers:
            if testbase.request_to_shutdown_gateway(cluster['cluster_name'], server, leader_cm) is not 0:
                log('scale in : failed to request_to_shutdown_gateway')
                return False

    return True


def pg_add(cluster, servers, leader_cm, start_gw=True):
    for server in servers:
        ret = delete_smr_log_dir( server['id'], server['smr_base_port'] )
        if ret is not 0:
            log('failed to delete smr log dir')
            return False

        ret = delete_redis_check_point( server['id'] )
        if ret is not 0:
            log('failed to delete redis check point')
            return False

    cmd = 'pg_add %s %d' % (cluster['cluster_name'], servers[0]['pg_id'])
    result = cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
    jobj = json.loads(result)
    if jobj['state'] != 'success':
        log('failed to execute. cmd:%s, result:%s' % (cmd, result))
        return False

    for server in servers:
        testbase.initialize_info_of_cm_about_pgs(cluster, server, leader_cm)

    for server in servers:
        if testbase.request_to_start_smr( server ) is not 0:
            return False

    for server in servers:
        if testbase.request_to_start_redis( server, check=False ) is not 0:
            return False

    time.sleep( 10 )
    for server in servers:
        if testbase.wait_until_finished_to_set_up_role( server ) is not 0:
            return False

    if start_gw:
        for server in servers:
            if testbase.request_to_start_gateway( cluster['cluster_name'], server, leader_cm ) is not 0:
                log('failed to request_to_start_gateway')
                return False

    return True


def pgs_add(cluster, server, leader_cm, pg_id=None, rm_ckpt=True):
    ret = delete_smr_log_dir( server['id'], server['smr_base_port'] )
    if ret is not 0:
        log('failed to delete smr log dir')
        return False

    if rm_ckpt:
        ret = delete_redis_check_point( server['id'] )
        if ret is not 0:
            log('failed to delete redis check point')
            return False

    testbase.initialize_info_of_cm_about_pgs(cluster, server, leader_cm, pg_id=pg_id)

    if testbase.request_to_start_smr( server ) is not 0:
        return False

    if testbase.request_to_start_redis( server ) is not 0:
        return False

    time.sleep( 10 )
    if testbase.wait_until_finished_to_set_up_role( server ) is not 0:
        return False

    return True


def gw_add(cluster_name, gw_id, pm_name, pm_ip, gw_port, mgmt_ip, mgmt_port):
    cmd = 'gw_add %s %d %s %s %d' % (cluster_name, gw_id, pm_name, pm_ip, gw_port)
    ret = cm_command(mgmt_ip, mgmt_port, cmd)
    if ret != None and len(ret) >= 2:
        ret = ret[:-2]
    log('cmd:"%s", ret:"%s"' % (cmd, ret))
    if not ret.startswith('{"state":"success","msg":"+OK"}'):
        log('failed to add gateway')
        return False
    return True


def gw_del(cluster_name, gw_id, mgmt_ip, mgmt_port):
    cmd = 'gw_del %s %d' % (cluster_name, gw_id)
    ret = cm_command(mgmt_ip, mgmt_port, cmd)
    if ret != None and len(ret) >= 2:
        ret = ret[:-2]
    log('cmd:"%s", ret:"%s"' % (cmd, ret))
    if not ret.startswith('{"state":"success","msg":"+OK"}'):
        log('failed to delete gateway')
        return False
    return True


def cluster_util_getdump(dest_id, src_ip, src_port, target_rdb_filename, range_from, range_to):
    # remote partial checkpoint
    cmd = "./cluster-util --getdump %s %d %s %d-%d" % (src_ip, src_port,
                          target_rdb_filename, range_from, range_to, )
    p = exec_proc_async(cluster_util_dir(dest_id), cmd, True, None, subprocess.PIPE, None)

    ret = p.wait()
    for line in p.stdout:
        if line.find("Checkpoint Sequence Number:") != -1:
            log("seqnumber : " + line[line.rfind(":")+1:])
            seq = int(line[line.rfind(":")+1:])
        log(">>>" + str(line.rstrip()))

    if ret != 0:
        return False
    return True


def migration(cluster, src_pg_id, dst_pg_id, range_from, range_to, tps, num_part=8192):
    log("== Start migration from pg(%d) to pg(%d), range(%d-%d) ==" % (src_pg_id, dst_pg_id,
                                                                         range_from, range_to))
    leader_cm = cluster['servers'][0]
    src_master = get_server_by_role_and_pg( cluster['servers'], 'master', src_pg_id )
    dst_master = get_server_by_role_and_pg( cluster['servers'], 'master', dst_pg_id )

    src_redis = redis_mgmt.Redis(src_master['id'])
    ret = src_redis.connect(src_master['ip'], src_master['redis_port'])

    dst_redis = redis_mgmt.Redis(dst_master['id'])
    ret = dst_redis.connect(dst_master['ip'], dst_master['redis_port'])

    smr = smr_mgmt.SMR(src_master['id'])
    ret = smr.connect(src_master['ip'], src_master['smr_mgmt_port'])
    if ret != 0:
        log('failed to connect to smr(source master)')
        return False

    cmd = 'migconf migstart %d-%d\r\n' % (range_from, range_to)
    dst_redis.write(cmd)
    res = dst_redis.read_until('\r\n')

    # remote partial checkpoint
    cmd = "./cluster-util --getandplay %s %d %s %d %d-%d %d" % (src_master['ip'], src_master['redis_port'],
                          dst_master['ip'], dst_master['redis_port'],
                          range_from, range_to, tps)
    p = exec_proc_async(cluster_util_dir(src_master['id']), cmd, True, None, subprocess.PIPE, None)

    ret = p.wait()
    for line in p.stdout:
        if line.find("Checkpoint Sequence Number:") != -1:
            log("seqnumber : " + line[line.rfind(":")+1:])
            seq = int(line[line.rfind(":")+1:])
        log(">>>" + str(line.rstrip()))

    if ret != 0:
        return False

    # remote catchup (smr log migration)
    dst_host = dst_master['ip']
    dst_port = dst_master['smr_base_port']

    if range_from == 0 and range_to == num_part-1:
        rle = '%d %d' % (1, num_part)
    elif range_from == 0:
        rle = '%d %d %d %d' % (1, range_to+1, 0, num_part-1-range_to)
    elif range_to == num_part-1:
        rle = '%d %d %d %d' % (0, range_from, 1, range_to-range_from+1)
    else:
        rle = '%d %d %d %d %d %d' % (0, range_from, 1, range_to-range_from+1,
                                     0, num_part-1-range_to)

    smr.write('migrate start %s %d %d %d %d %s\r\n' % (dst_host, dst_port,
                                                       seq, tps, num_part, rle))
    response = smr.read_until('\r\n')
    if response[:3] != '+OK':
        log('failed to execute migrate start command, response:%s' % response)
        return False

    while True:
        smr.write('migrate info\r\n')
        response = smr.read_until('\r\n')
        seqs = response.split()
        logseq = int(seqs[1].split(':')[1])
        mig = int(seqs[2].split(':')[1])
        log('migrate info: %s' % response)
        time.sleep(1)
        if (logseq-mig < 500000):
            log('Remote catchup almost done. try mig2pc')
            break

    # mig2pc
    cmd = 'mig2pc %s %d %d %d %d' % (cluster['cluster_name'], src_pg_id, dst_pg_id,
                                     range_from, range_to)
    result = cm_command(leader_cm['ip'], leader_cm['cm_port'], cmd)
    log('mig2pc result : ' + result)
    if not result.startswith('{"state":"success","msg":"+OK"}\r\n'):
        log('failed to execute mig2pc command, result:%s' % result)
        return False

    # finish migration
    smr.write('migrate interrupt\r\n')
    response = smr.read_until('\r\n')
    log('migrate interrupt: %s' % response)
    smr.disconnect()

    cmd = 'migconf migend\r\n'
    dst_redis.write(cmd)
    res = dst_redis.read_until('\r\n')

    cmd = 'migconf clearstart %d-%d\r\n' % (range_from, range_to)
    src_redis.write(cmd)
    res = src_redis.read_until('\r\n')

    # clean up migrated partition of source
    cmd = "./cluster-util --rangedel %s %d %d-%d %d" % (src_master['ip'], src_master['redis_port'],
                          range_from, range_to, tps)
    p = exec_proc_async(cluster_util_dir(src_master['id']), cmd, True, None, subprocess.PIPE, None)
    ret = p.wait()
    for line in p.stdout:
        log(">>>" + str(line.rstrip()))

    if ret != 0:
        return False

    cmd = 'migconf clearend\r\n'
    src_redis.write(cmd)
    res = src_redis.read_until('\r\n')

    return True


def migrate_mgmt_cluster(cluster_name, src_cm_ip, src_cm_port, src_zk_addr, dest_cm_ip, dest_cm_port, dest_zk_addr):
    arcci_log = 'bin/log/arcci_log64'

    # src <- load generation
    src_arcci = ARC_API(src_zk_addr, cluster_name, logFilePrefix = arcci_log, so_path = c.ARCCI_SO_PATH)
    slg = load_generator.LoadGenerator_ARCCI(0, src_arcci)
    slg.start()

    # src_cm <- cluster_off
    if cluster_off(src_cm_ip, src_cm_port, cluster_name) == False:
        log("cluster_off fail")
        return False

    # src_cm -- cluster_dump --> dest_cm
    ret, cdump = cluster_dump(src_cm_ip, src_cm_port, cluster_name)
    if ret == False:
        log("cluster_dump fail")
        return False

    ret = cluster_load(dest_cm_ip, dest_cm_port, cdump)
    if ret == False:
        log("cluster_load fail")
        return False

    # dest_cm <- cluster_on
    if cluster_on(dest_cm_ip, dest_cm_port, cluster_name) == False:
        log("cluster_on fail")
        return False

    # dest <- load generation
    dest_arcci = ARC_API(dest_zk_addr, cluster_name, logFilePrefix = arcci_log, so_path = c.ARCCI_SO_PATH)
    dlg = load_generator.LoadGenerator_ARCCI(1, dest_arcci)
    dlg.start()

    # src <- stop load generation
    slg.quit()
    slg.join()
    if await(5, False)(lambda c: c, lambda : slg.consistency) == False:
        log("consistency error on load_generator with src_zk")
        return False

    # src_cm <- cluster_purge
    if cluster_purge(src_cm_ip, src_cm_port, cluster_name) == False:
        log("cluster_on fail")
        return False

    # dest <- stop load generation
    dlg.quit()
    dlg.join()
    if await(5, False)(lambda c: c, lambda : dlg.consistency) == False:
        log("consistency error on load_generator with dest_zk")
        return False

    return True


def get_smr_state( server, leader_cm ):
    return _get_smr_state( server['id'], server['cluster_name'], leader_cm['ip'], leader_cm['cm_port'] )

def _get_smr_state( id, cluster_name, mgmt_ip, mgmt_port ):
    cc = telnet.Telnet( 'cc' )
    ret = cc.connect( mgmt_ip, mgmt_port )
    if ret is not 0:
        log( 'failed to connect to cc, id:%d' % id )
        return None

    cc.write( 'pgs_info %s %d\r\n' %(cluster_name, id) )
    response = cc.read_until( '\r\n' )
    if response is None:
        log( 'error, response from pgs:%d is None.' % id )
        return None

    jobj = json.loads( response )
    if jobj['state'] != 'success':
        log( 'error, response of pgs_info, id:%d, res:%s' % (id, response) )
        return None

    return jobj['data']['state']


def get_smr_role_of_cm( server, leader_cm ):
    cc = telnet.Telnet( 'cc' )
    ret = cc.connect( leader_cm['ip'], leader_cm['cm_port'] )
    if ret is not 0:
        log( 'failed to connect to cc, id:%d' % server['id'] )
        return None

    cc.write( 'pgs_info %s %d\r\n' %(server['cluster_name'], server['id']) )
    response = cc.read_until( '\r\n' )
    if response is None:
        log( 'error, response from pgs:%d is None.' % server['id'] )
        return None

    jobj = json.loads( response )
    if jobj['state'] != 'success':
        log( 'error, response of pgs_info, id:%d' % server['id'] )
        return None

    return jobj['data']['smr_Role']


def get_role_of_server( server ):
    return _get_role_of_server( server['ip'], server['smr_mgmt_port'] )


"""@return role(0,1,2,3), state_timestamp"""
def _get_role_of_server( ip, smr_mgmt_port, verbose=True ):
    out = Output('')
    out.ts = -1

    try:
        smr = smr_mgmt.SMR( smr_mgmt_port )
        if smr.connect( ip, smr_mgmt_port, timeout=1, verbose=verbose ) is not 0:
            if verbose:
                log('get_role_of_server(), server(%s:%d) is stopped.' % (ip, smr_mgmt_port))
            return out

        smr.write( 'ping\r\n' )
        response = smr.read_until( '\r\n', 1 )
        smr.disconnect()
        if response is None:
            if verbose:
                log('response is None, IP:%s, PORT:%d' % (ip, smr_mgmt_port))
            return out

        data = str.split( response, " " )
        if len( data ) < 3:
            if verbose:
                log( 'invalied response:%s, IP:%s, PORT:%d' % (response, ip, smr_mgmt_port) )
            return out

    except IOError as e:
        log( 'IOError %s' % e )
        return out

    out = Output(data[1])
    out.ts = int(data[2])

    return out


def smr_role( ip, port, verbose=True ):
    out = _get_role_of_server( ip, port, verbose )
    ts = out.ts
    out = Output( num_to_role( out ) )
    out.ts = ts
    return out


def num_to_role( num ):
    if num == c.ROLE_MASTER:
        return 'M'
    elif num == c.ROLE_SLAVE:
        return 'S'
    elif num == c.ROLE_LCONN:
        return 'L'
    else:
        return '?'


def check_role_consistency( server, leader_cm, logging=False ):
    cm_role = get_smr_role_of_cm( server, leader_cm )
    if cm_role == None:
        cm_role = '?'
    active_role = num_to_role( get_role_of_server( server ) )

    if cm_role == active_role:
        if logging:
            log( 'succeeded : role consistency, %s:%d %s==%s' % (server['ip'], server['smr_mgmt_port'], cm_role, active_role) )
        return True, cm_role, active_role
    else:
        if logging:
            log( 'failed : role consistency, %s:%d %s!=%s' % (server['ip'], server['smr_mgmt_port'], cm_role, active_role) )
        return False, cm_role, active_role


def get_smr_info( server, leader_cm ):
    cc = telnet.Telnet( 'cc' )
    ret = cc.connect( leader_cm['ip'], leader_cm['cm_port'] )
    if ret is not 0:
        log( 'failed to connect to cc, id:%d' % server['id'] )
        return None

    cc.write( 'pgs_info %s %d\r\n' %(server['cluster_name'], server['id']) )
    response = cc.read_until( '\r\n' )
    if response is None:
        log( 'error, response from pgs:%d is None.' % server['id'] )
        return None

    jobj = json.loads( response )
    if jobj['state'] != 'success':
        log( 'error, response of pgs_info, id:%d' % server['id'] )
        return None

    return jobj['data']


def get_quorum( master ):
    smr = smr_mgmt.SMR( 'smr' )
    ret = smr.connect( master['ip'], master['smr_mgmt_port'] )
    if ret is not 0:
        log( 'failed to connect to cc, id:%d' % master['id'] )
        return None

    smr.write( 'getquorum\r\n' )
    response = smr.read_until( '\r\n' )
    smr.disconnect()
    if response is None:
        log( 'error, response from pgs:%d is None.' % master['id'] )
        return None

    try:
        return int( response )
    except:
        return None

def cmd_to_smr( server, cmd ):
    return cmd_to_smr_addr( server['ip'], server['smr_mgmt_port'], cmd )


def cmd_to_smr_addr( ip, port, cmd, timeout=3 ):
    smr = smr_mgmt.SMR( 'smr' )
    ret = smr.connect( ip, port, timeout )
    if ret is not 0:
        log( 'failed to connect to smr, %s:%d' % (ip, port) )
        return None

    smr.write( cmd )
    response = smr.read_until( '\r\n', timeout )
    if response is None:
        log( 'error, response from smr:%s:%d is None.' % (ip, port) )
        return None

    return response


def role_lconn( server ):
    return role_lconn_addr( server['ip'], server['smr_mgmt_port'])


def role_lconn_addr( ip, port ):
    return cmd_to_smr_addr( ip, port, 'role lconn\r\n')


def strtime():
    d = datetime.datetime.now()
    format = '%Y-%m-%d_%H:%M:%S'
    return d.strftime(format)


def get_mss( cluster ):
    master = get_server_by_role( cluster['servers'], 'master' )
    if master is None:
        log( 'failed to get master' )

    slave1 = get_server_by_role( cluster['servers'], 'slave' )
    if slave1 is None:
        log( 'failed to get slave1' )

    if len(cluster['servers']) > 2:
        slave2 = None
        for server in cluster['servers']:
            id = server['id']
            if id != master['id'] and id != slave1['id']:
                slave2 = server
                break
        if slave2 is None:
            log( 'failed to get slave2' )

        return master, slave1, slave2
    else:
        return master, slave1


def get_timestamp_of_pgs( server ):
    smr = smr_mgmt.SMR( server['id'] )
    if smr.connect( server['ip'], server['smr_mgmt_port'] ) is not 0:
        log('failed to connect to server, id:%d' % server['id'])
        return -1

    smr.write( 'ping\r\n' )
    response = smr.read_until( '\r\n', 3 )
    smr.disconnect()

    if response is None:
        log('response is None, id:' % server['id'])
        return -1

    data = str.split( response[:-2], " " )
    if len( data ) < 3:
        log( 'invalied response:%s' % response )
        return -1

    return long(data[2])

# return -1 if error.
def get_clients_count_of_gw( ip, port ):
    gw = gateway_mgmt.Gateway( '0' )
    ret = gw.connect( ip, port )
    if ret != 0:
        log( 'failed : connect to %s:%d' % (ip, port) )
        return -1

    info = gw.info('gateway')
    if info == None:
        return -1

    gw.disconnect()
    return info['gateway_connected_clients']

# return -1 if error.
def get_clients_count_of_redis( ip, port ):
    cmd = 'netstat -nap | grep :%d | grep ESTABLISHED | grep java' % (port)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    log('%s\n%s' % (cmd, p.communicate()[0]))

    redis = redis_mgmt.Redis( '0' )
    ret = redis.connect( ip, port )
    if ret != 0:
        log( 'failed : connect to %s:%d' % (ip, port) )
        return -1

    redis.write( 'info clients\r\n' )
    redis.read_until( '\r\n' )
    redis.read_until( '\r\n' )
    res = redis.read_until( '\r\n' )
    if res == '':
        log( 'failed : response from %s:%d is None' % (ip, port) )
        return -1
    no = int( res.split(':')[1] )
    return no

# return -1 if error.
def get_clients_count_of_smr( port ):
    cmd = 'netstat -nap | grep :%d | grep ESTABLISHED | grep java' % (port)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    if 0 != p.wait():
        return 0
    log('%s\n%s' % (cmd, p.communicate()[0]))

    p = subprocess.Popen('%s | wc -l' % cmd, shell=True, stdout=subprocess.PIPE)
    if 0 != p.wait():
        return 0
    connection_count = int(p.communicate()[0])
    return connection_count


# return -1 if error.
def put_some_data(cluster, duration=10, max_load_generator=100):
    # start load generator
    load_gen_thrd_list = {}
    log('put some data - start')
    for i in range(max_load_generator):
        ip, port = get_rand_gateway(cluster)
        load_gen_thrd_list[i] = load_generator.LoadGenerator(i, ip, port)
        load_gen_thrd_list[i].start()
    time.sleep(duration)
    log('put some data - end')

    # check consistency of load_generator
    for i in range(len(load_gen_thrd_list)):
        load_gen_thrd_list[i].quit()
    for i in range(len(load_gen_thrd_list)):
        load_gen_thrd_list[i].join()
        if not load_gen_thrd_list[i].isConsistent():
            log('Data are inconsistent.')
            return -1
    return 0


def info(ip, port, cmd, key, sep=':'):
    try:
        con = telnetlib.Telnet(ip, port)

        con.write(cmd + '\r\n')
        reply = con.read_until('\r\n')

        size = int(reply[1:])
        readlen = 0
        while readlen <= size:
            reply = con.read_until('\r\n')
            reply.strip()
            reply = reply[:-2]

            if reply.find(key) != -1 and reply.find(sep) != -1:
                tokens = reply.split(sep)
                if tokens[0] == key:
                    log('>>> %s' % reply)
                    return tokens[1]

            readlen += len(reply)

    except:
        log(sys.exc_info()[0])
        log(sys.exc_info()[1])
        log(sys.exc_info()[2])

    return None


def rdb_bgsave_in_progress(server):
    try:
        done = info(server['ip'], server['redis_port'], 'info persistence', 'rdb_bgsave_in_progress')
        if done == '1':
            return True
        elif done == '0':
            return False
        else:
            log('error : invalid value of rdb_bgsave_in_progress "%s"' % done)
            return None
    except:
        log(sys.exc_info())
        return None


def rdb_last_save_time(server):
    try:
        epoch_time = info(server['ip'], server['redis_port'], 'info persistence', 'rdb_last_save_time')
        epoch_time = int(epoch_time)
        return epoch_time
    except:
        log(sys.exc_info())
        return None


def rdb_last_bgsave_status(server):
    try:
        status = info(server['ip'], server['redis_port'], 'info persistence', 'rdb_last_bgsave_status')
        return status
    except:
        log(sys.exc_info())
        return None


def bgsave(server, max_try=120):
    try:
        log('>>> start : bgsave. IP:%s, PORT:%d' % (server['ip'], server['redis_port']))

        # get rdb_last_save_time
        old_bgsave_time = rdb_last_save_time(server)
        if old_bgsave_time == None:
            log('>>> failed : get rdb_last_save_time')
            return False
        time.sleep(1.1)

        # request bgsave
        redis = redis_mgmt.Redis(server['id'])
        ret = redis.connect(server['ip'], server['redis_port'])
        if ret != 0:
            log('>>> failed : connect to redis%d(%s:%d)' % (server['id'], server['ip'], server['redis_port']))
            return False

        redis.write('bgsave\r\n')
        ret = redis.read_until('\r\n')
        if ret != '+Background saving started\r\n':
            log('>>> failed : request to start bgsave. res="%s"' % ret)
            return False
        log( '>>> succeeded : request to start bgsave')

        redis.write('quit\r\n')
        ret = redis.read_until('\r\n')
        if ret != '+OK\r\n':
            log('>>> failed : disconnect from redis. res="%s"' % ret)
            return False
        log( '>>> succeeded : disconnect from redis')

        # wait until the bgsave is over
        log('>>> waiting : bgsave done...  IP:%s, PORT:%d' % (server['ip'], server['redis_port']))
        in_progress = ''
        for try_cnt in range(max_try):
            in_progress = rdb_bgsave_in_progress(server)
            if in_progress == None:
                log('>>> failed : get rdb_bgsave_in_progress')
                return False

            if in_progress == False:
                log('>>> succeeded : bgsave done')
                break

            time.sleep(1)

        if in_progress == True:
            log('>>> failed : bgsave done')
            return False

        # check result
        new_bgsave_time = rdb_last_save_time(server)
        if new_bgsave_time == None:
            log('>>> failed : get rdb_last_save_time')
            return False

        bgsave_status = rdb_last_bgsave_status(server)
        if bgsave_status == None:
            log('>>> failed : get rdb_last_bgsave_status')
            return False

        if old_bgsave_time < new_bgsave_time and bgsave_status == 'ok':
            log('>>> succeeded : bgsave. rdb_last_save_time:%d, rdb_last_bgsave_status:%s' % (new_bgsave_time, bgsave_status))
            return True
        else:
            log('>>> filed : bgsave. rdb_last_save_time:%d, rdb_last_bgsave_status:%s' % (new_bgsave_time, bgsave_status))
            return False

    except:
        log(sys.exc_info())
        return False


def shutdown_pgs(server, leader_cm, check=True):
    # shutdown
    ret = testbase.request_to_shutdown_smr( server )
    if ret != 0:
        log('failed to shutdown smr%d' % server['id'])
        return False
    ret = testbase.request_to_shutdown_redis( server )
    if ret != 0:
        log('failed to shutdown redis%d' % server['id'])
        return False

    # check state F
    if check:
        max_try = 20
        expected = 'F'
        for i in range( 0, max_try):
            state = get_smr_state( server, leader_cm )
            if expected == state:
                break;
            time.sleep( 1 )
        if expected != state:
            log('server%d - state:%s, expected:%s' % (server['id'], state, expected) )
            return False

    return True

def recover_pgs(server, leader_cm, check=True):
    # recovery
    ret = testbase.request_to_start_smr( server )
    if ret != 0:
        log('failed to start smr')
        return False

    ret = testbase.request_to_start_redis( server, max_try=120 )
    if ret != 0:
        log('failed to start redis')
        return False

    ret = testbase.wait_until_finished_to_set_up_role( server, 11 )
    if ret != 0:
        log('failed to role change. smr_id:%d' % (server['id']))
        return False

    redis = redis_mgmt.Redis( server['id'] )
    ret = redis.connect( server['ip'], server['redis_port'] )
    if ret != 0:
        log('failed to connect to redis')
        return False

    # check state N
    if check:
        max_try = 20
        expected = 'N'
        for i in range( 0, max_try):
            state = get_smr_state( server, leader_cm )
            if expected == state:
                break;
            time.sleep( 1 )
        role = get_role_of_server( server )
        if expected != state:
            log('server%d - state:%s, expected:%s, role:%s' % (server['id'], state, expected, role))
            return False

    return True

def failover(server, leader_cm):
    if shutdown_pgs(server, leader_cm) == False:
        return False
    if recover_pgs(server, leader_cm) == False:
        return False
    return True


def appdata_set(cluster_name, type, id, daemon_id, period, base_time, holding_period, net_limit, output_format, service_url):
    out = Output()

    cmd = 'appdata_set %s %s %d %d %s %s %d %d %s %s' % (cluster_name, type, id, daemon_id, period, base_time, holding_period, net_limit, output_format, service_url)
    out.reply = cm_command(cmd)
    jobj = json.loads(out.reply)

    if jobj['state'] != 'success':
        out.success = False
        return out
    out.success = True
    return out


def appdata_get(cluster_name, type, id):
    out = Output()

    cmd = 'appdata_get %s %s %d' % (cluster_name, type, id)
    out.reply = cm_command(cmd)
    jobj = json.loads(out.reply)

    if jobj['state'] != 'success':
        out.success = False
        return out
    out.success = True
    return out


def roleNumberToChar( role ):
    if role == c.ROLE_MASTER:
        return 'M'
    elif role == c.ROLE_SLAVE:
        return 'S'
    else:
        return 'N'


def json_to_str(json_data):
    return json.dumps(json_data, sort_keys=True, indent=4, separators=(',', ' : '), default=handle_not_json_format_object)


def handle_not_json_format_object(o):
    return None


def get_gateway_affinity(cluster_name):
    ret = zk_cmd('get /RC/NOTIFICATION/CLUSTER/%s/AFFINITY' % cluster_name)
    znode_data = ret['znode_data']
    log('Gateway affinity znode data : %s' % znode_data)
    affinity_data = znode_data
    return affinity_data


def get_cluster_affinity_hit_ratio(servers):
    ratios = []
    for s in servers:
        ret, r = get_gateway_affinity_hit_ratio(s)
        if ret == False:
            return False, 0.0

        ratios.append(r)

    ratio = 0.0
    for r in ratios:
        ratio += r
    return True, ratio / len(ratios)


def get_gateway_affinity_hit_ratio(server):
    total_cmds = 0.0
    local_cmds = 0.0
    ratio = 0.0

    ip = server['ip']
    port = server['gateway_port']

    gw = gateway_mgmt.Gateway(server['id'])
    ret = gw.connect(ip, port)
    if ret != 0:
        log('failed : connect to %s:%d' % (ip, port))
        return False, 0.0

    try:
        info = gw.info('gateway')
        total_cmds = float(info['gateway_total_commands_processed'])
        local_cmds = float(info['gateway_total_commands_lcon'])
        if total_cmds != 0.0:
            ratio = local_cmds / total_cmds

        log('[%s:%d] total_cmds:%d, local_cmds:%d, ratio:%03f' % (ip, port, total_cmds, local_cmds, ratio))

    finally:
        gw.disconnect()

    return True, ratio


def deepcopy_server(server):
    copy_server = {}
    copy_server['id'] = server['id']
    copy_server['cluster_name'] = server['cluster_name']
    copy_server['ip'] = server['ip']
    copy_server['pm_name'] = server['pm_name']
    copy_server['cm_port'] = server['cm_port']
    copy_server['pg_id'] = server['pg_id']
    copy_server['smr_base_port'] = server['smr_base_port']
    copy_server['smr_mgmt_port'] = server['smr_mgmt_port']
    copy_server['gateway_port'] = server['gateway_port']
    copy_server['redis_port'] = server['redis_port']
    copy_server['zk_port'] = server['zk_port']
    return copy_server


def get_proc_fd_cnt( proc_uid ):
    p = proc_mgmt.get( proc_uid )
    pid = p.getPopen().pid
    return len(os.listdir('/proc/%d/fd' % pid))


def get_childproc_fd_cnt(parent_proc_uid, child_proc_name):
    p = proc_mgmt.get( parent_proc_uid )
    ppid = p.getPopen().pid

    cmd = "ps -ef | grep -v grep | grep %d | grep %s | awk -F \" \" '{print $2}'" % (ppid, child_proc_name)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    if 0 != p.wait():
        return -1
    pid = p.communicate()[0].strip()
    fds = os.listdir('/proc/%s/fd' % pid)
    log('======================================================================')
    log('file descriptors. pid:%s, fd_cnt:%d' % (str(pid), len(fds)))
    log(fds)
    return len(fds)


def get_childproc_socket_cnt(parent_proc_uid, child_proc_name, dst_ports):
    p = proc_mgmt.get( parent_proc_uid )
    ppid = p.getPopen().pid

    log(' ### SOCKET INFO BEGIN. PPID:%s, CHILD_PROC_NAME:%s ###' % (str(ppid), child_proc_name))

    cmd = "ps --ppid %d -f | grep -v awk | awk '%s' | awk -F \" \" '{print $2}'" % (ppid, child_proc_name)
    log(' get pid. cmd: "%s"' % cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    if 0 != p.wait():
        log('shell command fail. cmd: "%s"' % cmd)
        return -1
    pid = int(p.communicate()[0].strip())
    log(' pid: %d' % pid)

    socket_infos = []
    for port in dst_ports:
        cmd = 'netstat -nap | grep -v grep | grep " %d/java" | grep ESTABLISHED' % pid
        log(' netstat. cmd: "%s"' % cmd)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        for socket_info in p.communicate()[0].strip().split(os.linesep):
            log(socket_info)
            if len(socket_info) > 0:
                if int(socket_info.split()[4].split(":")[1]) == port:
                    socket_infos.append(socket_info)

    log(' ### SOCKET INFO END. PID:%d, SOCKET_CNT:%d ###' % (pid, len(socket_infos)))
    for socket_info in socket_infos:
        log(socket_info)
    return len(socket_infos)


def command(ip, port, req):
    reply = ""
    conn = None

    try:
        conn = telnetlib.Telnet( ip, port, 3)
        conn.write(req + '\r\n')
        reply = conn.read_until('\r\n', 3)
        if reply.find('-ERR No available server for executing info command') != -1:
            log('-ERR No available server for executing info command')
            return -1
        elif reply == '':
            log('[%s:%d] Gateway reply is Null.' % (ip, port))
            return -1

        size = int(reply[1:])

        readlen = 0
        reply_value = None
        while readlen <= size:
            reply += conn.read_until('\r\n', 3)
            readlen += len(reply)

    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)

    finally:
        conn.close()

    return reply


def gw_info_redis_disccons(ip, port):
    gw = gateway_mgmt.Gateway('0')
    if gw.connect(ip, port) != 0:
        return -1

    info = gw.info('gateway')
    if info == None:
        return -1

    gw.disconnect()
    return info['gateway_disconnected_redis']


def gw_info_client_cnt(ip, port):
    gw = gateway_mgmt.Gateway('0')
    if gw.connect(ip, port) != 0:
        return -1

    info = gw.info('gateway')
    if info == None:
        return -1

    gw.disconnect()
    return info['gateway_connected_clients']


def upgrade_gw(server, leader_cm):
    ip = server['ip']
    port = server['gateway_port']
    id = server['id']
    cluster_name = server['cluster_name']

    # delete gw info from confmaster and shutdown gw
    if testbase.request_to_shutdown_gateway(cluster_name, server, leader_cm) != 0:
        log('[UPGRADE_GW] Failed to shutdown gateway %d (%s:%d)' % (id, ip, port))
        return False

    # restart gw and add gw info to confmaster
    if testbase.request_to_start_gateway(cluster_name, server, leader_cm) != 0:
        log('[UPGRADE_GW] Failed to start gateway %d (%s:%d)' % (id, ip, port))
        return False

    return True


def upgrade_pgs(upgrade_server, leader_cm, cluster):
    key_prefix = 'upgrade_pgs'

    # get role
    role = smr_role(upgrade_server['ip'], upgrade_server['smr_mgmt_port'])
    if role == 'M':
        type = 'master'
    elif role == 'S':
        type = 'slave'
    else:
        log('failed : invalid role. smr:%d %s:%d, role:%s' % (upgrade_server['id'], upgrade_server['ip'], upgrade_server['smr_base_port'], role))
        return False

    log('[start] upgrade pgs%d. type:%s' % (upgrade_server['id'], type))
    log_server_state(cluster)

    # start load generator
    load_gen_list = {}
    for i in range(len(cluster['servers'])):
        server = cluster['servers'][i]
        load_gen = load_generator.LoadGenerator(server['id'], server['ip'], server['gateway_port'])
        load_gen.start()
        load_gen_list[i] = load_gen

    # backup data
    bgsave_ret = bgsave(upgrade_server)
    if bgsave_ret == False:
        log('failed : bgsave. pgs%d' % upgrade_server['id'])
        return False
    log('succeeded : bgsave. pgs%d' % upgrade_server['id'])

    # change master
    if type == 'master':
        master = role_change(leader_cm, cluster['cluster_name'], (upgrade_server['id'] + 1) % len(cluster['servers']))
        if master == -1:
            log('failed : role_change.')
            return False
        log('succeeded : role_change, new master:%d' % master)

        for s in cluster['servers']:
            ret, cm_role, active_role = check_role_consistency(s, leader_cm)
            if ret == False:
                log('failed : role consistency, %s:%d %s!=%s' % (s['ip'], s['smr_mgmt_port'], cm_role, active_role))
                return False
            log('succeeded : role consistency, %s:%d %s==%s' % (s['ip'], s['smr_mgmt_port'], cm_role, active_role))

    # detach pgs from cluster
    cmd = 'pgs_leave %s %d forced\r\n' % (upgrade_server['cluster_name'], upgrade_server['id'])
    ret = cm_command(leader_cm['ip'], leader_cm['cm_port'], cmd)
    jobj = json.loads(ret)
    if jobj['msg'] != '+OK':
        log('failed : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]))
        return False
    log('succeeded : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]))

    # check if pgs is removed
    success = False
    for try_cnt in range(10):
        redis = redis_mgmt.Redis(upgrade_server['id'])
        ret = redis.connect(upgrade_server['ip'], upgrade_server['redis_port'])
        if ret != 0:
            log('failed : connect to redis%d(%s:%d)' % (upgrade_server['id'], upgrade_server['ip'], upgrade_server['redis_port']))
            return False
        log('succeeded : connect to redis%d(%s:%d)' % (upgrade_server['id'], upgrade_server['ip'], upgrade_server['redis_port']))

        info = redis.info('all')
        if info == None:
            log('failed : get reply of "info stats" from redis%d(%s:%d)' % (upgrade_server['id'], upgrade_server['ip'], upgrade_server['redis_port']))
            return False

        log('redis%d`s instantaneous_replied_ops_per_sec : %d' % (upgrade_server['id'], info['instantaneous_replied_ops_per_sec']))
        if info['instantaneous_replied_ops_per_sec'] <= 20:
            success = True
            break
        time.sleep(1)

    if success == False:
        log('failed : pgs does not removed.')
        return False
    log('succeeded : pgs is removed')

    # change state of pgs to lconn
    cmd = 'pgs_lconn %s %d\r\n' % (upgrade_server['cluster_name'], upgrade_server['id'])
    ret = cm_command(leader_cm['ip'], leader_cm['cm_port'], cmd)
    jobj = json.loads(ret)
    if jobj['msg'] != '+OK':
        log('failed : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]))
        return False
    log('succeeded : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]))

    # shutdown
    ret = testbase.request_to_shutdown_smr(upgrade_server)
    if ret != 0:
        log('failed : shutdown smr. id:%d' % upgrade_server['id'])
        return False

    ret = testbase.request_to_shutdown_redis(upgrade_server)
    if ret != 0:
        log('failed : shutdown redis. id:%d' % upgrade_server['id'])
        return False
    log('succeeded : shutdown pgs%d.' % upgrade_server['id'])

    if load_gen.isConsistent() != True:
        log('failed : data consistency does not satisfied.')
    log('succeeded : data consistency is satisfied.')

    # set new values
    ip, port = get_rand_gateway(cluster)
    gw = gateway_mgmt.Gateway('0')
    gw.connect(ip, port)
    for i in range(0, 10000):
        cmd = 'set %s%d %d\r\n' % (key_prefix, i, i)
        gw.write(cmd)
        res = gw.read_until('\r\n')
        if res != '+OK\r\n':
            log('failed : set values to gw(%s:%d). cmd:%s, res:%s' % (ip, port, cmd[:-2], res[:-2]))
            return False
    log('succeeded : write initial values')

    # attach pgs to cluster
    cmd = 'pgs_join %s %d\r\n' % (upgrade_server['cluster_name'], upgrade_server['id'])
    ret = cm_command(leader_cm['ip'], leader_cm['cm_port'], cmd)
    jobj = json.loads(ret)
    if jobj['msg'] != '+OK':
        log('failed : cmd="%s", reply="%s"' % (cmd[:-2], ret))
        return False
    log('succeeded : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]))

    # recovery
    log('restart pgs%d.' % upgrade_server['id'])
    ret = testbase.request_to_start_smr(upgrade_server, verbose=2)
    if ret != 0:
        log('failed : start smr. id:%d' % upgrade_server['id'])
        return False
    log('succeeded : to start smr. id:%d' % upgrade_server['id'])

    ret = testbase.request_to_start_redis(upgrade_server, False)
    if ret != 0:
        log('failed : start redis. id:%d' % upgrade_server['id'])
        return False
    log('succeeded : start redis. id:%d' % upgrade_server['id'])

    wait_count = 20
    ret = testbase.wait_until_finished_to_set_up_role(upgrade_server, wait_count)
    if ret != 0:
        log('failed : role change. smr_id:%d' % (upgrade_server['id']))
        return False
    ret = check_if_redis_is_running_properly(upgrade_server['redis_port'])
    if ret != 0:
        log('failed : connect to redis')
        return False
    log('succeeded : start pgs')

    # check state N
    max_try = 20
    expected = 'N'
    for i in range(0, max_try):
        state = get_smr_state(upgrade_server, leader_cm)
        if expected == state:
            break;
        time.sleep(1)
    if expected != state:
        log('server%d - state:%s, expected:%s' % (upgrade_server['id'], state, expected))
        return False
    log('succeeded : pgs%d state changed to N.' % upgrade_server['id'])

    # check if pgs is added
    success = False
    for try_cnt in range(10):
        redis = redis_mgmt.Redis(upgrade_server['id'])
        ret = redis.connect(upgrade_server['ip'], upgrade_server['redis_port'])
        if ret != 0:
            log('failed : connect to smr%d(%s:%d)' % (upgrade_server['id'], upgrade_server['ip'], upgrade_server['redis_port']))
            return False
        log('succeeded : connect to smr%d(%s:%d)' % (upgrade_server['id'], upgrade_server['ip'], upgrade_server['redis_port']))

        info = redis.info('all')
        if info == None:
            log('failed : get reply of "info stats" from redis%d(%s:%d)' % (upgrade_server['id'], upgrade_server['ip'], upgrade_server['redis_port']))
            return False

        log('redis%d`s instantaneous_replied_ops_per_sec : %d' % (upgrade_server['id'], info['instantaneous_replied_ops_per_sec']))
        if info['instantaneous_replied_ops_per_sec'] > 20:
            success = True
            break
        time.sleep(1)

    if success == False:
        log('failed : pgs does not added.')
        return False
    log('succeeded : pgs is added')

    # check role consistency
    for s in cluster['servers']:
        ret, cm_role, active_role = check_role_consistency(s, leader_cm)
        if ret == False:
            log('failed : role consistency, %s:%d %s!=%s' % (s['ip'], s['smr_mgmt_port'], cm_role, active_role))
            return False
    log('succeeded : role consistency, %s:%d %s==%s' % (s['ip'], s['smr_mgmt_port'], cm_role, active_role))

    # check new values
    redis = redis_mgmt.Redis(upgrade_server['id'])
    ret = redis.connect(upgrade_server['ip'], upgrade_server['redis_port'])
    if ret != 0:
        log('failed : connect to smr%d(%s:%d)' % (upgrade_server['id'], upgrade_server['ip'], upgrade_server['redis_port']))
        return False
    log('succeeded : connect to smr%d(%s:%d)' % (upgrade_server['id'], upgrade_server['ip'], upgrade_server['redis_port']))

    for i in range(0, 10000):
        cmd = 'get %s%d\r\n' % (key_prefix, i)
        redis.write(cmd)
        redis.read_until('\r\n')
        res = redis.read_until('\r\n')
        if res  != '%d\r\n' % i:
            log('failed to get values from redis%d. %s != %d' % (upgrade_server['id'], res, i))
            return False
    log('succeeded : check values with get operations on pgs%d.' % (upgrade_server['id']))

    # shutdown load generators
    for i in range(len(load_gen_list)):
        load_gen_list[i].quit()
        load_gen_list[i].join()

    log_server_state(cluster)

    return True


"""
Get the number of file descriptors of a confmaster in a cluster.
"""
def cm_get_fd_cnt(s):
    uid = cc_uid(s['id'])
    return get_childproc_fd_cnt(uid, 'confmaster')


def cm_get_socket_cnt(s, dst_ports):
    uid = cc_uid(s['id'])
    return get_childproc_socket_cnt(uid, '/java/*/jar/*/confmaster/*/jar/', dst_ports)


"""
Return (start number of worklog, end number of worklog)
"""
def worklog_info( leader_cm ):
    reply = cm_command( leader_cm['ip'], leader_cm['cm_port'], 'worklog_info')
    # worklog_info
    # {"state":"success","data":{"start":<int>,"end":<int>}}
    jobj = json.loads(reply)

    sno = jobj['data']['start']
    eno = jobj['data']['end']

    log('worklog_info: s(%d), e(%d)' % (sno, eno))

    return sno, eno


"""
Returns true if successful or false otherwise
"""
def is_leader_cm(cc):
    return _is_leader_cm(cc['ip'], cc['cm_port'])

def _is_leader_cm(ip, port):
    reply = cm_command(ip, port, 'cluster_ls')
    jobj = json.loads(reply)

    if jobj['state'] == 'success':
        return True
    else:
        return False

def copy_smrreplicator( id ):
    if not os.path.exists( smr_dir( id ) ):
        os.mkdir( smr_dir( id ) )

    src = '../smr/replicator/%s' % (c.SMR)
    shutil.copy(src, smr_dir( id ))

def copy_gw( id ):
    if not os.path.exists( gw_dir( id ) ):
        os.mkdir( gw_dir( id ) )

    src = '../gateway/%s' % c.GW
    shutil.copy(src, gw_dir( id ))

def copy_redis_server( id ):
    if not os.path.exists( redis_dir( id ) ):
        os.mkdir( redis_dir( id ) )

    src = '../redis-2.8.8/src/%s' % (c.REDIS)
    shutil.copy(src, redis_dir( id ))

def copy_cluster_util( id ):
    if not os.path.exists( cluster_util_dir( id ) ):
        os.mkdir( cluster_util_dir( id ) )

    src = '../redis-2.8.8/src/%s' % (c.CLUSTER_UTIL)
    shutil.copy(src, cluster_util_dir( id ))

def copy_dump_util( id ):
    if not os.path.exists( dump_util_dir( id ) ):
        os.mkdir( dump_util_dir( id ) )

    src = '../redis-2.8.8/src/%s' % (c.DUMP_UTIL)
    shutil.copy(src, dump_util_dir( id ))

def copy_log_util( id ):
    if not os.path.exists( log_util_dir( id ) ):
        os.mkdir( log_util_dir( id ) )

    src = '../smr/smr/%s' % (c.LOG_UTIL)
    shutil.copy(src, log_util_dir( id ))

def copy_dump_util_plugin( id ):
    if not os.path.exists( dump_util_dir( id ) ):
        os.mkdir( dump_util_dir( id ) )

    src = '../redis-2.8.8/src/%s' % (c.DUMP_UTIL_PLUGIN)
    shutil.copy(src, dump_util_dir( id ))

def copy_capi_so_file( id ):
    if not os.path.exists( capi_dir( id ) ):
        os.mkdir( capi_dir( id ) )

    src = '../api/arcci/.obj64/lib/%s' % (c.CAPI_SO_FILE)
    shutil.copy(src, capi_dir( id ))

def copy_capi32_so_file( id ):
    if not os.path.exists( capi_dir( id ) ):
        os.mkdir( capi_dir( id ) )

    src = '../api/arcci/.obj32/lib/%s' % (c.CAPI_SO_FILE)
    shutil.copy(src, capi_dir( id ))

def copy_capi_test_server( id ):
    if not os.path.exists( capi_dir( id ) ):
        os.mkdir( capi_dir( id ) )

    src = '../tools/local_proxy/.obj64/%s' % (c.CAPI_TEST_SERVER)
    shutil.copy(src, capi_dir( id ))

def copy_capi32_test_server( id ):
    if not os.path.exists( capi_dir( id ) ):
        os.mkdir( capi_dir( id ) )

    src = '../tools/local_proxy/.obj32/%s' % (c.CAPI_TEST_SERVER)
    shutil.copy(src, '%s/%s' % (capi_dir( id ), c.CAPI32_TEST_SERVER))

def copy_cm( id, zk_addr="127.0.0.1:2181" ):
    if not os.path.exists( cc_dir( id ) ):
        os.mkdir( cc_dir( id ) )

    src = '../confmaster/target/%s' % (c.CC)
    shutil.copy(src, '%s/%s' % (cc_dir( id ), c.CC))

    src = '../confmaster/target/%s' % (c.CM_PROPERTY_FILE_NAME)
    property_file = '%s/%s' % (cc_dir( id ), c.CM_PROPERTY_FILE_NAME)
    shutil.copy(src, property_file)
    os.system( 'sed -i "s/confmaster.zookeeper.address=.*/confmaster.zookeeper.address=%s/g" %s' % 
            (zk_addr, property_file) )

    src = '../confmaster/script/%s' % (c.CM_EXEC_SCRIPT)
    dst = '%s/%s' % (cc_dir( id ), c.CM_EXEC_SCRIPT)
    shutil.copy(src, dst)
    os.chmod( dst, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )

def start_confmaster( id, port, context ):
    f_properties = open( '%s/%s' % (cc_dir( id ), c.CM_PROPERTY_FILE_NAME), 'r' )
    contents = f_properties.read()
    contents = string.replace( contents,
                               'confmaster.port=%d' % c.CM_DEFAULT_PORT,
                               'confmaster.port=%d' % port)
    f_properties.close()

    f_properties = open( '%s/%s' % (cc_dir( id ), c.CM_PROPERTY_FILE_NAME), 'w' )
    f_properties.write( contents )
    f_properties.close()

    f_log_std = open_process_logfile( id, 'cc_std' )
    f_log_err = open_process_logfile( id, 'cc_err' )
    p = exec_proc_async( cc_dir( id ), './%s %s' % (c.CM_EXEC_SCRIPT, context), True, None, f_log_std, f_log_err )
    proc_mgmt.insert( cc_uid( id ), p )

    time.sleep(2)
    # check if the process is running properly
    error = True
    for try_cnt in range( 0, 20 ):
        try:
            t = telnetlib.Telnet( 'localhost', port )
            t.write( 'ping\r\n' )
            response = t.read_until( '\r\n' )
            t.close()
            if response.find('+PONG') is not -1:
                error = False
                break

        except socket.error:
            log('wait for connection established to confmaster, port=%d' % port)

        time.sleep(1)

    if error:
        log('confmaster is not running properly. port=%d' % port)
        kill_proc( p )
        return -1
    else:
        if _is_leader_cm('localhost', port):
            if start_cm('localhost', port) == False:
                kill_proc( p )
                return -1
        return 0

def start_cm(ip, port):
    try:
        response = cm_command(ip, port, 'cm_start')

        try:
            json_fmt = json.loads(response.strip())
            if json_fmt["state"] == "success":
                return True
        except ValueError as e:
            log('json decode error. "%s", response: "%s"' % (str(e), response))

    except socket.error:
        log('wait for connection established to confmaster, port=%d' % port)

    return False

def cluster_on(ip, port, cluster_name):
    return cm_success(cm_command(ip, port, 'cluster_on %s' % cluster_name))[0]

def cluster_off(ip, port, cluster_name):
    return cm_success(cm_command(ip, port, 'cluster_off %s' % cluster_name))[0]

def cluster_dump(ip, port, cluster_name):
    return cm_success(cm_command(ip, port, 'cluster_dump %s' % cluster_name))

def cluster_load(ip, port, dump):
    return cm_success(
            cm_command(
                ip, port, 'cluster_load %s' % json.dumps(dump).replace(' ', ''))
           )[0]

def cluster_purge(ip, port, cluster_name):
    return cm_success(cm_command(ip, port, 'cluster_purge %s' % cluster_name))[0]

def pgs_leave(ip, port, cluster_name, pgs_id):
    return cm_success(cm_command(ip, port, 'pgs_leave %s %d' % (cluster_name, pgs_id)))[0]

def pgs_join(ip, port, cluster_name, pgs_id):
    return cm_success(cm_command(ip, port, 'pgs_join %s %d' % (cluster_name, pgs_id)))[0]

def do_logutil_cmd (id, subcmd):
    parent_dir, log_dir = smr_log_dir( id )
    cmd = './%s %s %s' % (c.LOG_UTIL, subcmd, log_dir)
    p = exec_proc_async(log_util_dir(id), cmd, True, None, subprocess.PIPE, None)
    ret = p.wait()
    if ret <> 0:
        log('failed to make memory log.')
        log('  cmd:%s' % cmd)
        log('  return code:%d' % p.returncode)
        log(p.stdout.readlines())
        return -1
    else:
        return 0

def start_smr_replicator( id, ip, base_port, mgmt_port, verbose, check, log_delete_delay ):
    # setting log directory
    parent_dir, log_dir = smr_log_dir( id )
    full_path = '%s/%s' % (parent_dir, log_dir)
    if not os.path.exists( full_path ):
        cmd = 'mkdir %s' % full_path
        ret = shell_cmd_sync( cmd, 0 )
        if ret <> 0:
            log('failed to make log directory.')
            return -1
        # make in memory volume
        if config.opt_use_memlog:
            do_logutil_cmd(id, 'createlog')

    cmd = './%s -d %s -b %d -v %d -x %d' % (c.SMR, log_dir, base_port, verbose, log_delete_delay)
    log(cmd)
    f_log_std = open_process_logfile( id, 'smr_std' )
    f_log_err = open_process_logfile( id, 'smr_err' )
    p = exec_proc_async( smr_dir( id ), cmd, True, None, f_log_std, f_log_err )

    uid = smr_uid( id )
    proc_mgmt.insert( uid, p )

    # check if the process is running properly
    if check:
        if check_if_smr_is_running_properly( ip, mgmt_port ) is not 0:
            proc_mgmt.kill( uid )
            return -1

    return 0


def start_redis_server( id, smr_port, redis_port, check=True, max_try=30 ):
    cmd = './%s --smr-local-port %d --port %d --save ""' % (c.REDIS, smr_port, redis_port)
    f_log_std = open_process_logfile( id, 'redis_std' )
    f_log_err = open_process_logfile( id, 'redis_err' )
    p = exec_proc_async(   redis_dir( id )
                              , cmd
                              , True
                              , None
                              , f_log_std
                              , f_log_err )

    uid = redis_uid( id )
    proc_mgmt.insert( uid, p )

    # check if the process is running properly
    if check:
        if check_if_redis_is_running_properly( redis_port, 1, max_try ) is not 0:
            proc_mgmt.kill( uid )
            return -1
    return 0


def start_gateway( id, ip, cm_port, cluster_name, gw_port, check=True ):
    cmd = './%s -c %s -b %d -n %s -p %d' % (c.GW, ip, cm_port, cluster_name, gw_port)
    log(cmd)
    f_log_std = open_process_logfile( id, 'gw_std' )
    f_log_err = open_process_logfile( id, 'gw_err' )
    p = exec_proc_async( gw_dir( id ), cmd, True, None, f_log_std, f_log_err )

    uid = gateway_uid( id )
    proc_mgmt.insert( uid, p )

    # check if the process is running properly
    if check:
        if check_if_gateway_is_running_properly( ip, gw_port ) is not 0:
            proc_mgmt.kill( uid )
            return -1
    return 0


def start_proc( working_dir, cmd ):
    f_properties.close()

    f_properties = open( '%s/%s' % (cc_dir( id ), c.CM_PROPERTY_FILE_NAME), 'w' )
    f_properties.write( contents )
    f_properties.close()

    f_log_std = open_process_logfile( id, 'cc_std' )
    f_log_err = open_process_logfile( id, 'cc_err' )
    p = exec_proc_async( cc_dir( id ), './%s' % c.CM_EXEC_SCRIPT, True, None, f_log_std, f_log_err )
    proc_mgmt.insert( cc_uid( id ), p )

    time.sleep(2)
    # check if the process is running properly
    error = True
    for try_cnt in range( 0, 10 ):
        try:
            t = telnetlib.Telnet( 'localhost', port )
            t.write( 'ping\r\n' )
            response = t.read_until( '\r\n' )
            t.close()
            if response.find('+PONG') is not -1:
                error = False
                break

        except socket.error:
            print 'failed to connect to confmaster, port=%d' % port

        time.sleep(1)

    if error:
        log('confmaster is not running properly.')
        kill_proc( p )
        return -1

    return 0


def killps_y( name ):
    _killps_y( name )
    log('kill processes "%s"' % name)
    shell_cmd_sync( 'ps -ef | grep smr', 0 )
    shell_cmd_sync( 'ps -ef | grep confmaster', 0 )
    return 0


def kill_smr( id ):
    uid = smr_uid( id )
    proc_mgmt.kill( uid )
    return 0


def kill_redis( id ):
    uid = redis_uid( id )
    proc_mgmt.kill( uid )
    return 0


def kill_all_processes():
    proc_mgmt.kill_all()
    return 0


def zk_cmd( cmd ):
    args = 'zkCli.sh -server localhost:2181 %s' % (cmd)
    p = exec_proc_async( './', args, True, out_handle=subprocess.PIPE, err_handle=subprocess.PIPE )
    p.wait()
    (std, err) = p.communicate()
    # zkCli.sh stdout : znode data
    #          stderr : output messages(not error) and error messages
    if err != '':
        log( 'failed to execute zkCli.sh. command:%s, err:%s' % (cmd, err) )

    lines = std.split('\n')
    znode_data = ''
    i = 0
    while i < len(lines):
        if lines[i] == 'WatchedEvent state:SyncConnected type:None path:null':
            j = i+1
            while j < len(lines)-1:
                if znode_data != '':
                    znode_data += '\n'
                znode_data += lines[j]
                j += 1
            break
        i += 1

    return {'err':err, 'znode_data':znode_data}


def delete_redis_check_point( id ):
    full_path = '%s/%s' % (redis_dir( id ), c.REDIS_CHECK_POINT_FILE_NAME)
    cmd = 'rm -f %s' % full_path
    ret = shell_cmd_sync( cmd, 0 )
    if ret <> 0:
        log('failed to delete redis check point')
        return -1
    return 0


def delete_smr_log_dir( id, base_port ):
    if config.opt_use_memlog:
        do_logutil_cmd(id, 'deletelog')

    parent_dir, log_dir = smr_log_dir( id )
    full_path = '%s/%s' % (parent_dir, log_dir)
    cmd = 'rm -rf %s' % full_path
    ret = shell_cmd_sync( cmd, 0 )
    if ret <> 0:
        log('failed to delete the log directory.')
        return -1
    return 0


def delete_smr_logs( id ):
    if config.opt_use_memlog:
        do_logutil_cmd(id, 'deletelog')

    parent_dir, log_dir = smr_log_dir( id )
    full_path = '%s/%s' % (parent_dir, log_dir)
    cmd = 'rm -rf %s/*' % full_path
    ret = shell_cmd_sync( cmd, 0 )
    if ret <> 0:
        log('failed to delete the logs.')
        return -1
    return 0


def shutdown_smr( id, ip, base_port ):
    uid = smr_uid( id )
    proc = proc_mgmt.get( uid )
    if proc is None:
        log('process(%s) does not exist.')
        return -1

    proc_mgmt.kill( uid )
    time.sleep( 1 )

    cmd = "kill `ps aux | grep smr-replicator | grep '\-b %d' | grep -v grep | awk '{print $2}'`" % base_port
    shell_cmd_sync(cmd, 0)
    log(cmd)

    time.sleep( 1 )
    return 0


def shutdown_redis( id, redis_port ):
    uid = redis_uid( id )
    proc = proc_mgmt.get( uid )
    if proc is None:
        return -1

    proc_mgmt.kill( uid )
    time.sleep( 1 )

    cmd = "kill `ps axu | grep redis-arc | grep '\-\-port %d' | grep -v grep | awk '{print $2}'`" % redis_port
    shell_cmd_sync(cmd, 0)

    time.sleep( 1 )
    return 0

def shutdown_hbc( id ):
    uid = hbc_uid( id )
    proc = proc_mgmt.get( uid )
    if proc is None:
        return -1

    proc_mgmt.kill( uid )
    time.sleep( 1 )
    return 0


def shutdown_cm( id ):
    uid = cc_uid( id )
    proc = proc_mgmt.get( uid )
    if proc is None:
        return -1

    proc_mgmt.kill( uid )
    time.sleep( 1 )
    return 0


def shutdown_gateway( id, port, force=False ):
    uid = gateway_uid( id )
    proc = proc_mgmt.get( uid )
    if proc is None:
        if not force:
            return -1
    else:
        proc_mgmt.kill( uid )
        time.sleep( 1 )

    cmd = "kill `ps axu | grep redis-gateway | grep '\-p %d' | grep -v grep | awk '{print $2}'`" % port
    shell_cmd_sync(cmd, 0)

    time.sleep( 1 )
    return 0


def backup_log( backup_log_dir ):
    try:
        shutil.copytree(c.homedir, backup_log_dir)
    except exceptions.OSError as e:
        if e.errno == 17:
            pass
        else:
            raise e
    return 0


def del_dumprdb( id ):
    dir = redis_dir( id )
    path = '%s/dump.rdb' % dir
    ret = os.path.exists( path )
    if ret:
        print 'delete %s' % path
        os.remove( path )
    return 0


def check_if_dumprdb_exists( id ):
    dir = redis_dir( id )
    path = '%s/dump.rdb' % dir
    print 'check_if_dumprdb_exists %s' % path
    ret = os.path.exists( path )
    return ret


def start_zookeeper( zk_dir ):
    old_cwd = os.path.abspath( os.getcwd() )
    try:
        zk_dir = zk_dir.replace('$HOME', os.getenv('HOME'))
        os.chdir( zk_dir )
        p = subprocess.Popen(
                ["./zkServer.sh", "start"], stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
        stdout, stderr = p.communicate()
        return stdout, p.returncode
    finally:
        os.chdir( old_cwd )
    return None, None


def stop_zookeeper( zk_dir ):
    old_cwd = os.path.abspath( os.getcwd() )
    try:
        zk_dir = zk_dir.replace('$HOME', os.getenv('HOME'))
        os.chdir( zk_dir )
        p = subprocess.Popen(
                ["./zkServer.sh", "stop"], stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
        stdout, stderr = p.communicate()
        return stdout, p.returncode
    finally:
        os.chdir( old_cwd )
    return None, None


"""
con_static: 
    {
    server_id : {
        "redis" : N,
        "smr" : N,
        "gw" : N
    }, ...
}

op:
    A function implemented in C corresponding to the intrinsic operators of Python in the operator module. 
    Such as operator.lt, operator.eq, operator.gt, ...

exclude: 
    ["gw", "redis", "smr"]
"""
def con_compare(con_static, op, exclude=[]):
    def con_compare_f(con_varying):
        print "compare: ", con_static, " ", str(op), " ", con_varying
        all_keys = ["gw", "redis", "smr"]
        ret = True
        for key in set(all_keys) - set(exclude):
            if not op(con_varying[key], con_static[key]):
                ret = False
        return ret
    return con_compare_f


def await(timeout_sec, expect=True):
    def await_f(condition, generator):
        end = time.time() + timeout_sec
        while time.time() < end:
            d = generator()
            if condition(d) == expect: return expect
            time.sleep(1)
        print('last generated data: ', d)
        return not expect
    return await_f 


def connection_count(server):
    redis = get_clients_count_of_redis(server['ip'], server['redis_port'])
    smr = get_clients_count_of_smr(server['smr_mgmt_port'])
    gw = get_clients_count_of_gw(server['ip'], server['gateway_port'])
    return {
            "redis" : redis,
            "smr" : smr,
            "gw" : gw
            }


def connection_count_closure(server):
    def connection_count_f():
        return connection_count(server)
    return connection_count_f


def ls( dir ):
    try:
        return os.listdir( dir )
    except OSError as e:
        log('failed to os.listdir.', e)
        return []


def cc_uid( id ):
    return 'cc%d' % (id)


def hbc_uid( id ):
    return 'hbc%d' % (id)


def smr_uid( id ):
    return 'smr%d' % (id)


def redis_uid( id ):
    return 'redis%d' % (id)


def gateway_uid( id ):
    return 'gateway%d' % (id)


def batch_daemon_uid( id ):
    return 'batch_daemon%d' % (id)


def smr_dir( id ):
    return '%s%d' % (c.SMR_DIR, id)


def smr_log_dir( id ):
    return smr_dir( id ), 'log%d' % (id)


def gw_dir( id ):
    return '%s%d' % (c.GW_DIR, id)


def redis_dir( id ):
    return '%s%d' % (c.REDIS_DIR, id)


def cluster_util_dir( id ):
    return '%s%d' % (c.CLUSTER_UTIL_DIR, id)


def dump_util_dir( id ):
    return '%s%d' % (c.DUMP_UTIL_DIR, id)

def log_util_dir( id ):
    return '%s%d' % (c.LOG_UTIL_DIR, id)

def capi_dir( id ):
    return '%s%d' % (c.CAPI_DIR, id)


def cc_dir( id ):
    return '%s%d' % (c.CC_DIR, id)


def hbc_dir( id ):
    return '%s%d' % (c.HBC_DIR, id)


def batch_daemon_dir( id ):
    return '%s%d' % (c.BATCH_DAEMON_DIR, id)

def remove_all_memory_logs():
    for dirpath, dirs, files in os.walk(c.homedir):
        for file in files:
            if file == "master-log":
                cmd = '../smr/smr/%s deletelog %s' % (c.LOG_UTIL, dirpath)
                os.system(cmd)

def rmr(path):
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(path)

