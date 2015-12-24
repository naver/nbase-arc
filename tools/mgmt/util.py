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

import datetime
import threading
import telnetlib
import logging
import time
import socket
import random
import tempfile
import traceback
import sys
import os
import json
from fabric.api import *
from fabric.colors import *
from fabric.contrib.console import *
from gw_cmd import *
from redis_cmd import *
import remote
import cm

config = None

def set_config(config_module):
    global config
    config = config_module

class Output:
    def __init__(self):
        pass

def strtime():
        d = datetime.datetime.now()
        format = '%Y%m%d_%H%M%S'
        return d.strftime(format)

class LogFormatter(logging.Formatter):
    converter = datetime.datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s.%03d" % (t, record.msecs)
        return s

class Socket:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def connect(self, host, port):
        self.sock.connect((host, port))

    def send(self, msg, msglen):
        totalsent = 0
        while totalsent < msglen:
            sent = self.sock.send(msg[totalsent:])
            if sent == 0:
                raise RuntimeError("socket connection broken")
            totalsent = totalsent + sent

    def recv(self, msglen):
        msg = ''
        while len(msg) < msglen:
            chunk = self.sock.recv(msglen-len(msg))
            if chunk == '':
                raise RuntimeError("socket connection broken")
            msg = msg + chunk
        return msg

CHECK_MACHINE_LOG_COLUMN = '|%26s |%10s | %s'
class PingpongWorker(threading.Thread):
    def __init__(self, ip, port, ping_msg, logger, interval=0.1, slow_response_time=10, try_cnt=None):
        threading.Thread.__init__(self)
        self.exit = False 
        self.max_response_time = 0
        self.last_reply = ''

        self.ip = ip
        self.port = port
        self.ping_msg = ping_msg
        self.logger = logger
        self.interval = interval
        self.slow_response_time = slow_response_time
        self.try_cnt = try_cnt
 
        try:
            self.sock = Socket()
            self.sock.connect(self.ip, self.port) 
        except socket.error as e:
            warn(red(e))
            warn(red('. IP:%s, REDIS_PORT:%d' % (self.ip, self.port)))
            raise

    def quit(self):
        self.exit = True
        return

    def ping(self):
        start_time = int(time.time() * 1000)
        send_data = self.ping_msg + '\r\n'
        self.sock.send(send_data, len(send_data))
        sent_time = int(time.time() * 1000)

        expected_to_recv = '+PONG\r\n'
        reply = self.sock.recv(len(expected_to_recv))
        end_time = int(time.time() * 1000)

        if reply.find('\r\n') != -1:
            reply = reply[:-2]

        self.handleResult('%s:%d' % (self.ip, self.port), reply, start_time, sent_time, end_time)

        return reply

    def handleResult(self, addr, reply, start_time, send_comp_time, end_time):
        response_time = end_time - start_time

        # Check response time
        log = None
        if self.max_response_time < response_time:
            log = 'MAX RESP'
            self.max_response_time = response_time
        elif response_time >= self.slow_response_time:
            log = 'SLOW RESP'

        if log != None and self.logger != None:
            self.logger.critical(CHECK_MACHINE_LOG_COLUMN % (
                '%s:%d' % (self.ip, self.port), log, 
                'PING="%s"\tREPLY="%s"\tRESPONSE_TIME=%d' % (self.ping_msg, reply, response_time)))

    def run(self):
        cnt = 0
        while self.exit == False:
            if self.try_cnt != None:
                if cnt >= self.try_cnt:
                    break
                cnt += 1

            self.last_reply = self.ping()
            time.sleep(self.interval)

    def get_last_reply(self):
        return self.last_reply

    def get_ip(self):
        return self.ip 

    def get_port(self):
        return self.port

class IostatWorker(threading.Thread):
    def __init__(self, host, logger, io_util_boundary=3.0):
        threading.Thread.__init__(self)
        self.exit = False 

        self.host = host
        self.logger = logger
        self.io_util_boundary = io_util_boundary
 
    def quit(self):
        self.exit = True
        return

    def iostat(self):
        # Get iostat
        iostat_map = execute(remote.iostat, hosts=[self.host])[self.host]
        if iostat_map == None:
            warn(red("[%s] Get iostat fail." % self.host))

        self.handleResult(iostat_map)

    def handleResult(self, iostat):
        # Check IO utilization
        io_util = ''
        log = None
        for device, stat in iostat.items():
            if len(io_util) > 0:
                io_util = io_util + ' '
            io_util = io_util + device + '=' + stat['%util']
            if float(stat['%util']) >= self.io_util_boundary:
                log = 'IO UTIL'

        if log != None:
            self.logger.critical(CHECK_MACHINE_LOG_COLUMN % (
                self.host, log, io_util))

    def run(self):
        while self.exit == False:
            self.iostat()

def command(ip, port, cmd, verbose=True, timeout=5):
    t = None
    try:
        t = telnetlib.Telnet(ip, port, timeout)
    except socket.error as e:
        if verbose:
            warn(red(e))
            warn(red('Connection failed. IP:%s, PORT:%d' % (ip, port)))
        raise

    t.write(cmd + '\r\n')
    reply = t.read_until('\r\n', timeout)
    t.close()
    return reply

def get_gw_state(ip, port, verbose=True):
    try:
        cmd = 'ping'
        reply = command(ip, port, cmd, verbose, timeout=2)
        
        if reply == '+PONG\r\n':
            return 'N'
        else:
            if verbose:
                warn(red('Gateway error. IP:%s, PORT:%d, CMD:"%s", REPLY:"%s"' % (ip, port, cmd, reply)))
            return 'F'
    except socket.error as e:
        if verbose:
            warn(red('get_gw_state fail, IP:%s, PORT:%d, CMD:"%s"' % (ip, port, cmd)))
        return 'F'

def get_gw_info(ip, port, verbose=True):
    out = Output()
    out.inactive_connections = -1
    out.gateway_connected_clients = -1

    try:
        with GwCmd(ip, port) as gw_cmd:
            out.inactive_connections = gw_cmd.info_redis_discoons()
            out.gateway_connected_clients = gw_cmd.info_num_of_clients()
            out.gateway_total_commands_processed = gw_cmd.info_total_commands_processed()
        return out

    except IOError as e:
        warn(red(e))
        return out 
    except TypeError as e:
        warn(red(e))
        return out

def get_gw_inactive_connections(ip, port, verbose=True):
    try:
        with GwCmd(ip, port) as gw_cmd:
            return gw_cmd.info_redis_discoons()

    except IOError as e:
        warn(red(e))
        return -1
    except TypeError as e:
        warn(red(e))
        return -1


class GwInactiveConnectionChecker(threading.Thread):
    inact_con = -1

    def __init__(self, gw_id, ip, port):
        threading.Thread.__init__(self)
        self.gw_id = gw_id
        self.ip = ip
        self.port = port

    def run(self):
        # Check Redis client connection
        self.inact_con = get_gw_inactive_connections(self.ip, self.port)

"""
Returns:
    boolean: True if success, else False
    total_con: sum of gw_inactive_connections
    dict: {'id' : gw_id, 'ip' : gw_ip, 'port' : gw_port, 'inact_con' : inact_con }
"""
def get_gws_inactive_cons(gw_list):
    thrs = []
    inact_cons = {} 
    for gw_id, gw_data in gw_list.items():
        gw_ip = gw_data['ip'].encode('ascii')
        gw_port = gw_data['port']

        inact_cons[gw_id] = {'id' : gw_id, 'ip' : gw_ip, 'port' : gw_port}

        thr = GwInactiveConnectionChecker(gw_id, gw_ip, gw_port)
        thr.start()
        thrs.append(thr)

    ok = True 
    total_inact_con = 0
    for thr in thrs:
        thr.join()
        inact_cons[thr.gw_id]['inact_con'] = thr.inact_con
        if thr.inact_con == -1:
            ok = False
        else:
            total_inact_con += thr.inact_con

    if total_inact_con != 0:
        ok = False
        
    return ok, total_inact_con, inact_cons

def check_gw_inactive_connections_par(gw_list):
    print magenta("\nGateway inactive connection test.")
    print magenta("Gateways:%s" % " ".join("%d(%s:%s)" % (id, data['ip'], data['port']) for id, data in gw_list.items()))

    # Check Redis client connection
    while True:
        ok, total_con, inact_cons = get_gws_inactive_cons(gw_list)
        print yellow("Gateway inactive connections. total:%d\n%s" % 
                (total_con, " ".join("%(id)d(%(inact_con)d)" % data for id, data in inact_cons.items())))

        if ok: 
            # Check consistency of inactive connections while 1 seconds
            done = True 
            for i in range(5):

                ok, total_con, inact_cons = get_gws_inactive_cons(gw_list)
                print yellow("Gateway inactive connections. total:%d\n%s" % 
                        (total_con, " ".join("%(id)d(%(inact_con)d)" % data for id, data in inact_cons.items())))
                if ok == False:
                    done = False
                    break
                time.sleep(0.2)

            if done:
                break
        time.sleep(0.5)
    
    print green("Gateway inactive connection test.")
    print green("Gateways:%s\n" % " ".join("%d(%s:%s)" % (id, data['ip'], data['port']) for id, data in gw_list.items()))
    return True

def check_gw_inactive_connections(ip, port, verbose=False):
    print magenta("\n[%s:%d] Gateway inactive connection test" % (ip, port))
    
    # Check Redis client connection
    while True:
        num_connected = get_gw_inactive_connections(ip, port)
        print yellow("[%s:%d] Gateway inactive connections:%d" % (ip, port, num_connected))

        if (num_connected == 0): 
            # Check consistency of inactive connections while 1 seconds
            ok = True 
            for i in range(5):
                cnt = get_gw_inactive_connections(ip, port)
                print yellow("[%s:%d] >> gateway inactive connections:%d" % (ip, port, cnt))
                if cnt != 0:
                    ok = False
                    break
                time.sleep(0.2)

            if ok:
                break
        time.sleep(0.5)

    print green("[%s:%d] Gateway inactive connection test success" % (ip, port))
    return True

def check_smr_state(ip, smr_base_port, host):
    while True:
        try:
            conn = telnetlib.Telnet(ip, smr_base_port + 3)
            conn.write('ping\r\n')
            ret = conn.read_until('\r\n', 1)
            conn.close()

            print yellow('[%s] >>> %s' % (host, ret.strip()))
            if ('+OK 2' in ret) or ('+OK 3' in ret): break

            time.sleep(0.5)
        except:
            time.sleep(0.5)
            continue

    print green('[%s] Join PGS success' % host)
    return True

def check_redis_state(ip, redis_port):
    while True:
        try:
            conn = telnetlib.Telnet(ip, redis_port)
            conn.write('ping\r\n')
            ret = conn.read_until('\r\n', 1)
            conn.close()

            print yellow('[%s:%d] >>> %s' % (ip, redis_port, ret.strip()))
            if '+PONG\r\n' == ret: break

            time.sleep(0.5)
        except:
            print yellow('[%s:%d] >>> retry to connect to redis' % (ip, redis_port))
            time.sleep(0.5)
            continue

    print green('[%s:%d] Check Redis state success' % (ip, redis_port))
    return True

def get_role_of_smr(ip, port, verbose=True):
    cmd = 'ping'
    try:
        reply = command(ip, port, cmd, verbose, timeout=2)
        
        ret = reply.split(' ')
        if len(ret) < 3:
            return '?'

        if ret[1] == '0':
            return 'F'
        elif ret[1] == '1':
            return 'L'
        elif ret[1] == '2':
            return 'M'
        elif ret[1] == '3':
            return 'S'
        else:
            if verbose:
                warn(red('SMR error. IP:%s, PORT:%d, CMD:"%s", REPLY:"%s"' % (ip, port, cmd, reply)))
            return '?'
    except socket.error as e:
        if verbose:
            warn(red('get_role_of_smr fail, IP:%s, PORT:%d, CMD:"%s"' % (ip, port, cmd)))
        return '?'

def get_logseq(ip, port):
    cmd = 'getseq log'
    try:
        reply = command(ip, port, cmd, False)

        logseq = {}
        if reply != None and reply != '':
            tokens = reply.split(' ')

            logseq['be_sent'] = -1
            for token in tokens:
                if token.find('min') != -1:
                    logseq['min'] = int(token.split(':')[1])
                elif token.find('commit') != -1:
                    logseq['commit'] = int(token.split(':')[1])
                elif token.find('max') != -1:
                    logseq['max'] = int(token.split(':')[1])
                elif token.find('be_sent') != -1:
                    logseq['be_sent'] = int(token.split(':')[1])

        return logseq
    except socket.error as e:
        warn(red('[%s:%d] Get logseq fail, CMD:"%s", REPLY:"%s"' % (ip, port, cmd, reply)))
        return None

def slot_rle_to_map(pn_pg_map):
    slot_info = pn_pg_map.split(' ')

    i = 0
    slot_no = 0
    slot_map = {}
    while i < len(slot_info):
        pg_id = int(slot_info[i])
        slot_len = int(slot_info[i+1])
        i = i + 2

        if pg_id not in slot_map.keys():
            slot_map[pg_id] = []
        slot_map[pg_id].append('%s:%s' % (slot_no, slot_no + slot_len - 1))
        
        slot_no = slot_no + slot_len

    for k, slot in slot_map.items():
        slot_map[k] = ' '.join(slot)

    return slot_map

# It returns the number of clients connections; <gateway connection count> + <mgmt-cc connection count>
def get_redis_client_connection_count(ip, port):
    conn = telnetlib.Telnet(ip, port)
    conn.write('client list\r\n')

    conn.read_until('\r\n')
    reply = conn.read_until('\r\n')

    connection_cnt = 0
    for line in reply.split('\n'):
        tokens = line.split(' ')
        for token in tokens:
            kv = token.split('=')
            if kv[0] == 'cmd':
                if kv[1] != 'info' and kv[1] != 'client':
                    connection_cnt  = connection_cnt + 1
                break

    conn.close()
    return connection_cnt 

# On success, get_redis_tps() returns tps; on error; it returns -1.
def redis_tps(ip, port):
    print ip, port
    try:
        with RedisCmd(ip, port) as redis_conn:
            return redis_conn.info_tps()

    except IOError as e:
        warn(red(e))
        return -1
    except TypeError as e:
        warn(red(e))
        return -1

# On success, change_master() returns pgs_id of new master; on error, it returns -1.
def change_master(cluster_name, pg_id, host):
    print magenta("\n[%s] Change master, PG:%d" % (host ,pg_id))

    # Get candidates for master
    candidates = []

    pgs_list = cm.get_pgs_list(cluster_name, pg_id)
    for id, data in pgs_list.items():
        if data['smr_role'] == 'S' and data['hb'] == 'Y':
            active_role = get_role_of_smr(data['ip'], data['mgmt_port'], verbose=False)
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

def json_to_str(json_data):
    return json.dumps(json_data, sort_keys=True, indent=4, separators=(',', ' : '), default=handle_not_json_format_object)

def handle_not_json_format_object(o):
    return None

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

def print_script(cluster_name, quorum_policy, pg_list, pgs_list, gw_list):
    print yellow("\n[SCRIPT]")

    print 'CLUSTER:'
    print "cluster_add %s %s" % (cluster_name, quorum_policy)
    print ''

    print 'GW:'
    for gw in gw_list:
        print "%s %d %s %s %d" % (cluster_name, gw['gw_id'], gw['pm'][0], gw['pm'][1], gw['port'])
    print ''

    print 'PG:'
    for pg in pg_list:
        print "pg_add %s %d" % (cluster_name, pg['id'])
    print ''

    print 'PN PG MAP:'
    for pg in pg_list:
        print "slot_set_pg %s %d:%d %d" % (cluster_name, pg['range_from'], pg['range_to'], pg['id'])
    print ''

    print 'PGS:'
    for pgs in pgs_list:
        print "%s %d %d %s %s %d" % (cluster_name, pgs['pgs_id'], pgs['pg_id'], pgs['pm'][0], pgs['pm'][1], pgs['port'])
    print ''

def cont():
    if config.confirm_mode and not confirm(cyan('Continue?')):
        return False
    if config.confirm_mode:
        config.confirm_mode = confirm(cyan("Confirm Mode?"))
    return True

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

