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
import traceback
import sys
from fabric.api import *
from fabric.colors import *
from fabric.contrib.console import *
from fabric.contrib.files import *
import util
import importlib
from gw_cmd import *

config = None

def set_config(config_module):
    global config
    config = config_module

def init():
    global cm_conn

    # Ping Check
    try:
        cm_conn = telnetlib.Telnet(config.CONF_MASTER_IP, config.CONF_MASTER_PORT)
        cm_conn.write('cluster_ls\r\n')
        ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
        ret = json.loads(ret)
        if 'redirect' == ret['state']:
            cm_conn.close()

            config.CONF_MASTER_IP = ret['data']['ip']
            config.CONF_MASTER_PORT = int(ret['data']['port'])
            cm_conn = telnetlib.Telnet(config.CONF_MASTER_IP, config.CONF_MASTER_PORT)
            cm_conn.write('cluster_ls\r\n')
            ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
            ret = json.loads(ret)
            if 'success' != ret['state']:
                warn(red('Can not connect to Conf master. Aborting...'))
                return False

        elif 'success' != ret['state']:
            warn(red('Can not connect to Conf master. Aborting...'))
            return False
    except:
        warn(red('Can not connect to Conf master. Aborting...'))
        return False

    return True

def pm_ls():
    cm_conn.write('pm_ls\r\n')
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret.encode('ascii'))
    if json_data['state'] != 'success':
        warn(red("Get pm_ls fail."))
        return None
    return json_data['data']

def pm_info(pm_name):
    cmd = 'pm_info %s\r\n' % pm_name
    cm_conn.write(cmd)
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        warn(red("Get pm_info fail."))
        return None
    return json_data['data']

def pm_add(name, ip):
    cmd = 'pm_add %s %s\r\n' % (name, ip)
    cm_conn.write(cmd)
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        warn(red('PM Add fail. cmd:%s, ret:%s' % (cmd[:-2], ret.strip())))
        return False
    return True

def cluster_ls():
    cm_conn.write('cluster_ls\r\n')
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        warn(red("Cluster '%s' doesn't exist." % cluster_name))
        return None
    return json_data

def cluster_add(name, quorum_policy):
    cm_conn.write('cluster_add %s %s\r\n' % (name, quorum_policy))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        warn(red("Add cluster '%s' fail." % name))
        return False
    return True

def cluster_del(name):
    cm_conn.write('cluster_del %s\r\n' % (name))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        warn(red("Delete cluster '%s' fail." % name))
        return False
    return True

def cluster_info(cluster_name):
    cm_conn.write('cluster_info %s\r\n' % cluster_name)
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        warn(red("Cluster '%s' doesn't exist." % cluster_name))
        return None
    json_data['data']['cluster_name'] = cluster_name
    return json_data

def pg_add(cluster_name, pg_id):
    cm_conn.write('pg_add %s %d\r\n' % (cluster_name, pg_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        warn(red("Add PG '%d' fail." % pg_id))
        return False
    return True

def pg_del(cluster_name, pg_id):
    cm_conn.write('pg_del %s %d\r\n' % (cluster_name, pg_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        warn(red("Delete PG '%d' fail." % pg_id))
        return False
    return True

def pg_infos(cluster_name, pg_ids):
    cluster_json = cluster_info(cluster_name)
    if cluster_json['state'] != 'success':
        warn(red("Cluster '%s' doesn't exist." % cluster_name))
        return None

    slot_map = util.slot_rle_to_map(cluster_json['data']['cluster_info']['PN_PG_Map'])

    cmd = ''
    for pg_id in pg_ids:
        cmd = cmd + 'pg_info %s %s\r\n' % (cluster_name, pg_id.encode('ascii'))

    cm_conn.write(cmd)

    pg_list = []
    for i in range(len(pg_ids)):
        reply = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
        pg_json = pgstr_to_json(int(pg_ids[i]), reply, slot_map)
        pg_list.append(pg_json)

    return sorted(pg_list, key=lambda x: int(x['pg_id']))
    
"""
Get pg information from mgmt-cc

Args:
    cluster_name : cluster name
    pg_id : partition group id
    cluster_json : It is cached data for cluster_info. 
                   When cluster_json is None, pg_info() get cluster-info from MGMT-CC

Returns:
    dict: On success, it returns dick of pg; on error, it returns None.
"""
def pg_info(cluster_name, pg_id, cluster_json=None):
    if cluster_json == None:
        cluster_json = cluster_info(cluster_name)
        if cluster_json['state'] != 'success':
            warn(red("Cluster '%s' doesn't exist." % cluster_name))
            return None

    slot_map = util.slot_rle_to_map(cluster_json['data']['cluster_info']['PN_PG_Map'])

    cm_conn.write('pg_info %s %d\r\n' % (cluster_name, pg_id))
    reply = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    return pgstr_to_json(pg_id, reply, slot_map)

def pgstr_to_json(pg_id, json_str, slot_map):
    json_data = json.loads(json_str)
    if json_data['state'] != 'success':
        warn(red("PG '%d' doesn't exist." % pg_id))
        return None

    pg_json = json_data['data']
    pg_json['pg_id'] = pg_id
    if slot_map.get(pg_id) == None:
        pg_json['slot'] = ''
    else:
        pg_json['slot'] = slot_map[pg_id]

    if len(pg_json['master_Gen_Map']) == 0:
        pg_json['master_Gen'] = -1
    else:
        pg_json['master_Gen'] = max(int(k) for k in pg_json['master_Gen_Map']) + 1

    return pg_json

def get_master_pgs(cluster_name, pg_id):
    pgs_list = get_pgs_list(cluster_name, pg_id)
    if pgs_list == None:
        warn(red("Get PGS list fail. CLUSTER_NAME:%s, PG_ID:%d" % (cluster_name, pg_id)))
        return None

    for pgs_id, pgs_data in pgs_list.items():
        if pgs_data['smr_role'] == 'M':
            return pgs_data

    return None

"""
Get pgs list from mgmt-cc

Args:
    cluster_name : cluster name
    pg_id : partition group id
    cluster_json : It is cached data for cluster_info. 
                   When cluster_json is None, pg_info() get cluster_info from MGMT-CC

Returns:
    dict: On success, it returns map of pgs; on error, it returns None.
"""
def get_pgs_list(cluster_name, pg_id, cluster_json=None):
    # Get cluster info from Conf Master
    if cluster_json == None:
        cluster_json = cluster_info(cluster_name)
        if cluster_json == None: return None

    exist = [k for loop_pg_data in cluster_json['data']['pg_list'] 
                    for k, v in loop_pg_data.iteritems() if k == 'pg_id' and int(v) == pg_id]
    if len(exist) == 0:
        warn(red("PG '%d' doesn't exist." % pg_id))
        return None

    pgs_list = {}
    # Get PG and PGS info from Json
    for pg_data in cluster_json['data']['pg_list']:
        cur_pg_id = int(pg_data['pg_id'])
        if cur_pg_id != pg_id:
            continue

        for pgs_id in pg_data['pg_data']['pgs_ID_List']:
            pgs_json_data = pgs_info(cluster_name, pgs_id)
            if pgs_json_data == None: 
                warn(red("PGS '%s' doesn't exist." % pgs_id))
                return None
            pgs_list[pgs_id] = pgs_json_data['data']

    if len(pgs_list) == 0:
        return None

    return pgs_list

def get_joined_pgs_count(cluster_name, pg_id):
    pgs_list = get_pgs_list(cluster_name, pg_id)
    if pgs_list == None:
        return -1

    cnt = 0
    for id, data in pgs_list.items():
        if data['hb'] == 'Y':
            cnt = cnt + 1
    return cnt

def pgs_info(cluster_name, pgs_id):
    cm_conn.write('pgs_info %s %d\r\n' % (cluster_name, pgs_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        return None

    json_data['data']['pgs_id'] = pgs_id
    json_data['data']['pg_id'] = json_data['data']['pg_ID']
    json_data['data']['ip'] = json_data['data']['pm_IP'].encode('ascii')
    json_data['data']['pm_name'] = json_data['data']['pm_Name'].encode('ascii')
    json_data['data']['redis_port'] = json_data['data']['backend_Port_Of_Redis']
    json_data['data']['smr_base_port'] = json_data['data']['replicator_Port_Of_SMR']
    json_data['data']['mgmt_port'] = json_data['data']['management_Port_Of_SMR']
    json_data['data']['smr_role'] = json_data['data']['smr_Role'].encode('ascii')
    json_data['data']['hb'] = json_data['data']['hb'].encode('ascii')

    return json_data

def pgs_add(cluster_name, pgs_id, pg_id, pm_name, pm_ip, port, host):
    if config.confirm_mode and not confirm(cyan('[%s] PGS Add, CLUSTER:%s, PGS_ID:%d. Continue?' % (host, cluster_name, pgs_id))):
        warn("Aborting at user request.")
        return False

    cmd = 'pgs_add %s %d %d %s %s %d %d\r\n' % (cluster_name, pgs_id, pg_id, pm_name, pm_ip, port, port + 9)
    cm_conn.write(cmd)
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success' or json_data['msg'] != '+OK':
        warn(red('[%s] PGS Add fail. cmd:%s, ret:%s' % (host, cmd[:-2], ret.strip())))
        return False

    cm_conn.write('pgs_info %s %d\r\n' % (cluster_name, pgs_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    print ret.strip() + '\n'

    print green('[%s] PGS Add success' % host)
    return True

def pgs_join(cluster_name, pgs_id, ip, smr_base_port, host):
    print magenta("\n[%s] PGS Join" % host)
    cm_conn.write('pgs_info %s %d\r\n' % (cluster_name, pgs_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    print ret.strip() + '\n'
            
    if config.confirm_mode and not confirm(cyan('[%s] PGS Join, PGS_ID:%d. Continue?' % (host, pgs_id))):
        warn("Aborting at user request.")
        return False

    cm_conn.write('pgs_join %s %d\r\n' % (cluster_name, pgs_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success' or json_data['msg'] != '+OK':
        warn(red('[%s] PGS Join fail, ret:%s' % (host, ret.strip())))
        return False

    return util.check_smr_state(ip, smr_base_port, host)

def pgs_leave(cluster_name, pgs_id, ip, redis_port, host, check=True, forced=False):
    print magenta("\n[%s] PGS Leave" % host)
    cm_conn.write('pgs_info %s %d\r\n' % (cluster_name, pgs_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    print ret.strip() + '\n'
            
    if config.confirm_mode and not confirm(cyan('[%s] PGS Leave, PGS_ID:%d. Continue?' % (host, pgs_id))):
        warn("Aborting at user request.")
        return False

    cmd = 'pgs_leave %s %d' % (cluster_name, pgs_id)
    if forced:
        cmd = cmd + ' forced'
    cm_conn.write(cmd + '\r\n')
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success' or json_data['msg'] != '+OK':
        warn(red('[%s] PGS Leave fail, ret:%s' % (host, ret.strip())))
        return False

    # Check Redis client connection
    if check == True:
        conn = None
        try:
            conn = telnetlib.Telnet(ip, redis_port)

            while True:
                conn.write('info clients\r\n')
                reply = conn.read_until('\r\n', config.TELNET_TIMEOUT)
                size = int(reply[1:])

                readlen = 0
                while readlen <= size:
                    ret = conn.read_until('\r\n', config.TELNET_TIMEOUT)
                    readlen += len(ret)
                    ret = ret.strip()
                    if ret.find('connected_clients') != -1 and ret.find(':') != -1:
                        tokens = ret.split(':')
                        print yellow('[%s:%d] >>> %s' % (ip, redis_port, ret))
                        num_connected = int(tokens[1])

                if (num_connected < 10): break
                time.sleep(0.5)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
            warn(red('[%s] PGS Leave fail, Can not connect to Redis server. %s:%d' % (host, ip, redis_port)))
            return False
        finally:
            if conn != None:
                conn.close()

    print green('[%s] PGS Leave success' % host)
    return True

def pgs_lconn(cluster_name, pgs_id, ip, smr_base_port, host):
    print magenta("\n[%s] PGS lconn" % host)
    cm_conn.write('pgs_info %s %d\r\n' % (cluster_name, pgs_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    print ret.strip() + '\n'
            
    if config.confirm_mode and not confirm(cyan('[%s] PGS lconn, PGS_ID:%d. Continue?' % (host, pgs_id))):
        warn("Aborting at user request.")
        return False

    cm_conn.write('pgs_lconn %s %d\r\n' % (cluster_name, pgs_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success' or json_data['msg'] != '+OK':
        warn(red('[%s] PGS lconn fail, ret:%s' % (host, ret.strip())))
        return False

    # Check SMR State
    try:
        conn = telnetlib.Telnet(ip, smr_base_port + 3)

        while True:
            conn.write('ping\r\n')
            ret = conn.read_until('\r\n', config.TELNET_TIMEOUT)
            print yellow('[%s] >>> %s' % (host, ret.strip()))
            if '+OK 1' in ret: break
            time.sleep(0.5)
    except:
        warn(red('[%s] PGS lconn fail, Can not connect to SMR replicator. %s:%d' % (host, ip, smr_base_port)))

    conn.close()

    print green('[%s] PGS lconn success' % host)
    return True

def pgs_del(cluster_name, pgs_id, host):
    if config.confirm_mode and not confirm(cyan('[%s] PGS Del, CLUSTER:%s, PGS_ID:%d. Continue?' % (host, cluster_name, pgs_id))):
        warn("Aborting at user request.")
        return False

    cm_conn.write('pgs_del %s %d\r\n' % (cluster_name, pgs_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success' or json_data['msg'] != '+OK':
        warn(red('[%s] PGS Del fail, ret:%s' % (host, ret.strip())))
        return False

    cm_conn.write('pgs_info %s %d\r\n' % (cluster_name, pgs_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] == 'success':
        warn(red('[%s] PGS Del fail, ret:%s' % (host, ret.strip())))
        return False

    print green('[%s] PGS Del success' % host)
    return True

# On success, role_change() returns pgs_id of new master; on error, it returns -1
def role_change(cluster_name, pgs_id, host):
    print magenta("\n[%s] Role Change" % host)

    cm_conn.write('role_change %s %d\r\n' % (cluster_name, pgs_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        print green("[%s] Role Change fail, ret:%s" % (host, ret.strip()))
        return -1

    print green("[%s] Role Change success" % host)
    if json_data.get('data') != None:
        return int(json_data['data']['master'])
    return pgs_id

def get_gw_list(cluster_name):
    # Get cluster info from Conf Master
    json_data = cluster_info(cluster_name)
    if json_data == None: return None

    gw_list = {}
    # Get GW info from Json
    for gw_id in json_data['data']['gw_id_list']:
        gw_json_data = gw_info(cluster_name, gw_id)
        if gw_json_data == None:
            warn(red("Get gateway info fail. CLUSTER_NAME:%s, GW_ID:%d" % (cluster_name, gw_id)))
            return

        gw_list[gw_id] = gw_json_data['data']

    if len(gw_list) == 0:
        return None

    return gw_list

def gw_info(cluster_name, gw_id):
    cm_conn.write('gw_info %s %d\r\n' % (cluster_name, gw_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        return None
    return json_data

def gw_add(cluster_name, gw_id, pm_name, ip, port, additional_clnt=0):
    host = ip
    print magenta("\n[%s] GW Add" % host)
            
    if config.confirm_mode and not confirm(cyan('[%s] GW Add, GW_ID:%d. Continue?' % (host, gw_id))):
        warn("Aborting at user request.")
        return False

    cmd = 'gw_add %s %d %s %s %d\r\n' % (cluster_name, gw_id, pm_name, ip, port)
    cm_conn.write(cmd)
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success' or json_data['msg'] != '+OK':
        warn(red('[%s] GW Add fail, ret:%s' % (host, ret.strip())))
        return False

    # Check gateway client connection
    try:
        with GwCmd(ip, port) as gw_cmd:
            # Auto mode
            ok = False
            for i in range(15):
                print yellow('[%s:%d] >>> inactive_connections:%d' % (host, port, gw_cmd.info_redis_discoons()))
                print yellow('[%s:%d] >>> gateway_connected_clients:%d' % (host, port, gw_cmd.info_num_of_clients()))
                if gw_cmd.info_redis_discoons() == 0 and gw_cmd.info_num_of_clients() >= 1 + config.CONF_MASTER_MGMT_CONS + additional_clnt:
                    ok = True
                    break
                else:
                    time.sleep(0.5)

            if ok == False:
                # Manual mode
                print magenta('\n[%s:%d] Begin manual mode.' % (host, port))

                while True:
                    print yellow('[%s:%d] >>> gateway_connected_clients:%d' % (host, port, gw_cmd.info_num_of_clients()))
                    print yellow('[%s:%d] >>> gateway_total_commands_processed:%d' % (host, port, gw_cmd.info_total_commands_processed()))

                    if not confirm(cyan('[%s:%d] GW Add, number of client connection is %d. Wait?' % (host, port, gw_cmd.info_num_of_clients()))):
                        break
                
    except:
        warn(red('[%s] GW Add fail, Can not connect to Gateway server. %s:%d' % (host, ip, port)))
        return False

    print green('[%s] GW Add success' % host)
    return True

def gw_del(cluster_name, gw_id, ip, port):
    host = ip
    print magenta("\n[%s] GW Del" % host)
    cm_conn.write('gw_info %s %d\r\n' % (cluster_name, gw_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    print ret.strip() + '\n'
            
    if config.confirm_mode and not confirm(cyan('[%s] GW Del, GW_ID:%d. Continue?' % (host, gw_id))):
        warn("Aborting at user request.")
        return False

    cm_conn.write('gw_del %s %d\r\n' % (cluster_name, gw_id))
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success' or json_data['msg'] != '+OK':
        warn(red('[%s] GW Del fail, ret:%s' % (host, ret.strip())))
        return False

    # Check gateway client connection
    try:
        with GwCmd(ip, port) as gw_cmd:
            # 1. Wait until all client-connections is disconnected.
            ok = False
            old_total_commands_processed = 0
            for i in range(16):
                num_of_clients = gw_cmd.info_num_of_clients()
                total_commands = gw_cmd.info_total_commands_processed()
                print yellow('[%s:%d] >>> gateway_connected_clients:%d' % (host, port, num_of_clients))
                print yellow('[%s:%d] >>> gateway_total_commands_processed:%d (normally +2)' % (host, port, total_commands))

                if num_of_clients == 1 and old_total_commands_processed + 3 > total_commands:
                    ok = True
                    break
                else:
                    time.sleep(0.5)
                old_total_commands_processed = total_commands

            # 2. Wait a few seconds for client's pipelining operations.
            if ok == True:
                for cti in range(config.CLIENT_TIMEOUT * 4):
                    num_of_clients = gw_cmd.info_num_of_clients()
                    total_commands = gw_cmd.info_total_commands_processed()
                    print yellow('[%s:%d] >>> gateway_connected_clients:%d' % (host, port, num_of_clients))
                    print yellow('[%s:%d] >>> gateway_total_commands_processed:%d (normally +2)' % (host, port, total_commands))

                    if num_of_clients != 1 and old_total_commands_processed + 3 <= total_commands:
                        ok = False
                        break
                    else:
                        time.sleep(0.5)
                    old_total_commands_processed = total_commands

            # Manual mode
            if ok == False:
                print magenta('[%s:%d] Begin manual checking.' % (host, port))

                while True:
                    num_of_clients = gw_cmd.info_num_of_clients()
                    total_commands = gw_cmd.info_total_commands_processed()
                    print yellow('[%s:%d] >>> gateway_connected_clients:%d' % (host, port, num_of_clients))
                    print yellow('[%s:%d] >>> gateway_total_commands_processed:%d (normally +2)' % (host, port, total_commands))

                    if not confirm(cyan('[%s:%d] GW Del, number of client connection is %d. Wait?' % (host, port, gw_cmd.info_num_of_clients()))):
                        break
                
    except:
        warn(red('[%s] GW Del fail, Can not connect to Gateway server. %s:%d' % (host, ip, port)))
        return False

    print green('[%s] GW Del success' % host)
    return True

def mig2pc(cluster_name, src_pg_id, dst_pg_id, range_from, range_to):
    cmd = 'mig2pc %s %d %d %d %d' % (cluster_name, src_pg_id, dst_pg_id, range_from, range_to)
    print cyan('Command : ' + cmd)
    cm_conn.write(cmd + '\r\n')
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    try:
        json_data = json.loads(ret)
        if json_data['state'] != 'success' or json_data['msg'] != '+OK':
            warn(red("mig2pc fail. cluster:%s, ret:\"%s\"" % (cluster_name, ret[:-2])))
            return False
        return True
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback)
        warn("mig2pc fail. cluster:%s" % cluster_name)
        return False

def slot_set_pg(cluster_name, dst_pg_id, range_from, range_to):
    cmd = 'slot_set_pg %s %d:%d %d' % (cluster_name, range_from, range_to, dst_pg_id)
    print cyan('Command : ' + cmd)
    cm_conn.write(cmd + '\r\n')
    ret = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(ret)
    if json_data['state'] != 'success' or json_data['msg'] != '+OK':
        warn(red("%s fail." % cmd))
        return False
    return True

def pgs_ls(cluster_name):
    cm_conn.write('pgs_ls %s\r\n' % cluster_name)
    reply = cm_conn.read_until('\r\n', config.TELNET_TIMEOUT)
    json_data = json.loads(reply)
    if json_data['state'] != 'success':
        return None
    return json_data
