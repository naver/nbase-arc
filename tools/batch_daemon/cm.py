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
from fabric.api import *
from fabric.colors import *
from fabric.contrib.console import *
from fabric.contrib.files import *
import config

cm_conn = None

class Output:
    def __init__(self):
        self.cm = True

def init():
    for i in range(3):
        if connect() == True:
            return True
        time.sleep(1)

    return False

def connect():
    global cm_conn

    # Ping Check
    try:
        cm_conn = telnetlib.Telnet(config.CONF_MASTER_IP, config.CONF_MASTER_PORT)
        cm_conn.write('cluster_ls\r\n')
        ret = cm_conn.read_until('\r\n', 1)
        ret = json.loads(ret)
        if 'redirect' == ret['state']:
            config.CONF_MASTER_IP = ret['data']['ip']
            config.CONF_MASTER_PORT = int(ret['data']['port'])
            return False
        elif 'success' != ret['state']:
            warn(red('Can not connect to Conf master. Aborting...'))
            return False
    except:
        warn(red('Can not connect to Conf master. Aborting...'))
        return False

    return True

def check_connection():
    if ping() == False:
        return reconnect()
    else:
        return True

def ping():
    try:
        cm_conn.write('ping\r\n')
        ret = cm_conn.read_until('\r\n', 3)
        json_data = json.loads(ret)
        if json_data['state'] != 'success' or json_data['msg'] != '+PONG':
            return False
    except:
        return False

    return True

def reconnect():
    global cm_conn

    if cm_conn != None:
        cm_conn.close()

    for i in range(3):
        if connect() == True:
            return True
        time.sleep(1)

    return False

def pg_list(cluster_name):
    out = Output()
    out.json = None

    if check_connection() == False: 
        out.cm = False
        return out 

    out_cluster_info = cluster_info(cluster_name)
    if out_cluster_info.cm == False:
        out.cm = False
        return out

    cluster_json = out_cluster_info.json
    if cluster_json['state'] != 'success':
        warn(red("Cluster '%s' doesn't exist." % cluster_name))
        return out

    slot_map = slot_rle_to_map(cluster_json['data']['cluster_info']['PN_PG_Map'])

    cmd = ''
    for pg in cluster_json['data']['pg_list']:
        pg_id = pg['pg_id'].encode('ascii')
        cmd = cmd + 'pg_info %s %s\r\n' % (cluster_name, pg_id)

    cm_conn.write(cmd)

    pg_list = []
    for pg in cluster_json['data']['pg_list']:
        pg_id = pg['pg_id'].encode('ascii')
        reply = cm_conn.read_until('\r\n', 1)
        pg_json = pgstr_to_json(int(pg_id), reply, slot_map)
        pg_list.append(pg_json)

    out.json = sorted(pg_list, key=lambda x: int(x['pg_id']))
    return out

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

# On success, get_pgs_list() returns map of pgs; on error, it returns None.
def get_pgs_list(cluster_name, pg_id):
    out = Output()
    out.json = None

    if check_connection() == False: 
        out.cm = False
        return out

    # Get cluster info from Conf Master
    out_cluster_info = cluster_info(cluster_name)
    if out_cluster_info.cm == False:
        out.cm = False
        return out

    if out_cluster_info.json == None: 
        return None
    cluster_json_data = out_cluster_info.json

    exist = [k for loop_pg_data in cluster_json_data ['data']['pg_list'] 
                    for k, v in loop_pg_data.iteritems() if k == 'pg_id' and int(v) == pg_id]
    if len(exist) == 0:
        warn(red("PG '%d' doesn't exist." % pg_id))
        return None

    pgs_list = {}
    # Get PG and PGS info from Json
    for pg_data in cluster_json_data['data']['pg_list']:
        cur_pg_id = int(pg_data['pg_id'])
        if cur_pg_id != pg_id:
            continue

        for pgs_id in pg_data['pg_data']['pgs_ID_List']:
            out_pgs_info = pgs_info(cluster_name, pgs_id)
            if out_pgs_info.cm == False:
                out.cm = False
                return out

            if out_pgs_info.json == None: 
                warn(red("PGS '%s' doesn't exist." % pgs_id))
                return None
            pgs_list[pgs_id] = out_pgs_info.json['data']

    out.json = pgs_list
    return out

def pgs_info(cluster_name, pgs_id):
    out = Output()
    out.json = None
    
    if check_connection() == False: 
        out.cm = False
        return out

    cm_conn.write('pgs_info %s %d\r\n' % (cluster_name, pgs_id))
    ret = cm_conn.read_until('\r\n', 1)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        return out

    json_data['data']['pgs_id'] = pgs_id
    json_data['data']['pg_id'] = json_data['data']['pg_ID']
    json_data['data']['ip'] = json_data['data']['pm_IP']
    json_data['data']['redis_port'] = json_data['data']['backend_Port_Of_Redis']
    json_data['data']['smr_base_port'] = json_data['data']['replicator_Port_Of_SMR']
    json_data['data']['mgmt_port'] = json_data['data']['management_Port_Of_SMR']
    json_data['data']['smr_role'] = json_data['data']['smr_Role'].encode('ascii')
    json_data['data']['hb'] = json_data['data']['hb']

    out.json = json_data
    return out 

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

    pg_json['master_Gen'] = max(int(k) for k in pg_json['master_Gen_Map']) + 1

    return pg_json

def cluster_ls():
    out = Output()
    out.json = None

    if check_connection() == False: 
        out.cm = False
        return None

    cm_conn.write('cluster_ls\r\n')
    ret = cm_conn.read_until('\r\n', 1)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        warn(red("cluster_ls fail. reply:%s" % ret))
        return out

    out.json = json_data
    return out

def cluster_info(cluster_name):
    out = Output()
    out.json = None

    if check_connection() == False: 
        out.cm = False
        return out

    cm_conn.write('cluster_info %s\r\n' % cluster_name)
    ret = cm_conn.read_until('\r\n', 1)
    json_data = json.loads(ret)
    if json_data['state'] != 'success':
        warn(red("Cluster '%s' doesn't exist." % cluster_name))
        return out

    out.json = json_data
    return out

# return list of appdata
def appdata_get(cluster_name):
    out = Output()
    out.appdata_list = None

    if check_connection() == False: 
        out.cm = False
        return out 

    try:
        cmd = 'appdata_get %s backup all\r\n' % cluster_name
        cm_conn.write(cmd)
        appdata_list  = cm_conn.read_until('\r\n')
        appdata_list = json.loads(appdata_list)
        if appdata_list['state'] != 'success':
            return out

        appdata_list = appdata_list['data']
        for appdata in appdata_list:
            appdata['name'] = '%s_%d' % (cluster_name, appdata['backup_id'])
            appdata['cluster_name'] = cluster_name
            appdata['type'] = appdata['type'].encode('ascii')
            appdata['period'] = appdata['period'].encode('ascii')
            appdata['holding_period'] = int(appdata['holding_period'].encode('ascii'))
            appdata['base_time'] = appdata['base_time'].encode('ascii')
            appdata['net_limit'] = int(appdata['net_limit'].encode('ascii'))
            appdata['output_format'] = appdata['output_format'].encode('ascii')
            appdata['service_url'] = appdata['service_url'].encode('ascii')

        out.appdata_list = appdata_list
        return out
    except IOError as e:
        warn(red(e))
        out.cm = False
        return out 

