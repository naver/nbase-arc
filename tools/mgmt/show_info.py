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

import telnetlib
import json
import time
import logging
import logging.handlers
import threading
import sys
from fabric.api import *
from fabric.colors import *
from fabric.contrib.console import *
from fabric.contrib.files import *
from gw_cmd import *
import remote
import util
import cm
import string

config = None

CLUSTER_PARTITION = '+--------------------+'
CLUSTER_COLUMN =    '|    CLUSTER NAME    |'
CLUSTER_FORMAT =    '| %18s |'
CLUSTER_HEADER = CLUSTER_PARTITION + '\n' + CLUSTER_COLUMN  + '\n' + CLUSTER_PARTITION

GW_PARTITION = '+-------+----------------+-------+-------+-----------+'
GW_COLUMN =    '| GW_ID |       IP       |  PORT | STATE | CLNT_CONN |'
GW_FORMAT =    '| %(gw_id)5d |%(pm_IP)15s |%(port)6d |  %(state)s(%(active_state)s) | %(clnt_conn)9d |'
GW_HEADER = GW_PARTITION + '\n' + GW_COLUMN  + '\n' + GW_PARTITION

PG_PARTITION = '+-------+--------------------+------+'
PG_COLUMN =    '| PG_ID |        SLOT        | MGEN |'
PG_FORMAT =    '| %(pg_id)5d | %(slot)18s | %(master_Gen)4s |'
PG_HEADER = PG_PARTITION + '\n' + PG_COLUMN  + '\n' + PG_PARTITION
PG_PREFIX = '|%7s|%20s|%6s|' % ('', '', '')

PGS_PARTITION = '+--------+------+----------------+-------+------+--------+--------+'
PGS_COLUMN =    '| PGS_ID | MGEN |       IP       |  PORT | ROLE | MEMLOG | QUORUM |'
PGS_FORMAT =    '| %(pgs_id)6d | %(master_Gen)4s |%(ip)15s |%(smr_base_port)6d | %(active_role)s(%(smr_role)s) | %(memlog)6s | %(quorum)6s |'
PGS_HEADER = PGS_PARTITION + '\n' + PGS_COLUMN  + '\n' + PGS_PARTITION

PG_PGS_PARTITION = PG_PARTITION[:-1] + PGS_PARTITION
PG_PGS_HEADER = PG_PARTITION[:-1] + PGS_PARTITION + '\n' + PG_COLUMN[:-1] + PGS_COLUMN + '\n' + PG_PARTITION[:-1] + PGS_PARTITION

CHECK_MEMLOG_PARTITION = '+--------+-----------+'
CHECK_MEMLOG_COLUMN = '| OPTION | UNMATCHED |'
CHECK_MEMLOG_FORMAT = '| %6s | %9s |'
CHECK_MEMLOG_HEADER = CHECK_MEMLOG_PARTITION + '\n' + CHECK_MEMLOG_COLUMN  + '\n' + CHECK_MEMLOG_PARTITION 

def set_config(config_module):
    global config
    config = config_module

def prompt_show_memlog():
    s = prompt(cyan("\nInput SHOW_MEMLOG(y/N)"))
    if len(s) > 0 and string.lower(s) == 'y':
        return True
    else:
        return False

def main(args):
    args = args.encode('ascii').split(' ')

    if len(args) > 1:
        cluster_name = args[1]
        menu_select_info(cluster_name)
    else:
        menu_show_clusters()

def menu_show_clusters():
    json_data = cm.cluster_ls()
    if json_data == None: return
    
    cluster_name_list = json_data['data']['list']

    if len(cluster_name_list) == 0:
        print yellow('There is no cluster.')
        return

    cluster_name_list.sort()

    while True:
        # Get max length of cluster names
        max_len = 0
        for i in range(len(cluster_name_list)):
            if max_len < len(cluster_name_list[i]):
                max_len = len(cluster_name_list[i])
        fmt = '%%3d. %%-%ds   ' % max_len

        # Show cluster list
        COLUMN_CNT = 3
        line_no = len(cluster_name_list) / COLUMN_CNT
        remainder = len(cluster_name_list) % COLUMN_CNT 
        if remainder != 0:
            line_no += 1

        lines = []
        for i in range(line_no):
            lines.append('')

        print yellow('  0. all')
        item = 0
        for i in range(min(COLUMN_CNT, len(cluster_name_list))):
            line_to = line_no
            if remainder != 0 and remainder < (i + 1):
                line_to -= 1

            for j in range(line_to):
                lines[j] += yellow(fmt % (item+1, cluster_name_list[item]))
                item += 1

        for line in lines:
            print line

        print yellow('  x. Up')

        # Select a cluster
        print cyan("Select a cluster")
        try:
            s = prompt(">>")
            if s.lower() == 'x':
                break
            menu_no = int(s)

        except:
            try:
                warn(red('Invalid input', menu_no))
            except:
                pass
            continue

        if menu_no < 0 or menu_no > len(cluster_name_list):
            warn(red('Invalid menu'))
            continue

        if menu_no == 0:
            memlog = prompt_show_memlog()
            for i in range(len(cluster_name_list)):
                cluster_name = cluster_name_list[i].encode('ascii')
                print cyan(' #### ' + cluster_name + ' #### ')
                show_all(cluster_name, memlog)
            continue
    
        menu_select_info(cluster_name_list[menu_no-1].encode('ascii'))

def menu_select_info(cluster_name):
    menu = [
                ["All", menu_show_all],
                ["GW list", menu_show_gw_list],
                ["PG list", menu_show_pg_list],
                ["PG > PGS list", menu_show_pgs_list],
                ["PGS", menu_show_pgs],
                ["Check memlog option", menu_check_memlog_option],
           ]

    while True:
        print "======================================="
        for i in range(0, len(menu)):
            print yellow("%d. %s" % (i + 1, menu[i][0]))
        print yellow("x. Up")
        print "======================================="

        try:
            s = prompt(">>")
            if s.lower() == 'x':
                break

            menu_no = int(s)

        except:
            try:
                warn(red('Invalid input', menu_no))
            except:
                pass
            continue

        if menu_no < 1 or menu_no > len(menu):
               warn(red('Invalid menu'))
               continue
            
        menu[menu_no - 1][1](cluster_name)

def show_cluster_list(cluster_name_list):
    print yellow(CLUSTER_HEADER)
    for cluster_name in cluster_name_list:
        print yellow(CLUSTER_FORMAT % cluster_name)
    print yellow(CLUSTER_PARTITION)

def show_opt(dick_name, d):
    for k, v in d.items():
        print yellow(OPT_FORMAT % (dick_name, k, str(v)))

def menu_show_all(cluster_name):
    memlog = prompt_show_memlog()
    show_all(cluster_name, memlog)

def show_all(cluster_name, memlog):
    # Check cluster
    cluster_json = cm.cluster_info(cluster_name)
    if cluster_json == None: return

    print ''
    if show_gw_list(cluster_name) == False:
        prompt("Press <Enter> to continue...")
    print ''

    pgs_list = util.get_all_pgs_info(cluster_name, memlog)

    print yellow(PG_PGS_HEADER)
    for pg in sorted(cluster_json['data']['pg_list'], key=lambda x: int(x['pg_id'])):
        print_pgs_with_pg(cluster_json, int(pg['pg_id']),
                filter(lambda pgs: pgs['pgs_id'] in pg['pg_data']['pgs_ID_List'], pgs_list))
        print yellow(PG_PARTITION[:-1] + PGS_PARTITION)
    print ''

def pgs_view_style(pgs):
    if pgs['active_role'] == 'M':
        bold = True
    else:
        bold = False

    if pgs['active_role'] != pgs['smr_role']:
        color_function = magenta
        abnormal = True
    elif pgs['active_role'] == 'M' or pgs['active_role'] == 'S':
        color_function = yellow
        abnormal = False
    else:
        color_function = red
        abnormal = True

    return bold, color_function, abnormal

def print_pgs_with_pg(cluster_json, pg_id, pgs_list, skip_warn=False):
    # get specific pg info
    pg = cm.pg_info(cluster_json['data']['cluster_name'], pg_id, cluster_json)

    begin = True
    warn = False
    for pgs in sorted(pgs_list, key=lambda x: int(x['pgs_id'])):
        bold, color_function, abnormal = pgs_view_style(pgs)

        if abnormal:
            warn = True

        if begin:
            prefix_pg = PG_FORMAT[:-1] % pg
            begin = False
        else:
            prefix_pg = PG_PREFIX[:-1]

        print yellow(prefix_pg[:-1]), color_function(PGS_FORMAT % pgs, bold)

    if not skip_warn and warn:        
        prompt("Press <Enter> to continue...")

def menu_show_pg_list(cluster_name):
    # Check cluster
    json_data = cm.cluster_info(cluster_name)
    if json_data == None: return

    pg_infos = []

    pg_ids = []
    for pg in json_data['data']['pg_list']:
        pg_ids.append(pg['pg_id'])
        
    pg_infos = cm.pg_infos(cluster_name, pg_ids)

    print yellow(PG_HEADER)
    for pg_info in pg_infos:
        print yellow(PG_FORMAT % pg_info)
    print yellow(PG_PARTITION)

def menu_show_gw_list(cluster_name):
    if show_gw_list(cluster_name) == False:
        prompt("Press <Enter> to continue...")

def show_gw_list(cluster_name):
    gw_list = cm.get_gw_list(cluster_name)  

    print yellow(GW_HEADER)

    if None == gw_list:
        print yellow('| %-37s |' % 'There is no GATEWAY.')
        print yellow(GW_PARTITION)
        return True

    gw_state = True
    for gw_id, gw_data in sorted(gw_list.items(), key=lambda x: int(x[0])):
        gw_data['gw_id'] = gw_id
        
        gw_data['active_state'] = util.get_gw_state(gw_data['pm_IP'], gw_data['port'], verbose=False)

        try:
            with GwCmd(gw_data['pm_IP'], gw_data['port']) as gw_cmd:
                gw_data['clnt_conn'] = gw_cmd.info_num_of_clients()
        except:
            gw_data['clnt_conn'] = 0

        if gw_data['state'] == 'N' and gw_data['active_state'] == 'N':
            print yellow(GW_FORMAT % gw_data)
        else:
            print red(GW_FORMAT % gw_data)
            gw_state = False

    print yellow(GW_PARTITION)
    return gw_state

def menu_show_pgs_list(cluster_name):
    # Check cluster
    json_data = cm.cluster_info(cluster_name)
    if json_data == None: return

    pg_ids = []
    for pg_json in json_data['data']['pg_list']:
        pg_ids.append(int(pg_json['pg_id']))

    if len(pg_ids) == 0:
        print yellow('There is no PGS.')
        return

    # Show PG list
    pg_ids.sort()
    for i in range(len(pg_ids)):
        if i != 0 and i % 16 == 0:
            print ''
        sys.stdout.write(yellow('%4d' % pg_ids[i]))

    # Select PG
    s = prompt(cyan('\nInput PG_ID(number) SHOW_MEMLOG(y/N)'))
    try:
        toks = s.split(' ')
        pg_id = int(toks[0])
        if len(toks) == 2 and string.lower(toks[1]) == 'y':
            memlog = True
        else:
            memlog = False

        show_pgs_list(cluster_name, pg_id, True, json_data, memlog)
    except:
        pass

"""
Show information of PGSes

Args:
    cluster_name : cluster name
    pg_id : partition group id
    print_header : When it is True, this function  prints the table header(PGS_COLUMN).
    cluster_json : It is cached data for cluster_info. 
                   When cluster_json is None, pg_info() get cluster_info from MGMT-CC

Returns:
    False if there is an error, otherwise show_pgs_list() returns True.
"""
def show_pgs_list(cluster_name, pg_id, print_header, cluster_json=None, memlog=False, skip_warn=False):
    # Check cluster
    if cluster_json == None:
        cluster_json = cm.cluster_info(cluster_name)
        if cluster_json == None:
            return False

    pg = filter(lambda pg: int(pg['pg_id']) == pg_id, cluster_json['data']['pg_list'])
    if len(pg) == 0:
        warn(red("PG '%d' doesn't exist." % pg_id))
        return True
    pg = pg[0]

    # Get pgs info
    pgs_list = util.get_deployed_pgs_info(cluster_name, pg['pg_data']['pgs_ID_List'], memlog)

    # Print
    print yellow(PG_PGS_HEADER)
    print_pgs_with_pg(cluster_json, pg_id, pgs_list, skip_warn=skip_warn)
    print yellow(PG_PGS_PARTITION)
    print ''

def menu_show_pgs(cluster_name):
    # Check cluster
    json_data = cm.cluster_info(cluster_name)
    if json_data == None: return

    pgs_ids = []
    for pg_json in json_data['data']['pg_list']:
        for pgs_id in pg_json['pg_data']['pgs_ID_List']:
            pgs_ids.append(pgs_id)

    if len(pgs_ids) == 0:
        print yellow('There is no PGS.')
        return

    # Show PGS list
    pgs_ids.sort()
    for i in range(len(pgs_ids)):
        if i != 0 and i % 16 == 0:
            print ''
        sys.stdout.write(yellow('%4d' % pgs_ids[i]))

    # Select PGS
    s = prompt(cyan('\nInput PGS_ID(number) SHOW_MEMLOG(y/N)'))
    try:
        toks = s.split(' ')
        pgs_id = int(toks[0])
        if len(toks) == 2 and string.lower(toks[1]) == 'y':
            memlog = True
        else:
            memlog = False
        show_pgs(json_data, pgs_id, memlog)
    except:
        warn(red("Failed to show pgs info. %s" % sys.exc_info()[0]))

def show_pgs(cluster_json, pgs_id, memlog=False):
    # Get pgs information
    pgs_unit_set = util.get_deployed_pgs_info(cluster_json['data']['cluster_name'], [pgs_id], memlog)
    if pgs_unit_set == None:
        warn(red("PGS '%d' doesn't exist." % pgs_id))
        return None

    print yellow(PG_PGS_HEADER)
    print_pgs_with_pg(cluster_json, int(pgs_unit_set[0]['pg_id']), pgs_unit_set)
    print yellow(PG_PGS_PARTITION)
    print ''

def menu_check_memlog_option(cluster_name):
    opt_memlog = config.get_cluster_opt(cluster_name)('smr')('use_memlog').v()
    pgs_readers = []

    pgs_list = util.get_all_pgs_info(cluster_name, True)
    unmatched_list = filter(lambda pgs : pgs['memlog'] != opt_memlog, pgs_list)

    # Print
    print yellow(CHECK_MEMLOG_HEADER)
    if len(unmatched_list) != 0:
        print(red(CHECK_MEMLOG_FORMAT % (opt_memlog, len(unmatched_list))))
    else:
        print(yellow(CHECK_MEMLOG_FORMAT % (opt_memlog, len(unmatched_list))))
    print(yellow(CHECK_MEMLOG_PARTITION + '\n'))

    if len(unmatched_list) > 0:
        print yellow(PGS_HEADER)
        for pgs in unmatched_list:
            bold, color_function, abnormal = pgs_view_style(pgs)
            if pgs['memlog'] != opt_memlog:
                print color_function(PGS_FORMAT % pgs, bold)
        print yellow(PGS_PARTITION + '\n')

