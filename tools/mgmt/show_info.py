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

PGS_PARTITION = '+--------+------+----------------+-------+------+--------+'
PGS_COLUMN =    '| PGS_ID | MGEN |       IP       |  PORT | ROLE | QUORUM |'
PGS_FORMAT =    '| %(pgs_id)6d | %(master_Gen)4s |%(ip)15s |%(smr_base_port)6d | %(active_role)s(%(smr_role)s) | %(quorum)6s |'
PGS_HEADER = PGS_PARTITION + '\n' + PGS_COLUMN  + '\n' + PGS_PARTITION

def set_config(config_module):
    global config
    config = config_module

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
            for i in range(len(cluster_name_list)):
                cluster_name = cluster_name_list[i].encode('ascii')
                print cyan(' #### ' + cluster_name + ' #### ')
                menu_show_all(cluster_name)
            continue
    
        menu_select_info(cluster_name_list[menu_no-1].encode('ascii'))

def menu_select_info(cluster_name):
    menu = [
                ["All", menu_show_all],
                ["GW list", menu_show_gw_list],
                #["Cluster > GW", menu_show_gw],
                ["PG list", menu_show_pg_list],
                #["Cluster > PG", menu_show_pg],
                ["PG > PGS list", menu_show_pgs_list],
                ["PGS", menu_show_pgs],
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

def menu_show_all(cluster_name):
    # Check cluster
    json_data = cm.cluster_info(cluster_name)
    if json_data == None: return

    print ''
    if show_gw_list(cluster_name) == False:
        prompt("Press <Enter> to continue...")
    print ''

    header = True
    for pg in sorted(json_data['data']['pg_list'], key=lambda x: int(x['pg_id'])):
        if show_pgs_list(cluster_name, int(pg['pg_id']), header, json_data) == False:
            prompt("Press <Enter> to continue...")
        header = False
    print ''

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
    s = prompt(cyan('\nSelect PG number'))

    try:
        pg_id = int(s)
        if show_pgs_list(cluster_name, pg_id, True, json_data) == False:
            prompt("Press <Enter> to continue...")
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
def show_pgs_list(cluster_name, pg_id, print_header, cluster_json=None):
    # Check cluster
    if cluster_json == None:
        cluster_json = cm.cluster_info(cluster_name)
        if cluster_json == None:
            return False

    exist = [k for loop_pg_data in cluster_json['data']['pg_list'] 
                    for k, v in loop_pg_data.iteritems() if k == 'pg_id' and int(v) == pg_id]
    if len(exist) == 0:
        warn(red("PG '%d' doesn't exist." % pg_id))
        return True

    # get specific pg info
    pg = cm.pg_info(cluster_name, pg_id, cluster_json)

    if print_header:
        print yellow(PG_PARTITION[:-1] + PGS_PARTITION)
        print yellow(PG_COLUMN[:-1] + PGS_COLUMN)
        print yellow(PG_PARTITION[:-1] + PGS_PARTITION)

    # get all pgs info
    pgs_list = cm.get_pgs_list(cluster_name, pg_id, cluster_json)  
    if None == pgs_list:
        print yellow('| %-57s |' % ("There is no PGS in PG '%d'." % pg_id))
        print yellow(PG_PARTITION[:-1] + PGS_PARTITION)
        return True

    for id, data in pgs_list.items():
        data['active_role'] = util.get_role_of_smr(data['ip'], data['mgmt_port'], verbose=False)
        if data['active_role'] == 'M':
            data['quorum'] = remote.get_quorum(data)
        else:
            data['quorum'] = ''

    pgs_state = True
    begin = True
    for pgs_id, pgs in sorted(pgs_list.items(), key=lambda x: int(x[0])):
        if pgs['active_role'] == 'M':
            bold = True
        else:
            bold = False

        if begin:
            prefix_pg = PG_FORMAT[:-1] % pg
            begin = False
        else:
            prefix_pg = PG_PREFIX[:-1]

        if pgs['active_role'] != pgs['smr_role']:
            print yellow(prefix_pg[:-1]), magenta(PGS_FORMAT % pgs)
            pgs_state = False
        elif pgs['active_role'] == 'M' or pgs['active_role'] == 'S':
            print yellow(prefix_pg[:-1]), yellow(PGS_FORMAT % pgs, bold)
        else:
            print yellow(prefix_pg[:-1]), red(PGS_FORMAT % pgs)
            pgs_state = False
            
    print yellow(PG_PARTITION[:-1] + PGS_PARTITION)
    return pgs_state

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
    s = prompt(cyan('\nSelect PGS number'))
    try:
        pgs_id = int(s.split(' ')[0])
        show_pgs(cluster_name, pgs_id)
    except:
        pass

def show_pgs(cluster_name, pgs_id):
    pgs = cm.pgs_info(cluster_name, pgs_id)
    if pgs == None:
        warn(red("PGS '%d' doesn't exist." % pgs_id))
        return

    pgs = pgs['data']
    pgs['active_role'] = util.get_role_of_smr(pgs['ip'], pgs['mgmt_port'], verbose=False)
    if pgs['active_role'] == 'M':
        pgs['quorum'] = remote.get_quorum(pgs)
    else:
        pgs['quorum'] = ''

    pg = cm.pg_info(cluster_name, pgs['pg_ID'])
    if pg == None:
        warn(red("PG '%d' doesn't exist." % pgs['pg_ID']))
        return

    if pgs['active_role'] == 'M': 
        bold = True
    else: 
        bold = False

    prefix_pg = PG_FORMAT[:-1] % pg

    # Print
    print yellow(PG_PARTITION[:-1] + PGS_PARTITION)
    print yellow(PG_COLUMN[:-1] + PGS_COLUMN)
    print yellow(PG_PARTITION[:-1] + PGS_PARTITION)

    if pgs['active_role'] != pgs['smr_role']:
        print yellow(prefix_pg[:-1]), magenta(PGS_FORMAT % pgs)
    elif pgs['active_role'] == 'M' or pgs['active_role'] == 'S':
        print yellow(prefix_pg[:-1]), yellow(PGS_FORMAT % pgs, bold)
    else:
        print yellow(prefix_pg[:-1]), red(PGS_FORMAT % pgs)
            
    print yellow(PG_PARTITION[:-1] + PGS_PARTITION)
