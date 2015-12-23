import constant as c
import config
import time
import os
import util
import smr_mgmt
import json


def setup_binaries( clusters, skip_copy_binaries, opt_32bit_binary_test ):
    if not os.path.exists( c.homedir ):
        os.mkdir( c.homedir )
    if not os.path.exists( c.logdir ):
        os.mkdir( c.logdir )

    # resource caching/skip structures
    server_id_dict = {} # boolean value
    if skip_copy_binaries is False:
        for cluster in clusters:
            for server in cluster['servers']:
                id = server['id']
                if server_id_dict.has_key(id):
                    continue
                server_id_dict[id] = True

                if copy_binaries(id, opt_32bit_binary_test) is not 0:
                    util.log('failed to copy_binaries, server_id: %d' % server['id'])
                    return -1
    util.log('initialize done')
    return 0


def cleanup_zookeeper_root():
    max_try = 20
    for i in range( max_try ):
        print 'try cleanup zookeeper, i:%d' % i
        ret = util.zk_cmd( 'rmr /RC' )
        err = ret['err']
        if err == '':
            return 0
        elif -1 != err.find( 'Node does not exist: /RC'):
            if err.count('/') == 1:
                return 0
        else:
            util.log('failed to clean up zookeeper, err:%s' % err)
        time.sleep(0.2)
    return -1


def cleanup_processes():
    ret = util.killps_y( 'smr-replicator' )
    if ret is not 0:
        util.log( 'failed to kill smr' )
        return -1

    ret = util.killps_y( 'redis-arc' )
    if ret is not 0:
        util.log( 'failed to kill redis-arc' )
        return -1

    ret = util.killps_y( 'redis-gateway' )
    if ret is not 0:
        util.log( 'failed to kill redis-gateway' )
        return -1

    ret = util.killps_y( 'confmaster-' )
    if ret is not 0:
        util.log( 'failed to kill confmaster' )
        return -1

    return 0


def cleanup_pgs_log_and_ckpt( cluster_name, server ):
    ret = util.delete_smr_log_dir( server['id'], server['smr_base_port'] )
    if ret is not 0:
        util.log('failed to delete smr log dir')
        return -1

    ret = util.delete_redis_check_point( server['id'] )
    if ret is not 0:
        util.log('failed to delete redis check point')
        return -1

    return 0


def request_to_start_cm( id, port ):
    ret = util.start_confmaster( id, port )
    if ret is not 0:
        util.log('failed to start_confmaster. id:%d, port:%d' % (id, port))
        return -1
    else:
        util.log('succeeded to start_confmaster. id:%d, port:%d' % (id, port))

    return 0


def initialize_cluster( cluster, leader_cm=None ):
    if leader_cm == None:
        leader_cm = cluster['servers'][0]
    servers = cluster['servers']

    initialize_physical_machine_znodes( leader_cm['ip'], leader_cm['cm_port'], config.machines )

    cmd = 'cluster_add %s %s' % (cluster['cluster_name'], cluster['quorum_policy'])
    result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd  )
    jobj = json.loads(result)
    if jobj['state'] != 'success':
        util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
        return -1

    slot_no = 0
    for pg_id in cluster['pg_id_list']:
        cmd = 'pg_add %s %d' % (cluster['cluster_name'], pg_id)
        result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
        jobj = json.loads(result)
        if jobj['state'] != 'success':
            util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
            return -1

        if cluster['slots'][slot_no] != -1:
            cmd = 'slot_set_pg %s %d:%d %d' % (cluster['cluster_name'], cluster['slots'][slot_no], cluster['slots'][slot_no+1], pg_id)
            result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
            jobj = json.loads(result)
            if jobj['state'] != 'success':
                util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
                return -1

        slot_no = slot_no + 2

    for server in servers:
        initialize_info_of_cm_about_pgs( cluster, server, leader_cm )

    return 0


def initialize_info_of_cm_about_pgs( cluster, server, leader_cm, pg_id=None ):
    if pg_id == None:
        pg_id = server['pg_id']

    cmd = 'pgs_add %s %d %d %s %s %d %d' % (cluster['cluster_name'], server['id'], pg_id, server['pm_name'], server['ip'], server['smr_base_port'], server['redis_port'])
    result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
    jobj = json.loads(result)
    if jobj['state'] != 'success':
        util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
        return -1

    cmd = 'pgs_join %s %d' % (cluster['cluster_name'], server['id'])
    result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
    try:
        jobj = json.loads(result)
        if jobj['state'] != 'success':
            util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
            return -1
    except json.ValueError:
        util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
        return -1
    return 0


def finalize_info_of_cm_about_pgs( cluster, server, leader_cm ):
    cmd = 'pgs_leave %s %d' % (cluster['cluster_name'], server['id'])
    result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
    jobj = json.loads(result)
    if jobj['state'] != 'success':
        util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
        return -1
    time.sleep( 3 )

    cmd = 'pgs_del %s %d' % (cluster['cluster_name'], server['id'])
    result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
    jobj = json.loads(result)
    if jobj['state'] != 'success':
        util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
        return -1
    time.sleep( 3 )
    return 0


def initialize_physical_machine_znodes( ip, port, pm_list ):
    for pm in pm_list:
        if pm['type'] == 'virtual':
            pm_ip = pm['virtual_ip']
        else:
            pm_ip = pm['ip']

        cmd = 'pm_add %s %s' % (pm['name'], pm_ip)
        util.log('ip:%s, port:%d, cmd:%s' % (ip, port, cmd))
        result = util.cm_command( ip, port, cmd )
        jobj = json.loads(result)
        if jobj['state'] != 'success':
            util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
            return -1
    return 0


def add_physical_machine_to_mgmt( mgmt_ip, mgmt_port, pm_name, pm_ip ):
    cmd = 'pm_add %s %s' % (pm_name, pm_ip)
    result = util.cm_command( mgmt_ip, mgmt_port, cmd )
    jobj = json.loads(result)
    if jobj['state'] != 'success':
        util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
        return -1
    return 0


def request_to_start_all_servers( cluster_name, server ):
    time.sleep( 1 )

    if request_to_start_smr( server ) is not 0:
        return -1

    if request_to_start_redis( server ) is not 0:
        return -1

    return 0


def request_to_start_gateway( cluster_name, server, leader_cm, check_state=True ):
    # add gateway configuration to confmaster
    cmd = 'gw_add %s %d %s %s %d' % (cluster_name, server['id'], server['pm_name'], server['ip'], server['gateway_port'])
    result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
    jobj = json.loads(result)
    if jobj['state'] != 'success':
        util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
        return -1

    # start gateway process
    ret = util.start_gateway( server['id'], server['ip'], leader_cm['cm_port'], cluster_name, server['gateway_port'] )
    if ret is not 0:
        util.log('failed to start_gateway. server:%s, id:%d' % (server['ip'], server['id']))
        return -1

    # check gateway state
    if check_state:
        ok = False
        try_cnt = 0
        while try_cnt < 5:
            try_cnt += 1
            if util.check_gateway_state( cluster_name, leader_cm, server ):
                ok = True
                break
            time.sleep(0.5)

        if not ok:
            util.log('failed to start_gateway, Invalid state of gateway.' % inactive_conns)
            return -1

    # check inactive redis connections of gateway
    if check_state:
        ok = False
        try_cnt = 0
        while try_cnt < 10:
            try_cnt += 1
            if util.gw_info_redis_disccons(server['ip'], server['gateway_port']) == 0:
                ok = True
                break
            else:
                time.sleep(0.5)

        if not ok:
            util.log('failed to start_gateway, invalid number of inactive redis connections. %d' % inactive_conns)
            return -1

    util.log('succeeded to start_gateway. server:%s, id:%d' % (server['ip'], server['id']))
    return 0


def wait_until_finished_to_set_up_role( server, max_try = 20 ):
    smr = smr_mgmt.SMR( server['id'] )
    if smr.connect( server['ip'], server['smr_mgmt_port'] ) is not 0:
        util.log('wait_until_finished_to_set_up_rol(), eserver%d(%s:%d) is stopped.' % (server['id'], server['ip'], server['smr_mgmt_port']))
        return -1

    try_count = 0
    for try_count in range( 0, max_try ):
        time.sleep( 0.2 )

        smr.write( 'ping\r\n' )
        response = smr.read_until( '\r\n' )
        data = str.split( response, ' ' )
        role = data[1]
        if role == c.ROLE_MASTER or role == c.ROLE_SLAVE:
            return 0

    util.log('failed to wait until finished to set up a role for %s:%d' % (server['ip'], server['smr_mgmt_port']))
    return -1


def kill_all_processes( server ):
    ret = util.kill_all_processes()
    if ret is not 0:
        util.log('failed to kill_all_processes')
        return -1
    return 0


def request_to_shutdown_smr( server ):
    if server is None:
        util.log('request_to_shutdown_smr::server is None')
        return -1

    id = server['id']
    ip = server['ip']
    smr_base_port = server['smr_base_port']

    if util.shutdown_smr( id, ip, smr_base_port ) is not 0:
        util.log('failed to shutdown_smr %d' % (id))
        return -1
    util.log('succeeded to shutdown_smr. server:%d' % (id))
    return 0


def request_to_shutdown_redis( server ):
    id = server['id']
    redis_port = server['redis_port']

    if util.shutdown_redis( id, redis_port ) is not 0:
        util.log('failed to shutdown_redis %d' % (id))
        return -1
    util.log('succeeded to shutdown_redis. server:%d' % (id))
    return 0


def request_to_shutdown_cm( server ):
    id = server['id']

    if util.shutdown_cm( id ) is not 0:
        util.log('failed to shutdown_cm%d' % (id))
        return -1
    return 0


def request_to_shutdown_gateway( cluster_name, server, leader_cm, check=False ):
    ip = server['ip']
    port = server['gateway_port']
    id = server['id']

    # delete gateway configuration from confmaster
    cmd = 'gw_del %s %d' % (cluster_name, id)
    result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
    jobj = json.loads(result)
    if jobj['state'] != 'success':
        util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
        return -1

    # check client connection
    ok = False
    for i in range(10):
        client_conn_num = util.gw_info_client_cnt(ip, port)
        if client_conn_num == 1:
            ok = True
            break
        else:
            time.sleep(1)

    if ok == False:
        util.log('failed to shutdown_gateway, invalid number of client connections. %d' % (client_conn_num - 1))
        return -1

    # shutdown gateway process
    if util.shutdown_gateway( id, port ) is not 0:
        util.log('failed to shutdown_gateway %d' % (id))
        return -1
    util.log('succeeded to shutdown_gateway. %d' % (id))
    return 0


def request_to_start_smr( server, verbose=3, log_delete_delay=86400 ):
    id = server['id']
    ip = server['ip']
    smr_base_port = server['smr_base_port']
    smr_mgmt_port = server['smr_mgmt_port']

    ret = util.start_smr_replicator( id, ip, smr_base_port, smr_mgmt_port, verbose, True, log_delete_delay )
    if ret is not 0:
        util.log('failed to start_smr_replicator. server:%d' % (id))
        return -1
    else:
        util.log('succeeded to start_smr_replicator. server:%d' % (id))
    return 0


def request_to_start_redis( server, check=True, max_try=30 ):
    id = server['id']
    ip = server['ip']
    smr_base_port = server['smr_base_port']
    redis_port = server['redis_port']

    ret = util.start_redis_server( id, smr_base_port, redis_port, check, max_try )
    if ret is not 0:
        util.log('failed to start_redis_server. server:%d' % (id))
        return -1
    else:
        util.log('succeeded to start_redis_server. server:%d' % (id))
    return 0


def setup_cm( id ):
    try:
        util.copy_cm( id )

    except IOError as e:
        print e
        util.log('Error: can not find file or read data')
        return -1

    return 0


def copy_binaries( id, opt_32bit_binary_test ):
    try:
        util.copy_smrreplicator( id )
        util.copy_gw( id )
        util.copy_redis_server( id )
        util.copy_cluster_util( id )
        util.copy_dump_util( id )
        util.copy_dump_util_plugin( id )
        util.copy_log_util( id )
        util.copy_capi_so_file( id )
        util.copy_capi_test_server( id )
        util.copy_cm( id )

        if opt_32bit_binary_test:
            util.copy_capi32_so_file( id )
            util.copy_capi32_test_server( id )

    except IOError as e:
        print e
        util.log('Error: can not find file or read data')
        return -1

    return 0
