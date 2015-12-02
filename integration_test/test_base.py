import xmlrpclib
import constant as c
import config
import time
import os
import util
import smr_mgmt
import demjson
import pdb


resource_files = ( 
  'util.py',
  'constant.py',
  'process_mgmt.py',
  'smr_mgmt.py',
  'telnet.py',
  'xml_rpc_server.py',
  'redis_mgmt.py',
  'demjson.py'
)


def initialize( physical_machines, clusters, skip_copy_binaries ):
  for physical_machine in physical_machines:
    rpc = connect_to_update_server( physical_machine )
    physical_machine['rpc'] = rpc

  for cluster in clusters:
    for server in cluster['servers']:
      if setup_rpc( server ) is not 0:
        return -1

      if skip_copy_binaries is False:
        if send_binaries_to_testmachine( rpc, server ) is not 0:
          util.log('failed to send_binaries_to_testmachine, ip:%s, id:%d' % (server['ip'], server['id']))
          return -1
  return 0


def connect_to_update_server( server ):
  updater_rpc = _connect_to_update_server( server['ip'], server['update_port'] )

  if _update_rpc_resources( updater_rpc ) is not 0:
    return -1

  if _start_proxyserver_skel( server['ip'], server['proxy_port'], updater_rpc ) is not 0:
    return -1

  return _connect_to_rpc_server( server['ip'], server['proxy_port'] )


def _connect_to_update_server( ip, port ):
  connection_string = 'http://%s:%d' % (ip, port)
  print connection_string 
  updater_rpc = xmlrpclib.ServerProxy( connection_string )

  if updater_rpc.rpc_cleanup_old_proxyserver_skel() is not 0:
    util.log('failed to rpc_cleanup_old_proxyserver_skel')
    return None 
  return updater_rpc


def _update_rpc_resources( updater_rpc ):
  # update resource files
  for file_name in resource_files:
    if update_resource_file( updater_rpc, file_name ) is not 0:
      util.log('failed to update_resource file(%s)' % file_name)
      return -1
  return 0


def update_resource_file( updater_rpc, file_name ):
  resource_file = open( file_name , 'rb' )
  ret = updater_rpc.rpc_update( xmlrpclib.Binary( resource_file.read() ), file_name )
  if ret is not 0:
    util.log('failed to udpate %s' % (file_name))
    return -1
  return 0


def _start_proxyserver_skel( ip, proxy_port, updater_rpc ):
  # update proxy_impl
  f_proxy = open( './proxyserver_skel.py', 'r' )
  data = f_proxy.read()
  f_proxy.close()

  ret = updater_rpc.rpc_init_proxyserver_skel( ip, proxy_port, data );
  if ret is not 0:
    util.log('failed to init_proxyserver_skel.')
    return -1
  else:
    util.log('succeed to init_proxyserver_skel.' )

  return 0


def _connect_to_rpc_server( ip, port ):
  return xmlrpclib.ServerProxy( 'http://%s:%d' % (ip, port) )


def cleanup_zookeeper_root( server ):
  max_try = 20
  for i in range( max_try ):
    print 'try cleanup zookeeper, i:%d' % i
    ret = server['rpc'].rpc_zk_cmd( 'rmr /RC' )
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


def cleanup_test_environment( cluster_name, server ):

#  ret = server['rpc'].rpc_killps_y( 'mgmt-hbc' )
#    if ret is not 0:
#      util.log( 'failed to kill mgmt-hbc' )
#      return -1

  ret = server['rpc'].rpc_killps_y( 'smr-replicator' )
  if ret is not 0:
    util.log( 'failed to kill smr' )
    return -1

  ret = server['rpc'].rpc_killps_y( 'redis-arc' )
  if ret is not 0:
    util.log( 'failed to kill redis-arc' )
    return -1

  ret = server['rpc'].rpc_killps_y( 'redis-gateway' )
  if ret is not 0:
    util.log( 'failed to kill redis-gateway' )
    return -1

  ret = server['rpc'].rpc_killps_y( 'confmaster-' )
  if ret is not 0:
    util.log( 'failed to kill confmaster' )
    return -1

  ret = cleanup_pgs_log_and_ckpt( cluster_name, server )
  if ret is not 0:
    util.log( 'failed to cleanup_pgs_data' )
    return -1

  return 0


def cleanup_pgs_log_and_ckpt( cluster_name, server ):
  ret = server['rpc'].rpc_delete_smr_log_dir( server['id'], server['smr_base_port'] )
  if ret is not 0:
    util.log('failed to delete smr log dir')
    return -1

  ret = server['rpc'].rpc_delete_redis_check_point( server['id'] )
  if ret is not 0:
    util.log('failed to delete redis check point')
    return -1

  return 0


def request_to_start_cm( id, rpc, port ):
  ret = rpc.rpc_start_confmaster( id, port )
  if ret is not 0:
    util.log('failed to rpc_start_confmaster. id:%d, port:%d' % (id, port))
    return -1
  else:
    util.log('succeeded to rpc_start_confmaster. id:%d, port:%d' % (id, port))

  return 0


def initialize_cluster( cluster, leader_cm=None ):
  if leader_cm == None:
    leader_cm = cluster['servers'][0]
  servers = cluster['servers']

  initialize_physical_machine_znodes( leader_cm['ip'], leader_cm['cm_port'], config.physical_machines )

  cmd = 'cluster_add %s %s' % (cluster['cluster_name'], cluster['quorum_policy'])
  result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd  )
  json = demjson.decode(result)
  if json['state'] != 'success':
    util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
    return -1

  slot_no = 0
  for pg_id in cluster['pg_id_list']:
    cmd = 'pg_add %s %d' % (cluster['cluster_name'], pg_id)
    result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
    json = demjson.decode(result)
    if json['state'] != 'success':
      util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
      return -1

    if cluster['slots'][slot_no] != -1:
      cmd = 'slot_set_pg %s %d:%d %d' % (cluster['cluster_name'], cluster['slots'][slot_no], cluster['slots'][slot_no+1], pg_id)
      result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
      json = demjson.decode(result)
      if json['state'] != 'success':
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
  json = demjson.decode(result)
  if json['state'] != 'success':
    util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
    return -1
  
  cmd = 'pgs_join %s %d' % (cluster['cluster_name'], server['id'])
  result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
  try:
    json = demjson.decode(result)
    if json['state'] != 'success':
      util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
      return -1
  except demjson.JSONDecodeError:
    util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
    return -1
  return 0


def finalize_info_of_cm_about_pgs( cluster, server, leader_cm ):
  cmd = 'pgs_leave %s %d' % (cluster['cluster_name'], server['id'])
  result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
  json = demjson.decode(result)
  if json['state'] != 'success':
    util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
    return -1
  time.sleep( 3 )

  cmd = 'pgs_del %s %d' % (cluster['cluster_name'], server['id'])
  result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
  json = demjson.decode(result)
  if json['state'] != 'success':
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
    json = demjson.decode(result)
    if json['state'] != 'success':
      util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
      return -1
  return 0


def add_physical_machine_to_mgmt( mgmt_ip, mgmt_port, pm_name, pm_ip ):
  cmd = 'pm_add %s %s' % (pm_name, pm_ip)
  result = util.cm_command( mgmt_ip, mgmt_port, cmd )
  json = demjson.decode(result)
  if json['state'] != 'success':
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
  json = demjson.decode(result)
  if json['state'] != 'success':
    util.log('failed to execute. cmd:%s, result:%s' % (cmd, result))
    return -1
 
  # start gateway process
  ret = server['rpc'].rpc_start_gateway( server['id'], server['ip'], leader_cm['cm_port'], cluster_name, server['gateway_port'] )
  if ret is not 0:
    util.log('failed to rpc_start_gateway. server:%s, id:%d' % (server['ip'], server['id']))
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
      util.log('failed to rpc_start_gateway, Invalid state of gateway.' % inactive_conns)
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
      util.log('failed to rpc_start_gateway, invalid number of inactive redis connections. %d' % inactive_conns)
      return -1

  util.log('succeeded to rpc_start_gateway. server:%s, id:%d' % (server['ip'], server['id']))
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
  ret = server['rpc'].rpc_kill_all_processes()
  if ret is not 0:
    util.log('failed to rpc_kill_all_processes')
    return -1
  return 0


def request_to_shutdown_smr( server ):
  if server is None:
    util.log('request_to_shutdown_smr::server is None')
    return -1

  id = server['id']
  rpc = server['rpc']
  ip = server['ip']
  smr_base_port = server['smr_base_port']

  if rpc.rpc_shutdown_smr( id, ip, smr_base_port ) is not 0:
    util.log('failed to rpc_shutdown_smr %d' % (id))
    return -1
  util.log('succeeded to rpc_shutdown_smr. server:%d' % (id))
  return 0


def request_to_shutdown_redis( server ):
  id = server['id']
  rpc = server['rpc']
  redis_port = server['redis_port']

  if rpc.rpc_shutdown_redis( id, redis_port ) is not 0:
    util.log('failed to rpc_shutdown_redis %d' % (id))
    return -1
  util.log('succeeded to rpc_shutdown_redis. server:%d' % (id))
  return 0


def request_to_shutdown_cm( server ):
  id = server['id']
  rpc = server['rpc']

  if rpc.rpc_shutdown_cm( id ) is not 0:
    util.log('failed to rpc_shutdown_cm%d' % (id))
    return -1
  return 0


def request_to_shutdown_gateway( cluster_name, server, leader_cm, check=False ):
  ip = server['ip']
  port = server['gateway_port']
  id = server['id']
  rpc = server['rpc']

  # delete gateway configuration from confmaster
  cmd = 'gw_del %s %d' % (cluster_name, id)
  result = util.cm_command( leader_cm['ip'], leader_cm['cm_port'], cmd )
  json = demjson.decode(result)
  if json['state'] != 'success':
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
      util.log('failed to rpc_shutdown_gateway, invalid number of client connections. %d' % (client_conn_num - 1))
      return -1

  # shutdown gateway process
  if rpc.rpc_shutdown_gateway( id, port ) is not 0:
    util.log('failed to rpc_shutdown_gateway %d' % (id))
    return -1
  util.log('succeeded to rpc_shutdown_gateway. %d' % (id))
  return 0


def request_to_start_smr( server, verbose=3, log_delete_delay=86400 ):
  rpc = server['rpc']
  id = server['id']
  ip = server['ip']
  smr_base_port = server['smr_base_port']
  smr_mgmt_port = server['smr_mgmt_port']

  ret = rpc.rpc_start_smr_replicator( id, ip, smr_base_port, smr_mgmt_port, verbose, True, log_delete_delay )
  if ret is not 0:
    util.log('failed to rpc_start_smr_replicator. server:%d' % (id))
    return -1
  else:
    util.log('succeeded to rpc_start_smr_replicator. server:%d' % (id))
  return 0


def request_to_start_redis( server, check=True, max_try=30 ):
  id = server['id']
  rpc = server['rpc']
  ip = server['ip']
  smr_base_port = server['smr_base_port']
  redis_port = server['redis_port']

  ret = rpc.rpc_start_redis_server( id, smr_base_port, redis_port, check, max_try )
  if ret is not 0:
    util.log('failed to rpc_start_redis_server. server:%d' % (id))
    return -1
  else:
    util.log('succeeded to rpc_start_redis_server. server:%d' % (id))
  return 0


def setup_rpc( server ):
  for physical_machine in config.physical_machines:
    if server['pm_name'] == physical_machine['name']:
      server['rpc'] = physical_machine['rpc']
      return 0
  util.log( 'failed to set up rpc. server:%d, pm:%s' % (server['id'], server['pm_name']) )
  return -1


def send_cm( rpc, id ):
  try:
    path = '../confmaster/target/%s' % (c.CC)
    cc = open( path, 'rb' )

    path = '../confmaster/target/%s' % (c.CM_PROPERTY_FILE_NAME)
    cc_property = open( path, 'rb' )

    try:
      if rpc.rpc_copy_cm( xmlrpclib.Binary( cc.read() ), id ) is not 0:
        util.log('failed to copy confmaster')
        return -1
      if rpc.rpc_copy_cm_res( xmlrpclib.Binary( cc_property .read() ), c.CM_PROPERTY_FILE_NAME, id ) is not 0:
        util.log('failed to copy cc_exec_script')
        return -1

    finally:
      if cc is not None:
        cc.close()
      if cc_property is not None:
        cc_property.close()

  except IOError as e:
    print e
    util.log('Error: can not find file or read data')
    return -1
   
  try:
    path = '../confmaster/script/%s' % (c.CM_EXEC_SCRIPT)
    cc_exec_script = open( path, 'rb' )

    if rpc.rpc_copy_cm_res( xmlrpclib.Binary( cc_exec_script.read() ), c.CM_EXEC_SCRIPT, id ) is not 0:
      util.log('failed to copy cc_exec_script')
      return -1

  except IOError:
    util.log('Error: can not find file or read data')
    return -1
  finally:
    if cc_exec_script is not None:
      cc_exec_script.close()

  return 0


def send_binaries_to_testmachine( rpc, server ):
  id = server['id']
  try:

    path = '../smr/replicator/%s' % (c.SMR)
    smr = open( path, 'rb' )

    path = '../gateway/%s' % (c.GW)
    gw = open( path, 'rb' )

    path = '../redis-2.8.8/src/%s' % (c.REDIS)
    redis = open( path, 'rb' )

    path = '../redis-2.8.8/src/%s' % (c.CLUSTER_UTIL)
    cluster_util = open( path, 'rb' )

    path = '../redis-2.8.8/src/%s' % (c.DUMP_UTIL)
    dump_util = open( path, 'rb' )

    path = '../redis-2.8.8/src/%s' % (c.DUMP_UTIL_PLUGIN)
    dump_util_plugin = open( path, 'rb' )

    path = '../smr/smr/%s' % (c.LOG_UTIL)
    log_util = open( path, 'rb' )

    path = '../confmaster/target/%s' % (c.CC)
    cm = open( path, 'rb' )

    path = '../confmaster/target/%s' % (c.CM_PROPERTY_FILE_NAME)
    cm_property = open( path, 'rb' )

    path = '../api/arcci/.obj64/lib/%s' % (c.CAPI_SO_FILE)
    capi_so_file = open(path, 'rb')

    path = '../api/arcci/.obj32/lib/%s' % (c.CAPI_SO_FILE)
    capi32_so_file = open(path, 'rb')

    path = '../tools/local_proxy/.obj64/%s' % (c.CAPI_TEST_SERVER)
    capi_test_server = open(path, 'rb')

    path = '../tools/local_proxy/.obj32/%s' % (c.CAPI_TEST_SERVER)
    capi32_test_server = open(path, 'rb')

    try:
      if rpc.rpc_copy_smrreplicator( xmlrpclib.Binary( smr.read() ), id ) is not 0:
        util.log('failed to copy smr-replicator')
        return -1
      if rpc.rpc_copy_gw( xmlrpclib.Binary( gw.read() ), id ) is not 0:
        util.log('failed to copy gateway')
        return -1
      if rpc.rpc_copy_redis_server( xmlrpclib.Binary( redis.read() ), id ) is not 0:
        util.log('failed to copy redis-arc')
        return -1
      if rpc.rpc_copy_cluster_util( xmlrpclib.Binary( cluster_util.read() ), id ) is not 0:
        util.log('failed to copy cluster-util')
        return -1
      if rpc.rpc_copy_dump_util( xmlrpclib.Binary( dump_util.read() ), id ) is not 0:
        util.log('failed to copy dump-util')
        return -1
      if rpc.rpc_copy_dump_util_plugin( xmlrpclib.Binary( dump_util_plugin.read() ), id ) is not 0:
        util.log('failed to copy dump-util')
        return -1
      if rpc.rpc_copy_log_util( xmlrpclib.Binary( log_util.read() ), id ) is not 0:
        util.log('failed to copy logutil')
        return -1
      if rpc.rpc_copy_capi_so_file( xmlrpclib.Binary( capi_so_file.read() ), id ) is not 0:
        util.log('failed to copy capi so file')
        return -1
      if rpc.rpc_copy_capi32_so_file( xmlrpclib.Binary( capi32_so_file.read() ), id ) is not 0:
        util.log('failed to copy capi so file')
        return -1
      if rpc.rpc_copy_capi_test_server( xmlrpclib.Binary( capi_test_server.read() ), id ) is not 0:
        util.log('failed to copy dump-util')
        return -1
      if rpc.rpc_copy_capi32_test_server( xmlrpclib.Binary( capi32_test_server.read() ), id ) is not 0:
        util.log('failed to copy dump-util')
        return -1
      if rpc.rpc_copy_cm( xmlrpclib.Binary( cm.read() ), id ) is not 0:
        util.log('failed to copy confmaster')
        return -1
      if rpc.rpc_copy_cm_res( xmlrpclib.Binary( cm_property.read() ), c.CM_PROPERTY_FILE_NAME, id ) is not 0:
        util.log('failed to copy cm_property')
        return -1

    finally:
      if smr is not None:
        smr.close()
      if gw is not None:
        gw.close()
      if redis is not None:
        redis.close()
      if cluster_util is not None:
        cluster_util.close()
      if dump_util is not None:
        dump_util.close()
      if dump_util_plugin is not None:
        dump_util_plugin.close()
      if log_util is not None:
        log_util.close()
      if cm is not None:
        cm.close()
      if cm_property is not None:
        cm_property.close()
      if capi_so_file is not None:
        capi_so_file.close()
      if capi32_so_file is not None:
        capi_so_file.close()
      if capi_test_server is not None:
        capi_test_server.close()
      if capi32_test_server is not None:
        capi32_test_server.close()

  except IOError as e:
    print e
    util.log('Error: can not find file or read data')
    return -1
  
  try:
    path = '../confmaster/script/%s' % (c.CM_EXEC_SCRIPT)
    cm_exec_script = open( path, 'rb' )

    if rpc.rpc_copy_cm_res( xmlrpclib.Binary( cm_exec_script.read() ), c.CM_EXEC_SCRIPT, id ) is not 0:
      util.log('failed to copy cm_exec_script')
      return -1

  except IOError:
    util.log('Error: can not find file or read data')
    return -1
  finally:
    if cm_exec_script is not None:
      cm_exec_script.close()

  return 0

def copy_dir( rpc, src, dest ):
  try:
    f = None

    for fname in os.listdir( path ):
      if fname.startswith( '.' ):
        continue

      util.log('try to copy %s to testserver' % (path))

      file_path = '%s/%s' % (path, fname)
      f = open( file_path, 'rb' )
      if rpc.rpc_copy_file( xmlrpclib.Binary( f.read() ), fname, id ) is not 0:
        util.log('failed to copy dir. path:%s' % (path))
        f.close()
        return False
      f.close()

  except IOError:
    util.log('Error: can not find file or read data')
    return False

  finally:
    if f is not None:
      f.close()

  return True

