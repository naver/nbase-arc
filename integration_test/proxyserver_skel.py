from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import threading
import os
import constant as c
import telnetlib
import socket
import time
import exceptions
import process_mgmt
import xml_rpc_server
import util
import subprocess
import string
import shutil
import config
reload(process_mgmt)
reload(xml_rpc_server)
reload(util)


server = None
proc_mgmt = process_mgmt.ProcessMgmt()


class RequestHandler(SimpleXMLRPCRequestHandler):
  rpc_paths = ('/RPC2',)


class Worker(threading.Thread):
  def __init__( self, s ):
    threading.Thread.__init__( self )
    self.server = s

  def run( self ):
    self.server.serve_forever()
    return 0


# o_binary : the type of this one is xmlrpclib.Binary.
def rpc_copy_binary( o_binary, file_name, dir ):
  util.print_frame('begin copy, file_name:"%s", dir:"%s"' % (file_name, dir))
  ret = util.write_executable_file( o_binary.data, dir, file_name )
  util.print_frame('end')
  return ret

def rpc_copy_smrreplicator( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.smr_dir( id ), c.SMR )
  util.print_frame('end')
  return ret

def rpc_copy_gw( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.gw_dir( id ), c.GW )
  util.print_frame('end')
  return ret

def rpc_copy_redis_server( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.redis_dir( id ), c.REDIS )
  util.print_frame('end')
  return ret

def rpc_copy_cluster_util( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.cluster_util_dir( id ), c.CLUSTER_UTIL )
  util.print_frame('end')
  return ret

def rpc_copy_dump_util( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.dump_util_dir( id ), c.DUMP_UTIL )
  util.print_frame('end')
  return ret

def rpc_copy_log_util( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.log_util_dir( id ), c.LOG_UTIL )
  util.print_frame('end')
  return ret

def rpc_copy_dump_util_plugin( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.dump_util_dir( id ), c.DUMP_UTIL_PLUGIN )
  util.print_frame('end')
  return ret

def rpc_copy_capi_so_file( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.capi_dir( id ), c.CAPI_SO_FILE )
  util.print_frame('end')
  return ret

def rpc_copy_capi32_so_file( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.capi_dir( id ), c.CAPI32_SO_FILE )
  util.print_frame('end')
  return ret

def rpc_copy_capi_test_server( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.capi_dir( id ), c.CAPI_TEST_SERVER )
  util.print_frame('end')
  return ret

def rpc_copy_capi32_test_server( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.capi_dir( id ), c.CAPI32_TEST_SERVER )
  util.print_frame('end')
  return ret

def rpc_copy_cm( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.cc_dir( id ), c.CC )
  util.print_frame('end')
  return ret

def rpc_copy_cm_res( o_binary, file_name, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.cc_dir( id ), file_name )
  util.print_frame('end')
  return ret


def rpc_copy_file( o_binary, dir, file_name ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, dir, file_name )
  util.print_frame('end')
  return ret


def rpc_copy_hbc( o_binary, id ):
  util.print_frame('begin')
  ret = util.write_executable_file( o_binary.data, util.hbc_dir( id ), c.HBC )
  util.print_frame('end')
  return ret


def rpc_start_confmaster( id, port ):
  util.print_frame('begin')

  f_properties = open( '%s/%s' % (util.cc_dir( id ), c.CM_PROPERTY_FILE_NAME), 'r' )
  contents = f_properties.read()
  contents = string.replace( contents, 
                             'confmaster.port=%d' % c.CM_DEFAULT_PORT, 
                             'confmaster.port=%d' % port)
  f_properties.close()

  f_properties = open( '%s/%s' % (util.cc_dir( id ), c.CM_PROPERTY_FILE_NAME), 'w' )
  f_properties.write( contents )
  f_properties.close()

  f_log_std = util.open_process_logfile( id, 'cc_std' )
  f_log_err = util.open_process_logfile( id, 'cc_err' )
  p = util.exec_proc_async( util.cc_dir( id ), './%s' % c.CM_EXEC_SCRIPT, True, None, f_log_std, f_log_err )
  proc_mgmt.insert( util.cc_uid( id ), p )

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
      print 'failed to connect to confmaster, port=%d' % port

    time.sleep(1)

  if error:
    util.log('confmaster is not running properly.')
    util.kill_proc( p )
    util.print_frame('end')
    return -1

  util.print_frame('end')
  return 0


def do_logutil_cmd (id, subcmd):
  util.print_frame('begin')
  parent_dir, log_dir = util.smr_log_dir( id ) 
  cmd = './%s %s %s' % (c.LOG_UTIL, subcmd, log_dir)
  p = util.exec_proc_async(util.log_util_dir(id), cmd, True, None, subprocess.PIPE, None)
  ret = p.wait()
  if ret <> 0:
    util.log('failed to make memory log.')
    util.log('  cmd:%s' % cmd)
    util.log('  return code:%d' % p.returncode)
    util.log(p.stdout.readlines())
    util.print_frame('end')
    return -1
  else:
    util.print_frame('end')
    return 0 

def rpc_start_smr_replicator( id, ip, base_port, mgmt_port, verbose, check, log_delete_delay ):
  util.print_frame('begin')
  # setting log directory
  parent_dir, log_dir = util.smr_log_dir( id ) 
  full_path = '%s/%s' % (parent_dir, log_dir)
  if not os.path.exists( full_path ):
    cmd = 'mkdir %s' % full_path
    ret = util.shell_cmd_sync( cmd, 0 )
    if ret <> 0:
      util.log('failed to make log directory.')
      util.print_frame('end')
      return -1
    # make in memory volume
    if config.opt_use_memlog:
      do_logutil_cmd(id, 'createlog')

  cmd = './%s -d %s -b %d -v %d -x %d' % (c.SMR, log_dir, base_port, verbose, log_delete_delay)
  util.log(cmd)
  f_log_std = util.open_process_logfile( id, 'smr_std' )
  f_log_err = util.open_process_logfile( id, 'smr_err' )
  p = util.exec_proc_async( util.smr_dir( id ), cmd, True, None, f_log_std, f_log_err )  

  uid = util.smr_uid( id )
  proc_mgmt.insert( uid, p )

  # check if the process is running properly
  if check:
    if util.check_if_smr_is_running_properly( ip, mgmt_port ) is not 0:
      proc_mgmt.kill( uid )
      util.print_frame('end')
      return -1

  util.print_frame('end')
  return 0


def rpc_start_redis_server( id, smr_port, redis_port, check=True, max_try=30 ):
  util.print_frame('begin')
  cmd = './%s --smr-local-port %d --port %d --save ""' % (c.REDIS, smr_port, redis_port)
  f_log_std = util.open_process_logfile( id, 'redis_std' )
  f_log_err = util.open_process_logfile( id, 'redis_err' )
  p = util.exec_proc_async(   util.redis_dir( id )
                            , cmd
                            , True
                            , None
                            , f_log_std
                            , f_log_err )

  uid = util.redis_uid( id )
  proc_mgmt.insert( uid, p )

  # check if the process is running properly
  if check:
    if util.check_if_redis_is_running_properly( redis_port, 1, max_try ) is not 0:
      proc_mgmt.kill( uid )
      util.print_frame('end')
      return -1
  util.print_frame('end')
  return 0


def rpc_start_gateway( id, ip, cm_port, cluster_name, gw_port, check=True ):
  util.print_frame('begin')
  cmd = './%s -c %s -b %d -n %s -p %d' % (c.GW, ip, cm_port, cluster_name, gw_port)
  util.log(cmd)
  f_log_std = util.open_process_logfile( id, 'gw_std' )
  f_log_err = util.open_process_logfile( id, 'gw_err' )
  p = util.exec_proc_async( util.gw_dir( id ), cmd, True, None, f_log_std, f_log_err )

  uid = util.gateway_uid( id )
  proc_mgmt.insert( uid, p )

  # check if the process is running properly
  if check:
    if util.check_if_gateway_is_running_properly( ip, gw_port ) is not 0:
      proc_mgmt.kill( uid )
      util.print_frame('end')
      return -1
  util.print_frame('end')
  return 0


def rpc_start_proc( working_dir, cmd ):
  util.print_frame('begin')

  f_properties.close()

  f_properties = open( '%s/%s' % (util.cc_dir( id ), c.CM_PROPERTY_FILE_NAME), 'w' )
  f_properties.write( contents )
  f_properties.close()

  f_log_std = util.open_process_logfile( id, 'cc_std' )
  f_log_err = util.open_process_logfile( id, 'cc_err' )
  p = util.exec_proc_async( util.cc_dir( id ), './%s' % c.CM_EXEC_SCRIPT, True, None, f_log_std, f_log_err )
  proc_mgmt.insert( util.cc_uid( id ), p )

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
    util.log('confmaster is not running properly.')
    util.kill_proc( p )
    util.print_frame('end')
    return -1

  util.print_frame('end')
  return 0


def rpc_killps_y( name ):
  util.print_frame('begin')
  print 'rpc_killps_y %s' % name
  util.killps_y( name )
  util.shell_cmd_sync( 'ps -ef | grep mgmt-hbc', 0 )
  util.shell_cmd_sync( 'ps -ef | grep smr', 0 )
  util.shell_cmd_sync( 'ps -ef | grep confmaster', 0 )
  util.print_frame('end')
  return 0


def rpc_kill_smr( id ):
  util.print_frame('begin')
  uid = util.smr_uid( id )
  proc_mgmt.kill( uid )
  util.print_frame('end')
  return 0


def rpc_kill_redis( id ):
  util.print_frame('begin')
  uid = util.redis_uid( id )
  proc_mgmt.kill( uid )
  util.print_frame('end')
  return 0


def rpc_kill_all_processes():
  util.print_frame('begin')
  proc_mgmt.kill_all()
  util.print_frame('end')
  return 0


def rpc_zk_cmd( cmd ):
  args = 'zkCli.sh -server localhost:2181 %s' % (cmd)
  p = util.exec_proc_async( './', args, True, out_handle=subprocess.PIPE, err_handle=subprocess.PIPE )
  p.wait()
  (std, err) = p.communicate()
  # zkCli.sh stdout : znode data
  #          stderr : output messages(not error) and error messages
  if err != '':
    util.log( 'failed to execute zkCli.sh. command:%s, err:%s' % (cmd, err) )
    util.print_frame('end')
  
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


def rpc_delete_redis_check_point( id ):
  util.print_frame('begin')
  full_path = '%s/%s' % (util.redis_dir( id ), c.REDIS_CHECK_POINT_FILE_NAME)
  cmd = 'rm -f %s' % full_path
  ret = util.shell_cmd_sync( cmd, 0 )
  if ret <> 0:
    util.log('failed to delete redis check point')
    util.print_frame('end')
    return -1
  util.print_frame('end')
  return 0
 

def rpc_delete_smr_log_dir( id, base_port ):
  if config.opt_use_memlog:
    do_logutil_cmd(id, 'deletelog')

  util.print_frame('begin')
  parent_dir, log_dir = util.smr_log_dir( id ) 
  full_path = '%s/%s' % (parent_dir, log_dir)
  cmd = 'rm -rf %s' % full_path
  ret = util.shell_cmd_sync( cmd, 0 )
  if ret <> 0:
    util.log('failed to delete the log directory.')
    util.print_frame('end')
    return -1
  util.print_frame('end')
  return 0


def rpc_delete_smr_logs( id ):
  if config.opt_use_memlog:
    do_logutil_cmd(id, 'deletelog')

  util.print_frame('begin')
  parent_dir, log_dir = util.smr_log_dir( id )
  full_path = '%s/%s' % (parent_dir, log_dir)
  cmd = 'rm -rf %s/*' % full_path
  ret = util.shell_cmd_sync( cmd, 0 )
  if ret <> 0:
    util.log('failed to delete the logs.')
    util.print_frame('end')
    return -1
  util.print_frame('end')
  return 0


def rpc_shutdown_smr( id, ip, base_port ):
  util.print_frame('begin')
  uid = util.smr_uid( id )
  proc = proc_mgmt.get( uid )
  if proc is None:
    util.log('process(%s) does not exist.')
    util.print_frame('end')
    return -1

  proc_mgmt.kill( uid )
  time.sleep( 1 )

  cmd = "kill `ps aux | grep smr-replicator | grep '\-b %d' | grep -v grep | awk '{print $2}'`" % base_port
  util.shell_cmd_sync(cmd, 0)
  util.log(cmd)

  time.sleep( 1 )
  util.print_frame('end')
  return 0


def rpc_shutdown_redis( id, redis_port ):
  util.print_frame('begin')
  uid = util.redis_uid( id )
  proc = proc_mgmt.get( uid )
  if proc is None:
    util.print_frame('end')
    return -1

  proc_mgmt.kill( uid )
  time.sleep( 1 )

  cmd = "kill `ps axu | grep redis-arc | grep '\-\-port %d' | grep -v grep | awk '{print $2}'`" % redis_port
  util.shell_cmd_sync(cmd, 0)

  time.sleep( 1 )
  util.print_frame('end')
  return 0

def rpc_shutdown_hbc( id ):
  util.print_frame('begin')
  uid = util.hbc_uid( id )
  proc = proc_mgmt.get( uid )
  if proc is None:
    util.print_frame('end')
    return -1

  proc_mgmt.kill( uid )
  time.sleep( 1 )
  util.print_frame('end')
  return 0


def rpc_shutdown_cm( id ):
  util.print_frame('begin')
  uid = util.cc_uid( id )
  proc = proc_mgmt.get( uid )
  if proc is None:
    util.print_frame('end')
    return -1

  proc_mgmt.kill( uid )
  time.sleep( 1 )
  util.print_frame('end')
  return 0


def rpc_shutdown_gateway( id, port, force=False ):
  util.print_frame('begin')
  uid = util.gateway_uid( id )
  proc = proc_mgmt.get( uid )
  if proc is None:
    util.print_frame('end')
    if not force:
      return -1
  else:
    proc_mgmt.kill( uid )
    time.sleep( 1 )

  cmd = "kill `ps axu | grep redis-gateway | grep '\-p %d' | grep -v grep | awk '{print $2}'`" % port
  util.shell_cmd_sync(cmd, 0)

  time.sleep( 1 )
  util.print_frame('end')
  return 0


def rpc_set_process_logfile_prefix( prefix ):
  util.set_process_logfile_prefix( prefix )
  return 0


def rpc_backup_log( backup_log_dir ):
  try:
    util.print_frame('begin')
    shutil.copytree(c.homedir, backup_log_dir)
    util.print_frame('end')
  except exceptions.OSError as e:
    if e.errno == 17:
      pass
    else:
      raise e
  return 0


def rpc_del_dumprdb( id ):
  dir = util.redis_dir( id )
  path = '%s/dump.rdb' % dir 
  ret = os.path.exists( path )
  if ret:
    print 'delete %s' % path
    os.remove( path )
  return 0


def rpc_check_if_dumprdb_exists( id ):
  dir = util.redis_dir( id )
  path = '%s/dump.rdb' % dir 
  print 'check_if_dumprdb_exists %s' % path
  ret = os.path.exists( path )
  return ret


def rpc_start_zookeeper( zk_dir ):
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


def rpc_stop_zookeeper( zk_dir ):
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


def rpc_ls( dir ):
  util.print_frame( 'begin rpc_ls' )
  try:
    return os.listdir( dir )
  except OSError as e:
    util.log('failed to os.listdir.', e)
    util.print_frame('end rpc_ls')
    return []
  util.print_frame( 'end rpc_ls' )


def rpc_get_proc_fd_cnt( proc_uid ):
    p = proc_mgmt.get( proc_uid )
    pid = p.getPopen().pid
    return util.get_proc_fd_cnt(pid)


def rpc_get_childproc_fd_cnt( parent_proc_uid, child_proc_name ):
    p = proc_mgmt.get( parent_proc_uid )
    ppid = p.getPopen().pid
    return util.get_childproc_fd_cnt( ppid, child_proc_name )


def rpc_get_childproc_socket_cnt( parent_proc_uid, child_proc_name, dst_ports ):
    p = proc_mgmt.get( parent_proc_uid )
    ppid = p.getPopen().pid
    return util.get_childproc_socket_cnt( ppid, child_proc_name, dst_ports )


def start( ip, port ):
  server = xml_rpc_server.XMLRPCServer( ip, port )

  # register RPC functions
  try:
    server.register_function( rpc_copy_binary )
    server.register_function( rpc_copy_smrreplicator )
    server.register_function( rpc_copy_gw )
    server.register_function( rpc_copy_redis_server )
    server.register_function( rpc_copy_cluster_util )
    server.register_function( rpc_copy_dump_util )
    server.register_function( rpc_copy_log_util )
    server.register_function( rpc_copy_dump_util_plugin )
    server.register_function( rpc_copy_capi_so_file )
    server.register_function( rpc_copy_capi32_so_file )
    server.register_function( rpc_copy_capi_test_server )
    server.register_function( rpc_copy_capi32_test_server )
    server.register_function( rpc_copy_cm )
    server.register_function( rpc_copy_hbc )
    server.register_function( rpc_copy_cm_res )
    server.register_function( rpc_start_confmaster )
    server.register_function( rpc_start_smr_replicator )
    server.register_function( rpc_start_redis_server )
    server.register_function( rpc_start_gateway )
    server.register_function( rpc_kill_all_processes )
    server.register_function( rpc_zk_cmd )
    server.register_function( rpc_delete_redis_check_point )
    server.register_function( rpc_delete_smr_log_dir )
    server.register_function( rpc_delete_smr_logs )
    server.register_function( rpc_shutdown_smr )
    server.register_function( rpc_shutdown_redis )
    server.register_function( rpc_shutdown_hbc )
    server.register_function( rpc_shutdown_cm )
    server.register_function( rpc_shutdown_gateway )
    server.register_function( rpc_killps_y )
    server.register_function( rpc_set_process_logfile_prefix )
    server.register_function( rpc_backup_log )
    server.register_function( rpc_check_if_dumprdb_exists )
    server.register_function( rpc_del_dumprdb )
    server.register_function( rpc_start_zookeeper )
    server.register_function( rpc_stop_zookeeper )
    server.register_function( rpc_ls )
    server.register_function( rpc_get_proc_fd_cnt )
    server.register_function( rpc_get_childproc_fd_cnt )
    server.register_function( rpc_get_childproc_socket_cnt )
  except exceptions.AttributeError:
    util.log('error occur when registering functions to the rpc of proxyserver')

  # create bin folder
  if not os.path.exists( c.homedir ):
    os.mkdir( c.homedir )
  if not os.path.exists( c.logdir ):
    os.mkdir( c.logdir )

  t = Worker( server )
  t.start()

  return server, t

