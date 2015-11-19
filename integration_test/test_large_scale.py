import unittest
import test_base
import util
import time
import random
import gateway_mgmt
import redis_mgmt
import smr_mgmt 
import default_cluster
import config
import load_generator
import telnet
import constant
import demjson
import xmlrpclib
import json
import pdb
import telnetlib
import threading

mgmt_ip = None
mgmt_port = None

def cluster_ls():
  reply = mgmt_cmd('cluster_ls')
  ret = json.loads(reply)
  state = ret['state']
  if 'success' == state:
    return ret['data']['list']
  else:
    return None 

def mgmt_cmd(cmd):
  global mgmt_ip
  global mgmt_port

  try_cnt = 0
  while try_cnt < 10:
    try_cnt += 1
    try:
      t = telnetlib.Telnet(mgmt_ip, mgmt_port)
      t.write('%s\r\n' % cmd)
      reply = t.read_until('\r\n')
      t.close()
      return reply
    except IOError as e:
      util.log('%s, IP:%s, PORT:%d' % (e, mgmt_ip, mgmt_port))
      time.sleep(0.5)
  return None

def cluster_info(cluster_name):
  reply = mgmt_cmd('cluster_info %s' % cluster_name)
  ret = json.loads(reply)
  if ret == None:
    util.log('cluster_info fail, cluster_name:%s' % cluster_name)
    return None

  state = ret['state']
  if 'success' == state:
    return ret['data']
  else:
    return None 

def check_cluster(cluster_name):
  print ''
  print '==================================================================='
  print 'CLUSTER %s' % cluster_name

  ok = True
  cluster = cluster_info(cluster_name)
  for pg in sorted(cluster['pg_list'], key=lambda k: int(k['pg_id'])):
      pg_id = pg['pg_id']
      print ''
      print 'PG %s' % pg_id
      
      master_count = 0
      slave_count = 0
      pgs_id_list = pg['pg_data']['pgs_ID_List']
      for pgs_id in pgs_id_list:
          pgs_info = get_pgs_info(cluster_name, pgs_id)
          ip = pgs_info['pm_IP']
          port = pgs_info['management_Port_Of_SMR']
          role = util.smr_role(ip, port)
          msg = '%s:%s %s %s' % (ip, port, role, pgs_info['smr_Role'])
          if role == pgs_info['smr_Role']:
              print '+%s' % msg
              if role == 'M':
                  master_count = master_count + 1
              elif role == 'S':
                  slave_count = slave_count + 1
          else:
              print '@%s' % msg

      expected_slave_count = 1
      if len(pgs_id_list) == 3:
          expected_slave_count = 2
  
      if master_count == 1 and slave_count == expected_slave_count :
          print '+master_count=%d, slave_count=%d' % (master_count, slave_count)
      else:
          ok = False
          print '-master_count=%d, slave_count=%d' % (master_count, slave_count)
  return ok

def get_pgs_info(cluster_name, pgs_id):
  reply = mgmt_cmd('pgs_info %s %s\r\n' % (cluster_name, pgs_id))
  ret = json.loads(reply)
  pgs_info = ret['data']
  return pgs_info

class DeployPGSThread(threading.Thread):
  def __init__( self, server ):
    threading.Thread.__init__( self )
    self.server = server
    self.success = False

  def get_server( self ):
    return self.server

  def is_success( self ):
    return self.success

  def run( self ):
    smr = None
    gw = None
    redis = None
    cluster_util = None

    try:
      path = '../smr/replicator/%s' % constant.SMR
      smr = open( path, 'rb' )
    
      path = '../gateway/%s' % constant.GW
      gw = open( path, 'rb' )
    
      path = '../redis-2.8.8/src/%s' % constant.REDIS
      redis = open( path, 'rb' )
    
      path = '../redis-2.8.8/src/%s' % constant.CLUSTER_UTIL
      cluster_util = open( path, 'rb' )
    
      id = self.server['id']
      rpc = self.server['rpc']

      smr.seek(0, 0)
      gw.seek(0, 0)
      redis.seek(0, 0)
      cluster_util.seek(0, 0)

      util.log('copy binaries, server_id=%d' % id)
      if rpc.rpc_copy_smrreplicator( xmlrpclib.Binary( smr.read() ), id ) is not 0:
        util.log('failed to copy smr-replicator')
        self.assertEqual(0, 1, 'failed to copy smr-replicator')
      if rpc.rpc_copy_gw( xmlrpclib.Binary( gw.read() ), id ) is not 0:
        util.log('failed to copy gateway')
        self.assertEqual(0, 1, 'failed to copy gateway')
      if rpc.rpc_copy_redis_server( xmlrpclib.Binary( redis.read() ), id ) is not 0:
        util.log('failed to copy redis-arc')
        self.assertEqual(0, 1, 'failed to copy redis-arc')
      if rpc.rpc_copy_cluster_util( xmlrpclib.Binary( cluster_util.read() ), id ) is not 0:
        util.log('failed to copy cluster-util')
        self.assertEqual(0, 1, 'failed to copy cluster-util')
      
    except IOError as e:
      util.log(e)
      util.log('Error: can not find file or read data')
      self.assertEqual(0, 1, 'Error: can not find file or read data')

    except:
      util.log('Error: file open error.')

    finally:
      if smr != None:
        smr.close()
      if gw != None:
        gw.close()
      if redis != None:
        redis.close()
      if cluster_util != None:
        cluster_util.close()

    self.success = True

class TestLargeScale( unittest.TestCase ):
  leader_cm = config.clusters[0]['servers'][0]
  key_base = 'key_large_scale'
  cluster = config.clusters[0]

  @classmethod
  def setUpClass( cls ):
    global mgmt_ip
    global mgmt_port

    mgmt_ip = cls.leader_cm['ip']
    mgmt_port = cls.leader_cm['cm_port']

    util.set_remote_process_logfile_prefix( config.clusters[0], 'TestLargeScale' )
    ret = default_cluster.initialize_starting_up_smr_before_redis( cls.cluster )
    if ret is not 0:
      util.log( 'failed to initialize_starting_up_smr_before_redis in TestUpgrade' ) 
      default_cluster.finalize( cls.cluster )
    return 0

  @classmethod
  def tearDownClass( cls ):
    default_cluster.finalize( cls.cluster ) 
    return 0

  def setUp( self ):
    return 0

  def tearDown( self ):
    return 0

  def test_large_scale_master_election( self ):
    util.print_frame()

    # initialize cluster information
    pgs_id = 10
    cluster = { 
                'cluster_name' : 'large_scale',
                'keyspace_size' : 8192,
                'quorum_policy' : '0:1',
                'slots' : [],
                'pg_id_list' : [],
                'servers' : []
              }
    pg_max = 32
    pgs_per_pg = 3
    for pg_id in range(pg_max):
      cluster['pg_id_list'].append(pg_id)
      cluster['slots'].append(8192 / pg_max * pg_id)
      if pg_id == pg_max - 1:
        cluster['slots'].append(8191)
      else:
        cluster['slots'].append(8192 / pg_max * (pg_id + 1) - 1)

      for pgs in range(pgs_per_pg):
        smr_base_port = 15000 + pgs_id * 20
        smr_mgmt_port = smr_base_port + 3
        gateway_port = smr_base_port + 10
        redis_port = smr_base_port + 9

        server = {}
        server['id'] = pgs_id
        pgs_id = pgs_id + 1
        server['cluster_name'] = cluster['cluster_name']
        server['ip'] = self.cluster['servers'][0]['ip']
        server['pm_name'] = self.cluster['servers'][0]['pm_name']
        server['cm_port'] = None
        server['pg_id'] = pg_id
        server['smr_base_port'] = smr_base_port
        server['smr_mgmt_port'] = smr_mgmt_port
        server['gateway_port'] = gateway_port
        server['redis_port'] = redis_port
        server['zk_port'] = 2181
        server['rpc'] = config.clusters[0]['servers'][0]['rpc']

        cluster['servers'].append(server)

    # send initialize commands to confmaster
    test_base.initialize_cluster(cluster, self.leader_cm)

#    deploy_threads = []
#    for server in cluster['servers']:
#      t = DeployPGSThread(server)
#      deploy_threads.append(t)
#      t.start()
#
#    for t in deploy_threads:
#      t.join()
#      self.assertTrue( t.is_success(), "Error: deploy PGS '%d' fail." % t.get_server()['id'] )

    # set up pgs binaries
    try:
      path = '../smr/replicator/%s' % constant.SMR
      smr = open( path, 'rb' )
    
      path = '../gateway/%s' % constant.GW
      gw = open( path, 'rb' )
    
      path = '../redis-2.8.8/src/%s' % constant.REDIS
      redis = open( path, 'rb' )
    
      path = '../redis-2.8.8/src/%s' % constant.CLUSTER_UTIL
      cluster_util = open( path, 'rb' )
    
      for server in cluster['servers']:
        id = server['id']
        rpc = server['rpc']

        smr.seek(0, 0)
        gw.seek(0, 0)
        redis.seek(0, 0)
        cluster_util.seek(0, 0)

        util.log('copy binaries, server_id=%d' % id)
        if rpc.rpc_copy_smrreplicator( xmlrpclib.Binary( smr.read() ), id ) is not 0:
          util.log('failed to copy smr-replicator')
          self.assertEqual(0, 1, 'failed to copy smr-replicator')
        if rpc.rpc_copy_gw( xmlrpclib.Binary( gw.read() ), id ) is not 0:
          util.log('failed to copy gateway')
          self.assertEqual(0, 1, 'failed to copy gateway')
        if rpc.rpc_copy_redis_server( xmlrpclib.Binary( redis.read() ), id ) is not 0:
          util.log('failed to copy redis-arc')
          self.assertEqual(0, 1, 'failed to copy redis-arc')
        if rpc.rpc_copy_cluster_util( xmlrpclib.Binary( cluster_util.read() ), id ) is not 0:
          util.log('failed to copy cluster-util')
          self.assertEqual(0, 1, 'failed to copy cluster-util')
      
    except IOError as e:
      util.log(e)
      util.log('Error: can not find file or read data')
      self.assertEqual(0, 1, 'Error: can not find file or read data')

    except:
      util.log('Error: file open error.')
    
    finally:
      if smr != None:
        smr.close()
      if gw != None:
        gw.close()
      if redis != None:
        redis.close()
      if cluster_util != None:
        cluster_util.close()

    # cleanup servers`s directories
    for server in cluster['servers']:
      ret = test_base.cleanup_pgs_log_and_ckpt( cluster['cluster_name'], server )
      self.assertEqual(ret, 0, 'failed to cleanup_test_environment, id=%d' % server['id'])

    # start pgs
    for server in cluster['servers']:
      ret = test_base.request_to_start_smr( server ) 
      self.assertEqual(ret, 0, 'failed to request_to_start_smr, id=%d' % server['id'])
  
    for server in cluster['servers']:
      ret = test_base.request_to_start_redis( server, check=False )
      self.assertEqual(ret, 0, 'failed to request_to_start_smr, id=%d' % server['id'])

    for server in cluster['servers']:
      ret = test_base.wait_until_finished_to_set_up_role(server)
      self.assertEqual(ret, 0, 'failed to role set up, id=%d' % server['id'])
  
    for i in range(4):
      server = cluster['servers'][i]
      ret = test_base.request_to_start_gateway( cluster['cluster_name'], server, self.leader_cm )
      self.assertEqual(ret, 0, 'failed to request_to_start_gateway, id=%d' % server['id'])

    clusters = cluster_ls()
    self.assertNotEqual(len(clusters), 0, 'There is no clsuter.')
    
    ok = True
    for c in clusters:
      if not util.check_cluster(str(c), self.leader_cm['ip'], self.leader_cm['cm_port'], check_quorum=True):
        ok = False

    self.assertEqual(ok, True, 'failed to initlize roles of pgs')
        
    
