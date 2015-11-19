import unittest
import test_base
import util
import time
import gateway_mgmt
import redis_mgmt
import smr_mgmt 
import default_cluster
import config
import load_generator
import telnet
import constant as c


class TestPGSHanging( unittest.TestCase ):
  cluster_2copy = config.clusters[3]
  cluster_3copy = config.clusters[0]
  max_load_generator = 1
  load_gen_thrd_list = {}
  key_base = 'hangkey'

  @classmethod
  def setUpClass( cls ):
    return 0

  @classmethod
  def tearDownClass( cls ):
    return 0

  def setUp( self ):
    return 0

  def tearDown( self ):
    ret = default_cluster.finalize( self.cluster ) 
    self.assertEquals( ret, 0, 'failed to TestPGSHanging.finalize' )
    return 0

  def setup_test_cluster( self, cluster ):
    self.cluster = cluster
    self.leader_cm = cluster['servers'][0]
    self.mgmt_ip = self.leader_cm['ip']
    self.mgmt_port = self.leader_cm['cm_port']

    util.set_remote_process_logfile_prefix( self.cluster, 'TestPGSHanging_%s' % self._testMethodName )
    ret = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
    if ret is not 0:
      default_cluster.finalize( self.cluster )
    self.assertEquals( ret, 0, 'failed to TestPGSHanging.initialize' )
    return 0

  def test_master_hang_2copy( self ):
    util.print_frame()

    self.setup_test_cluster( self.cluster_2copy )
    self.master_hang()
    return 0

  def test_master_hang_3copy( self ):
    util.print_frame()

    self.setup_test_cluster( self.cluster_3copy )
    self.master_hang()
    return 0

  def master_hang( self ):
    # get gateway info
    ip, port = util.get_rand_gateway( self.cluster )
    gw = gateway_mgmt.Gateway( self.cluster['servers'][0]['id'] )
    ret = gw.connect( ip, port )
    self.assertEqual( ret, 0, 'failed to connect to gateway, %s:%d' % (ip, port) )

    # set values
    for i in range( 0, 10000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      gw.write( cmd )
      res = gw.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values. cmd:%s, res:%s' % (cmd, res) )

    # get master, slave1, slave2
    if len(self.cluster['servers']) == 3:
      m, s1, s2 = util.get_mss( self.cluster )
      self.assertNotEqual( m, None, 'master is None.' )
      self.assertNotEqual( s1, None, 'slave1 is None.' )
      self.assertNotEqual( s2, None, 'slave2 is None.' )
    else:
      m, s1 = util.get_mss( self.cluster )
      self.assertNotEqual( m, None, 'master is None.' )
      self.assertNotEqual( s1, None, 'slave1 is None.' )
  
    util.log( 'server state before hang' )
    util.log_server_state( self.cluster )

    # hang
    smr = smr_mgmt.SMR( m['id'] )
    ret = smr.connect( m['ip'], m['smr_mgmt_port'] )
    self.assertEqual( ret, 0, 'failed to connect to master. %s:%d' % (m['ip'], m['smr_mgmt_port']) )
    smr.write( 'fi delay sleep 1 10000\r\n' )
    reply = smr.read_until( '\r\n', 1 )
    if reply != None and reply.find('-ERR not supported') != -1:
      self.assertEqual( 0, 1, 'make sure that smr has compiled with gcov option.' )

    time.sleep( 5 )

    # wait for forced master election
    success = False
    for i in range( 20 ):
      role = util.get_role_of_server( s1 ) 
      if role == c.ROLE_MASTER:
        success = True
        break
      
      if len(self.cluster['servers']) == 3:
        role = util.get_role_of_server( s2 )
        if role == c.ROLE_MASTER:
          success = True
          break
      time.sleep( 1 )

    util.log( 'server state transition after hang' )
    util.log_server_state( self.cluster )

    self.assertEqual( success, True, 'failed to forced master election' )

    redis1 = redis_mgmt.Redis( s1['id'] )
    ret = redis1.connect( s1['ip'], s1['redis_port'] )
    self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (s1['ip'], s1['redis_port']) )

    # set new values
    for i in range( 10000, 20000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      redis1.write( cmd )
      res = redis1.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values to redis1. cmd:%s, res:%s' % (cmd[:-2], res) )

    if len(self.cluster['servers']) == 3:
      redis2 = redis_mgmt.Redis( s2['id'] )
      ret = redis2.connect( s2['ip'], s2['redis_port'] )
      self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (s2['ip'], s2['redis_port']) )

      # check new values
      for i in range( 10000, 20000 ):
        cmd = 'get %s%d\r\n' % (self.key_base, i)
        redis2.write( cmd )
        redis2.read_until( '\r\n' )
        res = redis2.read_until( '\r\n' )
        self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis2. %s != %d' % (res, i) )

    # check if the haning server recovered and joined as a slave
    time.sleep( 7 )
    role = util.get_role_of_server( m ) 
    self.assertEqual( role, c.ROLE_SLAVE, 'failed to join as a slave' )
    
    redis0 = redis_mgmt.Redis( m['id'] )
    ret = redis0.connect( m['ip'], m['redis_port'] )
    self.assertEquals( ret, 0, 'failed to connect to redis(%s:%d).' % (m['ip'], m['redis_port']) )
  
    util.log( 'server state transition after hang' )
    util.log_server_state( self.cluster )

    # check new values
    for i in range( 10000, 20000 ):
      cmd = 'get %s%d\r\n' % (self.key_base, i)
      redis0.write( cmd )
      redis0.read_until( '\r\n' )
      res = redis0.read_until( '\r\n' )
      self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis2. %s != %d' % (res[:-2], i) )

    # check consistency
    self.assertEqual(util.check_cluster(self.cluster['cluster_name'], self.mgmt_ip, self.mgmt_port), True, 'role consistency fail')

    return 0

  def test_slave_hang_2copy( self ):
    util.print_frame()

    self.setup_test_cluster( self.cluster_2copy )
    self.slave_hang()

  def test_slave_hang_3copy( self ):
    util.print_frame()

    self.setup_test_cluster( self.cluster_3copy )
    self.slave_hang()

  def slave_hang( self ):
    # get gateway info
    ip, port = util.get_rand_gateway( self.cluster )
    gw = gateway_mgmt.Gateway( self.cluster['servers'][0]['id'] )
    ret = gw.connect( ip, port )
    self.assertEqual( ret, 0, 'failed to connect to gateway, %s:%d' % (ip, port) )

    # set values
    for i in range( 0, 10000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      gw.write( cmd )
      res = gw.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values. cmd:%s, res:%s' % (cmd, res) )

    # get master, slave1, slave2
    if len(self.cluster['servers']) == 3:
      m, s1, s2 = util.get_mss( self.cluster )
      self.assertNotEqual( m, None, 'master is None.' )
      self.assertNotEqual( s1, None, 'slave1 is None.' )
      self.assertNotEqual( s2, None, 'slave2 is None.' )
    else:
      m, s1 = util.get_mss( self.cluster )
      self.assertNotEqual( m, None, 'master is None.' )
      self.assertNotEqual( s1, None, 'slave1 is None.' )

    util.log( 'server state before hang' )
    util.log_server_state( self.cluster )

    # timestamp before hang
    ts_before = util.get_timestamp_of_pgs( s1 )
    self.assertNotEqual( ts_before, -1, 'failed to get a timestamp of pgs(%d), ts_before:%d' % (s1['id'], ts_before) )
  
    # hang
    util.log('pgs(id:%d, ip:%s, port:%d) is going to hang.' % (s1['id'], s1['ip'], s1['smr_mgmt_port']))
    smr = smr_mgmt.SMR( s1['id'] )
    ret = smr.connect( s1['ip'], s1['smr_mgmt_port'] )
    self.assertEqual( ret, 0, 'failed to connect to master. %s:%d' % (s1['ip'], s1['smr_mgmt_port']) )
    smr.write( 'fi delay sleep 1 6000\r\n' )
    reply = smr.read_until( '\r\n', 1 )
    if reply != None and reply.find('-ERR not supported') != -1:
      self.assertEqual( 0, 1, 'make sure that smr has compiled with gcov option.' )
    time.sleep( 7 )

    # wait for rejoin as a slave 
    success = False
    for i in range( 20 ):
      role = util.get_role_of_server( s1 ) 
      if role == c.ROLE_SLAVE:
        ts_after = util.get_timestamp_of_pgs( s1 )
        if ts_after != -1 and ts_before == ts_after:
          success = True
          break
      time.sleep( 1 )
    self.assertEqual( success, True, 'failed to rejoin as a slave' )

    util.log( 'server state transition after hang' )
    util.log_server_state( self.cluster )

    redis1 = redis_mgmt.Redis( s1['id'] )
    ret = redis1.connect( s1['ip'], s1['redis_port'] )
    self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (s1['ip'], s1['redis_port']) )

    # set new values
    for i in range( 10000, 20000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      redis1.write( cmd )
      res = redis1.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values to redis1. cmd:%s, res:%s' % (cmd[:-2], res) )

    if len(self.cluster['servers']) == 3:
      redis2 = redis_mgmt.Redis( s2['id'] )
      ret = redis2.connect( s2['ip'], s2['redis_port'] )
      self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (s2['ip'], s2['redis_port']) )

      # check new values
      for i in range( 10000, 20000 ):
        cmd = 'get %s%d\r\n' % (self.key_base, i)
        redis2.write( cmd )
        redis2.read_until( '\r\n' )
        res = redis2.read_until( '\r\n' )
        self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis2. %s != %d' % (res, i) )

    # check new values
    redis0 = redis_mgmt.Redis( m['id'] )
    ret = redis0.connect( m['ip'], m['redis_port'] )
    self.assertEquals( ret, 0, 'failed to connect to redis(%s:%d).' % (m['ip'], m['redis_port']) )

    for i in range( 10000, 20000 ):
      cmd = 'get %s%d\r\n' % (self.key_base, i)
      redis0.write( cmd )
      redis0.read_until( '\r\n' )
      res = redis0.read_until( '\r\n' )
      self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis2. %s != %d' % (res[:-2], i) )

    # check consistency
    self.assertEqual(util.check_cluster(self.cluster['cluster_name'], self.mgmt_ip, self.mgmt_port), True, 'role consistency fail')

    return 0

  def test_master_and_slave_hang_2copy( self ):
    util.print_frame()

    self.setup_test_cluster( self.cluster_2copy )
    self.master_and_slave_hang()

  def test_master_and_slave_hang_3copy( self ):
    util.print_frame()

    self.setup_test_cluster( self.cluster_3copy )
    self.master_and_slave_hang()

  def master_and_slave_hang( self ):
    # get gateway info
    ip, port = util.get_rand_gateway( self.cluster )
    gw = gateway_mgmt.Gateway( self.cluster['servers'][0]['id'] )
    ret = gw.connect( ip, port )
    self.assertEqual( ret, 0, 'failed to connect to gateway, %s:%d' % (ip, port) )

    # set values
    for i in range( 0, 10000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      gw.write( cmd )
      res = gw.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values. cmd:%s, res:%s' % (cmd, res) )

    # get master, slave1, slave2
    if len(self.cluster['servers']) == 3:
      m, s1, s2 = util.get_mss( self.cluster )
      self.assertNotEqual( m, None, 'master is None.' )
      self.assertNotEqual( s1, None, 'slave1 is None.' )
      self.assertNotEqual( s2, None, 'slave2 is None.' )
    else:
      m, s1 = util.get_mss( self.cluster )
      self.assertNotEqual( m, None, 'master is None.' )
      self.assertNotEqual( s1, None, 'slave1 is None.' )
 
    util.log( 'server state before hang' )
    util.log_server_state( self.cluster )
  
    # hang
    smr_master = smr_mgmt.SMR( m['id'] )
    ret = smr_master.connect( m['ip'], m['smr_mgmt_port'] )
    self.assertEqual( ret, 0, 'failed to connect to master. %s:%d' % (m['ip'], m['smr_mgmt_port']) )
    smr_slave = smr_mgmt.SMR( s1['id'] )
    ret = smr_slave.connect( s1['ip'], s1['smr_mgmt_port'] )
    self.assertEqual( ret, 0, 'failed to connect to master. %s:%d' % (s1['ip'], s1['smr_mgmt_port']) )

    smr_master.write( 'fi delay sleep 1 10000\r\n' )
    reply = smr_master.read_until( '\r\n', 1 )
    if reply != None and reply.find('-ERR not supported') != -1:
      self.assertEqual( 0, 1, 'make sure that smr has compiled with gcov option.' )

    smr_slave.write( 'fi delay sleep 1 10000\r\n' )

    util.log( 'server state transition after hang' )
    util.log_server_state( self.cluster )

    time.sleep( 5 )

    if len(self.cluster['servers']) == 3:
      # wait for forced master election
      success = True
      for i in range( 15 ):
        state = []
        util.check_cluster(self.cluster['cluster_name'], self.leader_cm['ip'], self.leader_cm['cm_port'], state)
        s2_state = filter(lambda s: s['pgs_id'] == s2['id'], state)[0]
        role = s2_state['active_role']
        if role == 'M':
          success = False
          break
        time.sleep( 1 )

      util.log( '' )
      util.log( 'It expects %s:%d is a slave, it can not transit to a master because it violate copy-quorum. (c:3, q:1, a:1)' )
      util.log( '' )
      util.log_server_state( self.cluster )

      self.assertEqual( success, True, 'failed to check copy-quorum' )

      ok = False
      for i in xrange(10):
        ok = util.check_cluster(self.cluster['cluster_name'], self.leader_cm['ip'], self.leader_cm['cm_port'])
        if ok:
          break
      self.assertTrue( ok, 'Cluster state is not normal!' ) 

      redis2 = redis_mgmt.Redis( s2['id'] )
      ret = redis2.connect( s2['ip'], s2['redis_port'] )
      self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (s2['ip'], s2['redis_port']) )

      # set new values
      for i in range( 10000, 20000 ):
        cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
        redis2.write( cmd )
        res = redis2.read_until( '\r\n' )
        self.assertEqual( res, '+OK\r\n', 'failed to set values to redis1. cmd:%s, res:%s' % (cmd[:-2], res) )

    util.log( 'server state transition after hang' )
    util.log_server_state( self.cluster )

    redis0 = redis_mgmt.Redis( m['id'] )
    ret = redis0.connect( m['ip'], m['redis_port'] )
    self.assertEqual( ret, 0, 'failed to connect to redis0(%s:%d).' % (m['ip'], m['redis_port']) )

    redis1 = redis_mgmt.Redis( s1['id'] )
    ret = redis1.connect( s1['ip'], s1['redis_port'] )
    self.assertEqual( ret, 0, 'failed to connect to redis1(%s:%d).' % (s1['ip'], s1['redis_port']) )

    if len(self.cluster['servers']) != 3:
      # set new values
      for i in range( 10000, 20000 ):
        cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
        redis0.write( cmd )
        res = redis0.read_until( '\r\n' )
        self.assertEqual( res, '+OK\r\n', 'failed to set values to redis0. cmd:%s, res:%s' % (cmd[:-2], res) )

    # check new values (m)
    for i in range( 10000, 20000 ):
      cmd = 'get %s%d\r\n' % (self.key_base, i)
      redis0.write( cmd )
      redis0.read_until( '\r\n' )
      res = redis0.read_until( '\r\n' )
      self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis(id:%d). %s != %d' % (m['id'], res, i) )

    # check new values (s1)
    for i in range( 10000, 20000 ):
      cmd = 'get %s%d\r\n' % (self.key_base, i)
      redis1.write( cmd )
      redis1.read_until( '\r\n' )
      res = redis1.read_until( '\r\n' )
      self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis(id:%d). %s != %d' % (s1['id'], res[:-2], i) )

    # check consistency
    self.assertEqual(util.check_cluster(self.cluster['cluster_name'], self.mgmt_ip, self.mgmt_port), True, 'role consistency fail')

    return 0

  def test_two_slaves_hang( self ):
    util.print_frame()

    self.setup_test_cluster( self.cluster_3copy )

    # get gateway info
    ip, port = util.get_rand_gateway( self.cluster )
    gw = gateway_mgmt.Gateway( self.cluster['servers'][0]['id'] )
    ret = gw.connect( ip, port )
    self.assertEqual( ret, 0, 'failed to connect to gateway, %s:%d' % (ip, port) )

    # set values
    for i in range( 0, 10000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      gw.write( cmd )
      res = gw.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values. cmd:%s, res:%s' % (cmd, res) )

    # get master, slave1, slave2
    m, s1, s2 = util.get_mss( self.cluster )
    self.assertNotEqual( m, None, 'master is None.' )
    self.assertNotEqual( s1, None, 'slave1 is None.' )
    self.assertNotEqual( s2, None, 'slave2 is None.' )

    util.log( 'server state before hang' )
    util.log_server_state( self.cluster )

    # timestamp before hang
    ts_before1 = util.get_timestamp_of_pgs( s1 )
    self.assertNotEqual( ts_before1, -1, 'failed to get a timestamp of pgs(%d), ts_before:%d' % (s1['id'], ts_before1) )
  
    ts_before2 = util.get_timestamp_of_pgs( s2 )
    self.assertNotEqual( ts_before2, -1, 'failed to get a timestamp of pgs(%d), ts_before:%d' % (s2['id'], ts_before2) )
 
    # hang
    smr1 = smr_mgmt.SMR( s1['id'] )
    ret = smr1.connect( s1['ip'], s1['smr_mgmt_port'] )
    self.assertEqual( ret, 0, 'failed to connect to master. %s:%d' % (s1['ip'], s1['smr_mgmt_port']) )

    smr2 = smr_mgmt.SMR( s2['id'] )
    ret = smr2.connect( s2['ip'], s2['smr_mgmt_port'] )
    self.assertEqual( ret, 0, 'failed to connect to master. %s:%d' % (s1['ip'], s1['smr_mgmt_port']) )
 
    smr1.write( 'fi delay sleep 1 8000\r\n' )
    reply = smr1.read_until( '\r\n', 1 )
    if reply != None and reply.find('-ERR not supported') != -1:
      self.assertEqual( 0, 1, 'make sure that smr has compiled with gcov option.' )

    smr2.write( 'fi delay sleep 1 8000\r\n' )
    time.sleep( 7 )

    # wait for rejoin as a slave 
    success = False
    for i in range( 20 ):
      role = util.get_role_of_server( s1 ) 
      if role == c.ROLE_SLAVE:
        ts_after = util.get_timestamp_of_pgs( s1 )
        if ts_after != -1 and ts_before1 == ts_after:
          success = True
          break
      time.sleep( 1 )
    self.assertEqual( success, True, 'failed to rejoin as a slave. %s:%d' % (s2['ip'], s2['smr_mgmt_port']) )

    success = False
    for i in range( 20 ):
      role = util.get_role_of_server( s2 ) 
      if role == c.ROLE_SLAVE:
        ts_after = util.get_timestamp_of_pgs( s2 )
        if ts_after != -1 and ts_before2 == ts_after:
          success = True
          break
      time.sleep( 1 )
    self.assertEqual( success, True, 'failed to rejoin as a slave. %s:%d' % (s2['ip'], s2['smr_mgmt_port']) )

    util.log( 'server state transition after hang' )
    util.log_server_state( self.cluster )

    redis1 = redis_mgmt.Redis( s1['id'] )
    ret = redis1.connect( s1['ip'], s1['redis_port'] )
    self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (s1['ip'], s1['redis_port']) )

    redis2 = redis_mgmt.Redis( s2['id'] )
    ret = redis2.connect( s2['ip'], s2['redis_port'] )
    self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (s2['ip'], s2['redis_port']) )

    # set new values
    for i in range( 10000, 20000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      redis1.write( cmd )
      res = redis1.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values to redis1. cmd:%s, res:%s' % (cmd[:-2], res) )

    # check new values
    for i in range( 10000, 20000 ):
      cmd = 'get %s%d\r\n' % (self.key_base, i)
      redis2.write( cmd )
      redis2.read_until( '\r\n' )
      res = redis2.read_until( '\r\n' )
      self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis2. %s != %d' % (res, i) )

    # check consistency
    self.assertEqual(util.check_cluster(self.cluster['cluster_name'], self.mgmt_ip, self.mgmt_port), True, 'role consistency fail')

    return 0

  def test_all_pgs_hang( self ):
    util.print_frame()

    self.setup_test_cluster( self.cluster_3copy )

    # get gateway info
    ip, port = util.get_rand_gateway( self.cluster )
    gw = gateway_mgmt.Gateway( self.cluster['servers'][0]['id'] )
    ret = gw.connect( ip, port )
    self.assertEqual( ret, 0, 'failed to connect to gateway, %s:%d' % (ip, port) )

    # set values
    for i in range( 0, 10000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      gw.write( cmd )
      res = gw.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values. cmd:%s, res:%s' % (cmd, res) )

    # get master, slave1, slave2
    m, s1, s2 = util.get_mss( self.cluster )
    self.assertNotEqual( m, None, 'master is None.' )
    self.assertNotEqual( s1, None, 'slave1 is None.' )
    self.assertNotEqual( s2, None, 'slave2 is None.' )
  
    util.log( 'server state before hang' )
    util.log_server_state( self.cluster )

    # hang
    smr_master = smr_mgmt.SMR( m['id'] )
    ret = smr_master.connect( m['ip'], m['smr_mgmt_port'] )
    self.assertEqual( ret, 0, 'failed to connect to master. %s:%d' % (m['ip'], m['smr_mgmt_port']) )
    smr_slave1 = smr_mgmt.SMR( s1['id'] )
    ret = smr_slave1.connect( s1['ip'], s1['smr_mgmt_port'] )
    self.assertEqual( ret, 0, 'failed to connect to master. %s:%d' % (s1['ip'], s1['smr_mgmt_port']) )
    smr_slave2 = smr_mgmt.SMR( s2['id'] )
    ret = smr_slave2.connect( s2['ip'], s2['smr_mgmt_port'] )
    self.assertEqual( ret, 0, 'failed to connect to master. %s:%d' % (s2['ip'], s2['smr_mgmt_port']) )

    m_ts = util.get_timestamp_of_pgs( m )
    s1_ts = util.get_timestamp_of_pgs( s1 )
    s2_ts = util.get_timestamp_of_pgs( s2 )

    smr_master.write( 'fi delay sleep 1 8000\r\n' )
    reply = smr_master.read_until( '\r\n', 1 )
    if reply != None and reply.find('-ERR not supported') != -1:
      self.assertEqual( 0, 1, 'make sure that smr has compiled with gcov option.' )

    smr_slave1.write( 'fi delay sleep 1 8000\r\n' )
    smr_slave2.write( 'fi delay sleep 1 8000\r\n' )

    time.sleep( 10 )

    # wait for forced master election
    success = False
    master = None
    for i in range( 20 ):
      role = util.get_role_of_server( s1 ) 
      ts = util.get_timestamp_of_pgs( s1 )
      if role == c.ROLE_MASTER and ts == s1_ts:
        master = s1
        success = True
        break
      role = util.get_role_of_server( s2 )
      ts = util.get_timestamp_of_pgs( s2 )
      if role == c.ROLE_MASTER and ts == s2_ts:
        master = s2
        success = True
        break
      role = util.get_role_of_server( m )
      ts = util.get_timestamp_of_pgs( m )
      if role == c.ROLE_MASTER and ts == m_ts:
        master = m 
        success = True
        break
      time.sleep( 1 )

    m_ts = util.get_timestamp_of_pgs( m )
    s1_ts = util.get_timestamp_of_pgs( s1 )
    s2_ts = util.get_timestamp_of_pgs( s2 )

    self.assertEqual( success, True, 'failed to forced master election' )

    servers = [m, s1, s2]
    for s in servers:
      if s != master:
        for i in range( 20 ):
          role = util.get_role_of_server( s ) 
          if role == c.ROLE_SLAVE:
            success = True
            break
          time.sleep( 1 )
        self.assertEqual( success, True, 'failed to rejoin as a slave, %s:%d' % (s['ip'], s['smr_mgmt_port']) )

    util.log( 'server state transition after hang' )
    util.log_server_state( self.cluster )

    redis0 = redis_mgmt.Redis( m['id'] )
    ret = redis0.connect( m['ip'], m['redis_port'] )
    self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (m['ip'], m['redis_port']) )

    # set values
    for i in range( 10000, 20000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      redis0 .write( cmd )
      res = redis0.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values. cmd:%s, res:%s' % (cmd, res) )

    redis1 = redis_mgmt.Redis( s1['id'] )
    ret = redis1.connect( s1['ip'], s1['redis_port'] )
    self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (s1['ip'], s1['redis_port']) )

    redis2 = redis_mgmt.Redis( s2['id'] )
    ret = redis2.connect( s2['ip'], s2['redis_port'] )
    self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (s2['ip'], s2['redis_port']) )

    # check new values (m)
    for i in range( 10000, 20000 ):
      cmd = 'get %s%d\r\n' % (self.key_base, i)
      redis0.write( cmd )
      redis0.read_until( '\r\n' )
      res = redis0.read_until( '\r\n' )
      self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis(id:%d). %s != %d' % (m['id'], res, i) )

    # check new values (s1)
    for i in range( 10000, 20000 ):
      cmd = 'get %s%d\r\n' % (self.key_base, i)
      redis1.write( cmd )
      redis1.read_until( '\r\n' )
      res = redis1.read_until( '\r\n' )
      self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis(id:%d). %s != %d' % (s1['id'], res[:-2], i) )

    # check new values (s2)
    for i in range( 10000, 20000 ):
      cmd = 'get %s%d\r\n' % (self.key_base, i)
      redis2.write( cmd )
      redis2.read_until( '\r\n' )
      res = redis2.read_until( '\r\n' )
      self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis(id:%d). %s != %d' % (s2['id'], res[:-2], i) )

    # check consistency
    ok = False
    for try_cnt in range(0, 10):
      ok = util.check_cluster(self.cluster['cluster_name'], self.mgmt_ip, self.mgmt_port)
      print ok
      if ok:
        break
      time.sleep(1)
    self.assertEqual(ok, True, 'role consistency fail')

    return 0

  def failover_while_hang( self, server ):
    # timestamp before hang
    ts_before = util.get_timestamp_of_pgs( server )
    self.assertNotEqual( ts_before, -1, 'failed to get a timestamp of pgs(%d), ts_before:%d' % (server['id'], ts_before) )
  
    # hang
    util.log('pgs(id:%d, ip:%s, port:%d) is going to hang.' % (server['id'], server['ip'], server['smr_mgmt_port']))
    smr = smr_mgmt.SMR( server['id'] )
    ret = smr.connect( server['ip'], server['smr_mgmt_port'] )
    self.assertEqual( ret, 0, 'failed to connect to master. %s:%d' % (server['ip'], server['smr_mgmt_port']) )
    smr.write( 'fi delay sleep 1 10000\r\n' )
    reply = smr.read_until( '\r\n', 1 )
    if reply != None and reply.find('-ERR not supported') != -1:
      self.assertEqual( 0, 1, 'make sure that smr has compiled with gcov option.' )

    time.sleep( 4 )

    # check state F
    max_try = 20
    expected = 'F'
    for i in range( 0, max_try):
      state = util.get_smr_state( server, self.leader_cm )
      if expected == state:
        break;
      time.sleep( 1 )
    self.assertEquals( expected , state,
                       'server%d - state:%s, expected:%s' % (server['id'], state, expected) )
    util.log( 'succeeded : pgs%d state changed to F.' % server['id'] )

    # shutdown
    util.log( 'shutdown pgs%d while hanging.' % server['id'] )
    ret = test_base.request_to_shutdown_smr( server )
    self.assertEqual( ret, 0, 'failed to shutdown smr. id:%d' % server['id'] )
    ret = test_base.request_to_shutdown_redis( server )
    self.assertEquals( ret, 0, 'failed to shutdown redis. id:%d' % server['id'] )

    # check state F
    max_try = 20
    expected = 'F'
    for i in range( 0, max_try):
      state = util.get_smr_state( server, self.leader_cm )
      if expected == state:
        break;
      time.sleep( 1 )
    self.assertEquals( expected , state,
                       'server%d - state:%s, expected:%s' % (server['id'], state, expected) )
    util.log( 'succeeded : pgs%d state changed to F.' % server['id'] )

    # recovery
    util.log( 'restart pgs%d.' % server['id'] )
    ret = test_base.request_to_start_smr( server )
    self.assertEqual( ret, 0, 'failed to start smr. id:%d' % server['id'] )

    ret = test_base.request_to_start_redis( server )
    self.assertEqual( ret, 0, 'failed to start redis. id:%d' % server['id'] )

    wait_count = 20
    ret = test_base.wait_until_finished_to_set_up_role( server, wait_count )
    self.assertEquals( ret, 0, 'failed to role change. smr_id:%d' % (server['id']) )

    redis = redis_mgmt.Redis( server['id'] )
    ret = redis.connect( server['ip'], server['redis_port'] )
    self.assertEquals( ret, 0, 'failed to connect to redis' )

    # check state N
    max_try = 20
    expected = 'N'
    for i in range( 0, max_try):
      state = util.get_smr_state( server, self.leader_cm )
      if expected == state:
        break;
      time.sleep( 1 )
    self.assertEquals( expected , state,
                       'server%d - state:%s, expected:%s' % (server['id'], state, expected) )
    util.log( 'succeeded : pgs%d state changed to N.' % server['id'] )

    # wait for rejoin as a slave 
    success = False
    for i in range( 20 ):
      role = util.get_role_of_server( server ) 
      if role == c.ROLE_SLAVE:
        ts_after = util.get_timestamp_of_pgs( server )
        if ts_after != -1 and ts_before != ts_after:
          success = True
          break
      time.sleep( 1 )
    self.assertEqual( success, True, 'failed to rejoin as a slave' )
    util.log( 'succeeded : pgs%d joined as a slave.' % server['id'] )

    return 0
  
  def test_master_failover_while_hang_2copy( self ):
    util.print_frame()

    self.setup_test_cluster( self.cluster_2copy )
    self.master_failover_while_hang()

  def test_master_failover_while_hang_3copy( self ):
    util.print_frame()

    self.setup_test_cluster( self.cluster_3copy )
    self.master_failover_while_hang()

  def master_failover_while_hang( self ):
    util.print_frame()

    # get gateway info
    ip, port = util.get_rand_gateway( self.cluster )
    gw = gateway_mgmt.Gateway( self.cluster['servers'][0]['id'] )
    ret = gw.connect( ip, port )
    self.assertEqual( ret, 0, 'failed to connect to gateway, %s:%d' % (ip, port) )

    # set values
    for i in range( 0, 10000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      gw.write( cmd )
      res = gw.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values. cmd:%s, res:%s' % (cmd, res) )

    # get master, slave1, slave2
    if len(self.cluster['servers']) == 3:
      m, s1, s2 = util.get_mss( self.cluster )
      self.assertNotEqual( m, None, 'master is None.' )
      self.assertNotEqual( s1, None, 'slave1 is None.' )
      self.assertNotEqual( s2, None, 'slave2 is None.' )
    else:
      m, s1 = util.get_mss( self.cluster )
      self.assertNotEqual( m, None, 'master is None.' )
      self.assertNotEqual( s1, None, 'slave1 is None.' )

    util.log( 'server state before hang' )
    util.log_server_state( self.cluster )

    self.failover_while_hang( m )

    util.log( 'server state transition after hang' )
    util.log_server_state( self.cluster )

    redis1 = redis_mgmt.Redis( m['id'] )
    ret = redis1.connect( m['ip'], m['redis_port'] )
    self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (m['ip'], m['redis_port']) )

    # set new values
    for i in range( 10000, 20000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      redis1.write( cmd )
      res = redis1.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values to redis1. cmd:%s, res:%s' % (cmd[:-2], res) )

    if len(self.cluster['servers']) == 3:
      redis2 = redis_mgmt.Redis( s2['id'] )
      ret = redis2.connect( s2['ip'], s2['redis_port'] )
      self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (s2['ip'], s2['redis_port']) )

      # check new values
      for i in range( 10000, 20000 ):
        cmd = 'get %s%d\r\n' % (self.key_base, i)
        redis2.write( cmd )
        redis2.read_until( '\r\n' )
        res = redis2.read_until( '\r\n' )
        self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis2. %s != %d' % (res, i) )
      util.log( 'succeeded : check values with set/get operations with pgs%d and pgs%d.' % (m['id'], s2['id']) )

    redis0 = redis_mgmt.Redis( m['id'] )
    ret = redis0.connect( m['ip'], m['redis_port'] )
    self.assertEquals( ret, 0, 'failed to connect to redis(%s:%d).' % (m['ip'], m['redis_port']) )
  
    # check new values
    for i in range( 10000, 20000 ):
      cmd = 'get %s%d\r\n' % (self.key_base, i)
      redis0.write( cmd )
      redis0.read_until( '\r\n' )
      res = redis0.read_until( '\r\n' )
      self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis2. %s != %d' % (res[:-2], i) )

    # check consistency
    self.assertEqual(util.check_cluster(self.cluster['cluster_name'], self.mgmt_ip, self.mgmt_port), True, 'role consistency fail')

    return 0

  def test_slave_failover_while_hang_2copy( self ):
    util.print_frame()

    self.setup_test_cluster( self.cluster_2copy )
    self.slave_failover_while_hang()

  def test_slave_failover_while_hang_3copy( self ):
    util.print_frame()

    self.setup_test_cluster( self.cluster_3copy )
    self.slave_failover_while_hang()

  def slave_failover_while_hang( self ):
    util.print_frame()

    # get gateway info
    ip, port = util.get_rand_gateway( self.cluster )
    gw = gateway_mgmt.Gateway( self.cluster['servers'][0]['id'] )
    ret = gw.connect( ip, port )
    self.assertEqual( ret, 0, 'failed to connect to gateway, %s:%d' % (ip, port) )

    # set values
    for i in range( 0, 10000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      gw.write( cmd )
      res = gw.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values. cmd:%s, res:%s' % (cmd, res) )

    # get master, slave1, slave2
    if len(self.cluster['servers']) == 3:
      m, s1, s2 = util.get_mss( self.cluster )
      self.assertNotEqual( m, None, 'master is None.' )
      self.assertNotEqual( s1, None, 'slave1 is None.' )
      self.assertNotEqual( s2, None, 'slave2 is None.' )
    else:
      m, s1 = util.get_mss( self.cluster )
      self.assertNotEqual( m, None, 'master is None.' )
      self.assertNotEqual( s1, None, 'slave1 is None.' )

    util.log( 'server state before hang' )
    util.log_server_state( self.cluster )

    self.failover_while_hang( s1 )

    util.log( 'server state transition after hang' )
    util.log_server_state( self.cluster )

    redis1 = redis_mgmt.Redis( s1['id'] )
    ret = redis1.connect( s1['ip'], s1['redis_port'] )
    self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (s1['ip'], s1['redis_port']) )

    # set new values
    for i in range( 10000, 20000 ):
      cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
      redis1.write( cmd )
      res = redis1.read_until( '\r\n' )
      self.assertEqual( res, '+OK\r\n', 'failed to set values to redis1. cmd:%s, res:%s' % (cmd[:-2], res) )

    if len(self.cluster['servers']) == 3:
      redis2 = redis_mgmt.Redis( s2['id'] )
      ret = redis2.connect( s2['ip'], s2['redis_port'] )
      self.assertEqual( ret, 0, 'failed to connect to redis(%s:%d).' % (s2['ip'], s2['redis_port']) )

      # check new values
      for i in range( 10000, 20000 ):
        cmd = 'get %s%d\r\n' % (self.key_base, i)
        redis2.write( cmd )
        redis2.read_until( '\r\n' )
        res = redis2.read_until( '\r\n' )
        self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis2. %s != %d' % (res, i) )
      util.log( 'succeeded : check values with set/get operations with pgs%d and pgs%d.' % (s1['id'], s2['id']) )

    redis0 = redis_mgmt.Redis( m['id'] )
    ret = redis0.connect( m['ip'], m['redis_port'] )
    self.assertEquals( ret, 0, 'failed to connect to redis(%s:%d).' % (m['ip'], m['redis_port']) )
  
    # check new values
    for i in range( 10000, 20000 ):
      cmd = 'get %s%d\r\n' % (self.key_base, i)
      redis0.write( cmd )
      redis0.read_until( '\r\n' )
      res = redis0.read_until( '\r\n' )
      self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis2. %s != %d' % (res[:-2], i) )

    # check consistency
    self.assertEqual(util.check_cluster(self.cluster['cluster_name'], self.mgmt_ip, self.mgmt_port), True, 'role consistency fail')
    return 0

  # TODO
  #def test_check_consistent_hang_pgs_and_gateway( self ):
  #  return 0

