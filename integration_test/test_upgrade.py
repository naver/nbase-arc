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
import constant as c
import demjson


class TestUpgrade( unittest.TestCase ):
  cluster = config.clusters[0]
  leader_cm = config.clusters[0]['servers'][0]
  key_base = 'hangkey'

  @classmethod
  def setUpClass( cls ):
    util.set_remote_process_logfile_prefix( cls.cluster, 'TestUpgrade' )
    ret = default_cluster.initialize_starting_up_smr_before_redis( cls.cluster, verbose=2 )
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

  def test_upgrade_master_smr( self ):
    util.print_frame()

    # get master, slave1, slave2
    m, s1, s2 = util.get_mss( self.cluster )
    self.assertNotEqual( m, None, 'master is None.' )
    self.assertNotEqual( s1, None, 'slave1 is None.' )
    self.assertNotEqual( s2, None, 'slave2 is None.' )

    ret = util.upgrade_pgs( m, self.leader_cm, self.cluster )
    self.assertTrue(ret, 'Failed to upgrade master pgs%d' % m['id'])
  
  def test_upgrade_slave_smr( self ):
    util.print_frame()

    # get master, slave1, slave2
    m, s1, s2 = util.get_mss( self.cluster )
    self.assertNotEqual( m, None, 'master is None.' )
    self.assertNotEqual( s1, None, 'slave1 is None.' )
    self.assertNotEqual( s2, None, 'slave2 is None.' )

    ret = util.upgrade_pgs( s1, self.leader_cm, self.cluster )
    self.assertTrue(ret, 'Failed to upgrade slave pgs%d' % s1['id'])

  def test_upgrade_smr_repeatedly( self ):
    util.print_frame()
    
    execution_count_master = 0 
    execution_count_slave = 0 
    old_target = None 
    for cnt in range( 5 ):
      target = random.choice( self.cluster['servers'] )
      while target == old_target:
        target = random.choice( self.cluster['servers'] )
      old_target = target

      role = util.get_role_of_server( target )
      if role == c.ROLE_SLAVE:
        ret = util.upgrade_pgs( target, self.leader_cm, self.cluster )
        self.assertTrue(ret, 'Failed to upgrade slave pgs%d' % target['id'])

        execution_count_master = execution_count_master + 1
      elif role == c.ROLE_MASTER:
        ret = util.upgrade_pgs( target, self.leader_cm, self.cluster )
        self.assertTrue(ret, 'Failed to upgrade master pgs%d' % target['id'])
        execution_count_slave = execution_count_slave + 1
      else:
        self.fail( 'unexpected role:%s' % role )
      time.sleep( 1 )
      
      m, s1, s2 = util.get_mss( self.cluster )
      self.assertNotEqual( m, None, 'master is None.' )
      self.assertNotEqual( s1, None, 'slave1 is None.' )
      self.assertNotEqual( s2, None, 'slave2 is None.' )

      if execution_count_master == 0:
        ret = util.upgrade_pgs( m, self.leader_cm, self.cluster )
        self.assertTrue(ret, 'Failed to upgrade master pgs%d' % m['id'])
      if execution_count_slave == 0:
        ret = util.upgrade_pgs( s2, self.leader_cm, self.cluster )
        self.assertTrue(ret, 'Failed to upgrade slave pgs%d' % s2['id'])

