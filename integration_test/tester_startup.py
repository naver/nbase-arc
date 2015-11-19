import unittest
import xmlrpclib
import constant as c
import config
import time
import os
import sys
import default_cluster
import test_crc16
import test_basic_op
import test_local_proxy
import test_restart_recovery
import test_check_point_and_log
import test_quorum_policy
import test_migration
import test_consistent_read
import test_scaleout
import test_confmaster
import test_heartbeat_checker
import test_pgs_hanging
import test_freeclient
import test_upgrade
import test_gateway
import test_dumputil
import test_maintenance
import test_large_scale
import test_gateway_mgmt
import test_redis_mgmt
import test_arcci
import test_network_isolation
import test_protocol_error
import util
import test_base
import getopt

USAGE = """usage: python tester_startup.py <options>
option:
    -i : Without test, run only nBase-ARC processes.
    -s : Skip copying binaries. (It must be used when binaries already be deployed.)
    -l <testlog_backup_dir> : Backup test-logs to the specified directory.
    -c : test 32bit client api
etc: In order to run tester_startup.py, updater_startup.py must be running.
     Read README file for more details.
"""

def main(argv):
  try:
    opts, args = getopt.getopt(argv, 'disl:c', ['large_scale_test_only', 'init', 'skip-copy-binaries=', 'backup_log_dir=', 'client-32bit-test'])
  except getopt.GetoptError:
    print USAGE
    sys.exit(-2)

  init = False
  backup_log_dir = None
  skip_copy_binaries = False
  large_scale_test_only = False
  client_32bit_test = False
  for opt, arg in opts:
    if opt in ("-i", '--init'):
      init = True
    elif opt in ("-l", '--backup_log_dir'):
      backup_log_dir = arg
    elif opt in ("-s", '--skip-copy-binareis'):
      skip_copy_binaries = True
    elif opt in ("-d", '--large_scale_test_only'):
      large_scale_test_only = True
    elif opt in ("-c", '--client-32bit-test'):
      client_32bit_test = True

  #if 0 != test_base.initialize_rpc( config.physical_machines, config.clusters, skip_copy_binaries ):
    #util.log('failed to initialize test_base')
    #return -1

  config.verify_config() 

  print "==================== S T A R T ====================="

  for physical_machine in config.physical_machines:
    rpc = test_base.connect_to_update_server( physical_machine )
    physical_machine['rpc'] = rpc

  #  resource caching/skip structures
  pm_ip_dict = {} # boolean value
  svr_ip_dict = {} # boolean value

  for physical_machine in config.physical_machines:
    pmid = physical_machine['ip']
    print 'Init PM %s' % pmid

    # skip if it is already done
    if pm_ip_dict.has_key(pmid):
      print 'Skip resource copy for PM %s' % pmid
      continue
    pm_ip_dict[pmid] = True

    for cluster in config.clusters:
      for server in cluster['servers']:
        if test_base.setup_rpc( server ) is not 0:
          return -1
        
	# Note: The detailed usage between real_ip and ip is ambiguous for now (hack it)
	# To be improved.
	if server.has_key('real_ip'):
	  svrid = server['real_ip'] + '/' + str(server['id'])
	else:
	  svrid = server['ip'] + '/' + str(server['id'])

	if svr_ip_dict.has_key(svrid):
	  print 'Skip resource copy for SERVER %s' % svrid
	  continue
	svr_ip_dict[svrid] = True

        if skip_copy_binaries is False:
	  print 'Init SERVER %s' % svrid
          if test_base.send_binaries_to_testmachine( rpc, server ) is not 0:
            util.log('failed to send_binaries_to_testmachine, ip:%s, id:%d' % (server['ip'], server['id']))
            return -1

  if init is True:
    if default_cluster.initialize_starting_up_smr_before_redis( config.clusters[0], verbose=2 ) is not 0:
      util.log('failed setting up servers.')
    else:
      util.log('finished successfully setting up servers.' )
    return 0
  else:
    util.log('begin tests')

    if large_scale_test_only == True:
      test_classes = [
          [test_large_scale, test_large_scale.TestLargeScale]
      ]
    elif client_32bit_test == True:
      test_classes = [
          [test_arcci, test_arcci.TestARCCI32]
      ]
    else:
      test_classes = [
          [test_heartbeat_checker, test_heartbeat_checker.TestHeartbeatChecker]
        , [test_scaleout, test_scaleout.TestScaleout]
        , [test_crc16, test_crc16.TestCRC16]
        , [test_local_proxy, test_local_proxy.TestLocalProxy]
        , [test_basic_op, test_basic_op.TestBasicOp]
        , [test_restart_recovery, test_restart_recovery.TestRestartRecovery]
        , [test_check_point_and_log, test_check_point_and_log.TestCheckPointAndLog]
        , [test_consistent_read, test_consistent_read.TestConsistentRead]
        , [test_migration, test_migration.TestMigration]
        , [test_quorum_policy, test_quorum_policy.TestQuorumPolicy]
        , [test_confmaster, test_confmaster.TestConfMaster]
        , [test_pgs_hanging, test_pgs_hanging.TestPGSHanging]
        , [test_freeclient, test_freeclient.TestFreeClient]
        , [test_upgrade, test_upgrade.TestUpgrade]
        , [test_gateway, test_gateway.TestGateway]
        , [test_dumputil, test_dumputil.TestDumpUtil]
        , [test_maintenance, test_maintenance.TestMaintenance]
        , [test_large_scale, test_large_scale.TestLargeScale]
        , [test_gateway_mgmt, test_gateway_mgmt.TestGatewayMgmt]
        , [test_redis_mgmt, test_redis_mgmt.TestRedisMgmt]
        , [test_arcci, test_arcci.TestARCCI64]
        , [test_network_isolation, test_network_isolation.TestNetworkIsolation]
        , [test_protocol_error, test_protocol_error.TestProtocolError]
      ]
  
    util.log('prefare testcases')
    results = []
    for test_class in test_classes:
      suite = unittest.TestSuite()
      t = unittest.TestLoader().loadTestsFromTestCase( test_class[1] )
      suite.addTests(t)
      
      util.log('run %s' % str(test_class[0]))
      ret = unittest.TextTestRunner().run( suite )
      results.append(ret)
    
    # remove all remaining memory logs (TODO need refactoring)
    if config.opt_use_memlog:
      util.remove_all_memory_logs()

    # summary
    errors = 0
    failures = 0
    for ret in results:
      errors += len(ret.errors)
      failures += len(ret.failures)
  
      for e in ret.errors:
        util.log(e[0])
        util.log(e[1])
  
      for f in ret.failures:
        util.log(f[0])
        util.log(f[1])
    util.log("Test done. failures:%d, errors:%d" % (failures, errors))

    if errors > 0 or failures > 0:
      if backup_log_dir is not None:
        for physical_machine in config.physical_machines:
          physical_machine['rpc'].rpc_backup_log( backup_log_dir )
      return -1
    else:
      util.log("No Error!")

    return 0

ret = main(sys.argv[1:])
exit( ret )

