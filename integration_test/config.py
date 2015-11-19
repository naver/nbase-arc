# export PATH=$ZOOKEEPER_HOME/bin:$PATH
import os
import sys

opt_use_memlog = os.path.exists("/tmp/opt_use_memlog")
sudoer = os.getenv("NBASE_ARC_TEST_SUDOER")

physical_machines = [
  {
    'name' : 'localhost',
    'ip' : '127.0.0.1',
    'update_port' : 8000, 
    'proxy_port' : 8004,
    'type' : 'real',
  },

  {
    'name' : 'virtual_localhost',
    'ip' : '127.0.0.1',
    'virtual_ip' :  '127.0.0.100',
    'update_port' : 8000, 
    'proxy_port' : 8004,
    'type' : 'virtual',
  },
  {
    'name' : 'virtual_localhost1',
    'ip' : '127.0.0.1',
    'virtual_ip' :  '127.0.0.101',
    'update_port' : 8000, 
    'proxy_port' : 8004,
    'type' : 'virtual',
  },

  {
    'name' : 'vm2',
    'ip' : '127.0.0.1',
    'virtual_ip' :  '127.0.0.101',
    'update_port' : 8000, 
    'proxy_port' : 8004,
    'type' : 'virtual',
  },

  {
    'name' : 'vm3',
    'ip' : '127.0.0.1',
    'virtual_ip' :  '127.0.0.102',
    'update_port' : 8000, 
    'proxy_port' : 8004,
    'type' : 'virtual',
  }
]

zookeeper_info = [
        {'id':0, 'bin_dir':'$HOME/bin/zk1/bin'},
        {'id':1, 'bin_dir':'$HOME/bin/zk2/bin'},
        {'id':2, 'bin_dir':'$HOME/bin/zk3/bin'},
]

server1  = { 
  'id' : 0,
  'cluster_name' : 'testCluster0',
  'ip' : '127.0.0.1', 
  'pm_name' : 'localhost',
  'cm_port' : 1122,
  'pg_id' : 0,
  'smr_base_port' : 8100,
  'smr_mgmt_port' : 8103,
  'gateway_port' : 8200,
  'gateway_mgmt_port' : 8201,
  'redis_port' : 8109,
  'zk_port' : 2181,
  'rpc' : None,
}

server2 = { 
  'id' : 1,
  'cluster_name' : 'testCluster0',
  'ip' : '127.0.0.1', 
  'pm_name' : 'localhost',
  'cm_port' : 1123,
  'pg_id' : 0,
  'smr_base_port' : 9100,
  'smr_mgmt_port' : 9103,
  'gateway_port' : 9200,
  'gateway_mgmt_port' : 9201,
  'redis_port' : 9109,
  'zk_port' : 2181,
  'rpc' : None,
}

server3 = { 
  'id' : 2,
  'cluster_name' : 'testCluster0',
  'ip' : '127.0.0.1', 
  'pm_name' : 'localhost',
  'cm_port' : 1124,
  'pg_id' : 0,
  'smr_base_port' : 10100,
  'smr_mgmt_port' : 10103,
  'gateway_port' : 10200,
  'gateway_mgmt_port' : 10201,
  'redis_port' : 10109,
  'zk_port' : 2181,
  'rpc' : None,
}

server4  = { 
  'id' : 3,
  'cluster_name' : 'testCluster0',
  'ip' : '127.0.0.1', 
  'pm_name' : 'localhost',
  'cm_port' : 1125,
  'pg_id' : 1,
  'smr_base_port' : 8110,
  'smr_mgmt_port' : 8113,
  'gateway_port' : 8210,
  'gateway_mgmt_port' : 8211,
  'redis_port' : 8119,
  'zk_port' : 2181,
  'rpc' : None,
}

server40  = { 
  'id' : 3,
  'cluster_name' : 'testCluster0',
  'ip' : '127.0.0.1', 
  'pm_name' : 'localhost',
  'cm_port' : 1125,
  'pg_id' : 0,
  'smr_base_port' : 8110,
  'smr_mgmt_port' : 8113,
  'gateway_port' : 8210,
  'gateway_mgmt_port' : 8211,
  'redis_port' : 8119,
  'zk_port' : 2181,
  'rpc' : None,
}

server5 = { 
  'id' : 4,
  'cluster_name' : 'testCluster0',
  'ip' : '127.0.0.1', 
  'pm_name' : 'localhost',
  'cm_port' : 1126,
  'pg_id' : 1,
  'smr_base_port' : 9110,
  'smr_mgmt_port' : 9113,
  'gateway_port' : 9210,
  'gateway_mgmt_port' : 9211,
  'redis_port' : 9119,
  'zk_port' : 2181,
  'rpc' : None,
}


server6 = { 
  'id' : 5,
  'cluster_name' : 'testCluster0',
  'ip' : '127.0.0.1', 
  'pm_name' : 'localhost',
  'cm_port' : 1127,
  'pg_id' : 1,
  'smr_base_port' : 10110,
  'smr_mgmt_port' : 10113,
  'gateway_port' : 10210,
  'gateway_mgmt_port' : 10211,
  'redis_port' : 10119,
  'zk_port' : 2181,
  'rpc' : None,
}

virtual_server1 = { 
  'id' : 0,
  'cluster_name' : 'network_isolation_cluster',
  'ip' : '127.0.0.100', 
  'real_ip' : '127.0.0.1', 
  'pm_name' : 'virtual_localhost',
  'cm_port' : 1122,
  'pg_id' : 0,
  'smr_base_port' : 8100,
  'smr_mgmt_port' : 8103,
  'gateway_port' : 8200,
  'gateway_mgmt_port' : 8201,
  'redis_port' : 8109,
  'zk_port' : 2181,
  'rpc' : None,
}

virtual_server2 = { 
  'id' : 1,
  'cluster_name' : 'network_isolation_cluster',
  'ip' : '127.0.0.100', 
  'real_ip' : '127.0.0.1', 
  'pm_name' : 'virtual_localhost',
  'cm_port' : 1123,
  'pg_id' : 0,
  'smr_base_port' : 9100,
  'smr_mgmt_port' : 9103,
  'gateway_port' : 9200,
  'gateway_mgmt_port' : 9201,
  'redis_port' : 9109,
  'zk_port' : 2181,
  'rpc' : None,
}

virtual_server21 = { 
  'id' : 1,
  'cluster_name' : 'network_isolation_cluster',
  'ip' : '127.0.0.100', 
  'real_ip' : '127.0.0.1', 
  'pm_name' : 'virtual_localhost',
  'cm_port' : 1123,
  'pg_id' : 1,
  'smr_base_port' : 9100,
  'smr_mgmt_port' : 9103,
  'gateway_port' : 9200,
  'gateway_mgmt_port' : 9201,
  'redis_port' : 9109,
  'zk_port' : 2181,
  'rpc' : None,
}

virtual_server3 = { 
  'id' : 2,
  'cluster_name' : 'network_isolation_cluster',
  'ip' : '127.0.0.100', 
  'real_ip' : '127.0.0.1', 
  'pm_name' : 'virtual_localhost',
  'cm_port' : 1124,
  'pg_id' : 0,
  'smr_base_port' : 10100,
  'smr_mgmt_port' : 10103,
  'gateway_port' : 10200,
  'gateway_mgmt_port' : 10201,
  'redis_port' : 10109,
  'zk_port' : 2181,
  'rpc' : None,
}

virtual_server4 = { 
  'id' : 3,
  'cluster_name' : 'network_isolation_cluster',
  'ip' : '127.0.0.101', 
  'real_ip' : '127.0.0.1', 
  'pm_name' : 'virtual_localhost1',
  'cm_port' : 1125,
  'pg_id' : 1,
  'smr_base_port' : 8110,
  'smr_mgmt_port' : 8113,
  'gateway_port' : 8210,
  'gateway_mgmt_port' : 8211,
  'redis_port' : 8119,
  'zk_port' : 2181,
  'rpc' : None,
}

virtual_server5 = { 
  'id' : 4,
  'cluster_name' : 'network_isolation_cluster',
  'ip' : '127.0.0.101', 
  'real_ip' : '127.0.0.1', 
  'pm_name' : 'virtual_localhost1',
  'cm_port' : 1126,
  'pg_id' : 1,
  'smr_base_port' : 9110,
  'smr_mgmt_port' : 9113,
  'gateway_port' : 9210,
  'gateway_mgmt_port' : 9211,
  'redis_port' : 9119,
  'zk_port' : 2181,
  'rpc' : None,
}

virtual_server6 = { 
  'id' : 5,
  'cluster_name' : 'network_isolation_cluster',
  'ip' : '127.0.0.101', 
  'real_ip' : '127.0.0.1', 
  'pm_name' : 'virtual_localhost1',
  'cm_port' : 1127,
  'pg_id' : 1,
  'smr_base_port' : 10110,
  'smr_mgmt_port' : 10113,
  'gateway_port' : 10210,
  'gateway_mgmt_port' : 10211,
  'redis_port' : 10119,
  'zk_port' : 2181,
  'rpc' : None,
}

vm1 = { 
  'id' : 0,
  'cluster_name' : 'no_opinion',
  'ip' : '127.0.0.100', 
  'real_ip' : '127.0.0.1', 
  'pm_name' : 'virtual_localhost',
  'cm_port' : 1122,
  'pg_id' : 0,
  'smr_base_port' : 8100,
  'smr_mgmt_port' : 8103,
  'gateway_port' : 8200,
  'gateway_mgmt_port' : 8201,
  'redis_port' : 8109,
  'zk_port' : 2181,
  'rpc' : None,
}

vm2 = { 
  'id' : 1,
  'cluster_name' : 'no_opinion',
  'ip' : '127.0.0.101', 
  'real_ip' : '127.0.0.1', 
  'pm_name' : 'vm2',
  'cm_port' : 1123,
  'pg_id' : 0,
  'smr_base_port' : 9100,
  'smr_mgmt_port' : 9103,
  'gateway_port' : 9200,
  'gateway_mgmt_port' : 9201,
  'redis_port' : 9109,
  'zk_port' : 2181,
  'rpc' : None,
}

vm3 = { 
  'id' : 2,
  'cluster_name' : 'no_opinion',
  'ip' : '127.0.0.102', 
  'real_ip' : '127.0.0.1', 
  'pm_name' : 'vm3',
  'cm_port' : 1124,
  'pg_id' : 0,
  'smr_base_port' : 10100,
  'smr_mgmt_port' : 10103,
  'gateway_port' : 10200,
  'gateway_mgmt_port' : 10201,
  'redis_port' : 10109,
  'zk_port' : 2181,
  'rpc' : None,
}

clusters = [
  # 0
  {
    'cluster_name' : 'testCluster0',
    'keyspace_size' : 8192,
    'quorum_policy' : '0:1',
    'slots' : [0,8191],
    'pg_id_list' : [0],
    'servers' : [server1, server2, server3]
  },

  # 1
  { 
    'cluster_name' : 'testCluster0',
    'keyspace_size' : 8192,
    'quorum_policy' : '0:1',
    'slots' : [0,8191, -1, -1],
    'pg_id_list' : [0, 1],
    'servers' : [server1, server2, server3, server4, server5, server6]
  },

  # 2
  { 
    'cluster_name' : 'testCluster0',
    'keyspace_size' : 8192,
    'quorum_policy' : '0:1',
    'slots' : [0,4095,4096,8191],
    'pg_id_list' : [0, 1],
    'servers' : [server1, server2, server3, server4, server5, server6]
  },

  # 3
  { 
    'cluster_name' : 'testCluster0',
    'keyspace_size' : 8192,
    'quorum_policy' : '0:1',
    'slots' : [0,8191],
    'pg_id_list' : [0],
    'servers' : [server1, server2]
  },

  # 4
  { 
    'cluster_name' : 'testCluster0',
    'keyspace_size' : 8192,
    'quorum_policy' : '0:1',
    'slots' : [0,8191, -1, -1],
    'pg_id_list' : [0, 1],
    'servers' : [server1, server2, server3, server4, server5, server6]
  },

  # 5
  {
    'cluster_name' : 'network_isolation_cluster_1',
    'keyspace_size' : 8192,
    'quorum_policy' : '0:1',
    'slots' : [0,4095,4096,8191],
    'pg_id_list' : [0, 1],
    'servers' : [virtual_server1, virtual_server2, virtual_server3, server4, server5, server6]
  },

  # 6
  {
    'cluster_name' : 'network_isolation_cluster_2',
    'keyspace_size' : 8192,
    'quorum_policy' : '0:1:2',
    'slots' : [0,8191],
    'pg_id_list' : [0],
    'servers' : [virtual_server1, server2, virtual_server3]
  },

  # 7
  {
    'cluster_name' : 'network_isolation_cluster_1_2copy',
    'keyspace_size' : 8192,
    'quorum_policy' : '0:1',
    'slots' : [0,4095,4096,8191],
    'pg_id_list' : [0, 1],
    'servers' : [server40, virtual_server1, server5, virtual_server21]
  },

  # 8
  {
    'cluster_name' : 'no_opinion',
    'keyspace_size' : 8192,
    'quorum_policy' : '0:1:2',
    'slots' : [0,8191],
    'pg_id_list' : [0],
    'servers' : [vm1, vm2, vm3]
  },

  # 9
  {
    'cluster_name' : 'network_isolation_cluster_3',
    'keyspace_size' : 8192,
    'quorum_policy' : '0:1',
    'slots' : [0,4095,4096,8191],
    'pg_id_list' : [0, 1],
    'servers' : [virtual_server1, virtual_server2, virtual_server3, virtual_server4, virtual_server5, virtual_server6]
  },
]

def verify_config():
  print "### Verify config ###"
  if opt_use_memlog == None:
    print "  Invalid opt_use_memlog."
    sys.exit(-1)
  else:
    print "  opt_use_memlog : %s" % opt_use_memlog 

  if sudoer == None:
    print "  Invalid sudoer. Check environmental variable, NBASE_ARC_TEST_SUDOER."
    sys.exit(-1)
  else:
    print "  sudoer : %s" % sudoer

