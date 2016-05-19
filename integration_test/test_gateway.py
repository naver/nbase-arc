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

import unittest
import testbase
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
import json


class TestGateway( unittest.TestCase ):
    cluster = config.clusters[0]
    leader_cm = config.clusters[0]['servers'][0]
    key_base = 'test_gateway_key'

    @classmethod
    def setUpClass( cls ):
        ret = default_cluster.initialize_starting_up_smr_before_redis( cls.cluster )
        if ret is not 0:
            util.log( 'failed to initialize_starting_up_smr_before_redis in TestUpgrade' )
            default_cluster.finalize( cls.cluster )
        return 0

    @classmethod
    def tearDownClass( cls ):
        ret = default_cluster.finalize( cls.cluster )
        return 0

    def setUp( self ):
        util.set_process_logfile_prefix( 'TestGateway_%s' % self._testMethodName )
        return 0

    def tearDown( self ):
        return 0

    def test_pgs_add_and_del_repeatedly( self ):
       util.print_frame()

       execution_count_master = 0
       execution_count_slave = 0
       old_target = None
       for cnt in range( 50 ):
            target = random.choice( self.cluster['servers'] )
            while target == old_target:
                target = random.choice( self.cluster['servers'] )
            old_target = target

            role = util.get_role_of_server( target )
            if role == c.ROLE_SLAVE:
                self.pgs_add_and_del( target, 'slave' )
                execution_count_master = execution_count_master + 1
            elif role == c.ROLE_MASTER:
                self.pgs_add_and_del( target, 'master' )
                execution_count_slave = execution_count_slave + 1
            else:
                self.fail( 'unexpected role:%s' % role )

    def pgs_add_and_del( self, upgrade_server, type ):
        util.print_frame()

        util.log( '[start] add and del pgs%d. type:%s' % (upgrade_server['id'], type) )
        util.log_server_state( self.cluster )

        # start load generator
        load_gen_list = {}
        for i in range( len(self.cluster['servers']) ):
            server = self.cluster['servers'][i]
            load_gen = load_generator.LoadGenerator(server['id'], server['ip'], server['gateway_port'])
            load_gen.start()
            load_gen_list[i] = load_gen

        # detach pgs from cluster
        cmd = 'pgs_leave %s %d\r\n' % (upgrade_server['cluster_name'], upgrade_server['id'])
        ret = util.cm_command( self.leader_cm['ip'], self.leader_cm['cm_port'], cmd )
        jobj = json.loads(ret)
        self.assertEqual( jobj['msg'], '+OK', 'failed : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )
        util.log( 'succeeded : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )

        # set new values
        ip, port = util.get_rand_gateway(self.cluster)
        gw = gateway_mgmt.Gateway( '0' )
        gw.connect( ip, port )
        for i in range( 0, 50 ):
            cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
            gw.write( cmd )
            res = gw.read_until( '\r\n' )
            self.assertEqual( res, '+OK\r\n', 'failed to set values to gw(%s:%d). cmd:%s, res:%s' % (ip, port, cmd[:-2], res[:-2]) )

        # attach pgs to cluster
        cmd = 'pgs_join %s %d\r\n' % (upgrade_server['cluster_name'], upgrade_server['id'])
        ret = util.cm_command( self.leader_cm['ip'], self.leader_cm['cm_port'], cmd )
        jobj = json.loads(ret)
        self.assertEqual( jobj['msg'], '+OK', 'failed : cmd="%s", reply="%s"' % (cmd[:-2], ret) )
        util.log( 'succeeded : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )
        time.sleep( 3 )

        stable = False
        for i in xrange(20):
            stable = util.check_cluster(self.cluster['cluster_name'], self.leader_cm['ip'], self.leader_cm['cm_port'])
            if stable:
                break;
            time.sleep(0.5)
        self.assertTrue(stable, 'Unstable cluster')

        # check new values
        redis = redis_mgmt.Redis( upgrade_server['id'] )
        ret = redis.connect( upgrade_server['ip'], upgrade_server['redis_port'] )
        self.assertEquals( ret, 0, 'failed : connect to smr%d(%s:%d)' % (upgrade_server['id'], upgrade_server['ip'], upgrade_server['redis_port']) )

        for i in range( 0, 50 ):
            cmd = 'get %s%d\r\n' % (self.key_base, i)
            redis.write( cmd )
            redis.read_until( '\r\n' )
            res = redis.read_until( '\r\n' )
            self.assertEqual( res, '%d\r\n' % i, 'failed to get values from redis%d. %s != %d' % (upgrade_server['id'], res, i) )
        util.log( 'succeeded : check values with get operations on pgs%d.' % (upgrade_server['id']) )

        # shutdown load generators
        for i in range( len(load_gen_list) ):
            load_gen_list[i].quit()
            load_gen_list[i].join()

        util.log_server_state( self.cluster )

        return 0

    def test_gateway_lookup( self ):
        util.print_frame()

        # check initial states
        for server in self.cluster['servers']:
            success = False
            for try_cnt in range( 5 ):
                cmd = 'get /RC/NOTIFICATION/CLUSTER/%s/GW/%d' % (self.cluster['cluster_name'], server['id'])
                ret = util.zk_cmd( cmd )
                ret = ret['err']
                if -1 != ret.find('cZxid'):
                    success = True
                    break
                time.sleep( 1 )

            self.assertEqual( success, True, 'failed : cmd="%s", ret="%s"' % (cmd, ret) )
            util.log( 'succeeded : cmd="%s", ret="%s"' % (cmd, ret) )

        # shutdown gateway
        for server in self.cluster['servers']:
            success = False

            ret = util.shutdown_gateway( server['id'], server['gateway_port'] )
            self.assertEqual( ret, 0, 'failed : shutdown gateawy%d' % server['id'] )

            for try_cnt in range( 10 ):
                cmd = 'get /RC/NOTIFICATION/CLUSTER/%s/GW/%d' % (self.cluster['cluster_name'], server['id'])
                ret = util.zk_cmd( cmd )
                ret = ret['err']
                if -1 != ret.find('Node does not exist'):
                    success = True
                    break
                time.sleep( 1 )

            self.assertEqual( success, True, 'failed : cmd="%s", ret="%s"' % (cmd, ret) )
            util.log( 'succeeded : cmd="%s", ret="%s"' % (cmd, ret) )

        # restart gateway
        for server in self.cluster['servers']:
            success = False

            ret = util.start_gateway( server['id'], server['ip'], self.leader_cm['cm_port'], server['cluster_name'], server['gateway_port'])
            self.assertEqual( ret, 0, 'failed : start gateawy%d' % server['id'] )

            for try_cnt in range( 5 ):
                cmd = 'get /RC/NOTIFICATION/CLUSTER/%s/GW/%d' % (self.cluster['cluster_name'], server['id'])
                ret = util.zk_cmd( cmd )
                ret = ret['err']
                if -1 != ret.find('cZxid'):
                    success = True
                    break
                time.sleep( 1 )

            self.assertEqual( success, True, 'failed : cmd="%s", ret="%s"' % (cmd, ret) )
            util.log( 'succeeded : cmd="%s", ret="%s"' % (cmd, ret) )

        return 0
