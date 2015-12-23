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
import gateway_mgmt
import redis_mgmt
import default_cluster
import config
import load_generator
import telnet
import random


class TestRestartRecovery( unittest.TestCase ):
    cluster = config.clusters[0]
    leader_cm = config.clusters[0]['servers'][0]
    max_load_generator = 1
    load_gen_thrd_list = {}

    @classmethod
    def setUpClass( cls ):
        return 0

    @classmethod
    def tearDownClass( cls ):
        return 0

    def setUp( self ):
        util.set_process_logfile_prefix( 'TestRestartRecovery_%s' % self._testMethodName )
        ret = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
        if ret is not 0:
            default_cluster.finalize( self.cluster )
        self.assertEquals( ret, 0, 'failed to test_restart_recovery.initialize' )

    def tearDown( self ):
        ret = default_cluster.finalize( self.cluster )
        self.assertEquals( ret, 0, 'failed to test_restart_recovery.finalize' )
        return 0

    def failure_recovery( self, role, wait_count=10, redis_only=False ):
        time.sleep( 2 )

        # get gateway info
        ip, port = util.get_rand_gateway( self.cluster )
        gw = gateway_mgmt.Gateway( self.cluster['servers'][0]['id'] )
        ret = gw.connect( ip, port )
        self.assertEqual( ret, 0, 'failed to connect to gateway, %s:%d' % (ip, port) )

        # set value
        key = 'new_key_haha'
        cmd = 'set %s 12345\r\n' % (key)
        gw.write( cmd )
        res = gw.read_until( '\r\n' )
        self.assertEquals( res, '+OK\r\n' )

        # shutdown
        server = util.get_server_by_role( self.cluster['servers'], role )

        if redis_only == False:
            ret = testbase.request_to_shutdown_smr( server )
            self.assertEqual( ret, 0, 'failed to shutdown smr' )

        ret = testbase.request_to_shutdown_redis( server )
        self.assertEquals( ret, 0, 'failed to shutdown redis' )

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

        # set value
        check_value = '54321'
        cmd = 'set %s %s\r\n' % (key, check_value)
        gw.write( cmd )
        res = gw.read_until( '\r\n' )
        self.assertEquals( res, '+OK\r\n' )
        gw.disconnect()

        # recovery
        if redis_only == False:
            ret = testbase.request_to_start_smr( server )
            self.assertEqual( ret, 0, 'failed to start smr' )

        ret = testbase.request_to_start_redis( server )
        self.assertEqual( ret, 0, 'failed to start redis' )

        ret = testbase.wait_until_finished_to_set_up_role( server, wait_count )
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
        role = util.get_role_of_server( server )
        self.assertEquals( expected , state,
                           'server%d - state:%s, expected:%s, role:%s' % (server['id'], state, expected, role) )

        # check value
        cmd = 'get %s\r\n' % (key)
        redis.write( cmd )
        redis.read_until( '\r\n'  )
        response = redis.read_until( '\r\n'  )
        self.assertEqual( response, '%s\r\n' % (check_value), 'inconsistent %s, %s' % (response, check_value) )

    def test_1_slave_failure_recovery( self ):
        util.print_frame()
        self.failure_recovery( 'slave' )

    def test_2_master_failure_recovery( self ):
        util.print_frame()
        self.failure_recovery( 'master' )

    def test_3_repeated_failure_recovery( self ):
        util.print_frame()
        for i in range( 0, 3 ):
            self.failure_recovery( 'master' )
            util.log( 'succeeded to failure_recovery, role:master, cnt:%d' % i )
            self.failure_recovery( 'slave' )
            util.log( 'succeeded to failure_recovery, role:slave, cnt:%d' % i )

    def failover( self, server ):
        # shutdown
        ret = testbase.request_to_shutdown_smr( server )
        self.assertEqual( ret, 0, 'failed to shutdown smr' )
        ret = testbase.request_to_shutdown_redis( server )
        self.assertEquals( ret, 0, 'failed to shutdown redis' )

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

        # recovery
        ret = testbase.request_to_start_smr( server )
        self.assertEqual( ret, 0, 'failed to start smr' )

        ret = testbase.request_to_start_redis( server )
        self.assertEqual( ret, 0, 'failed to start redis' )

        ret = testbase.wait_until_finished_to_set_up_role( server, 10 )
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
        role = util.get_role_of_server( server )
        self.assertEquals( expected , state,
                           'server%d - state:%s, expected:%s, role:%s' % (server['id'], state, expected, role) )

    """
    MGEN 2       3         4         5
    PGS0 M ----> X - - - - - - - - - - - - > O S
    PGS1 S ----> M ----> X S ----> O M ---->   M
    PGS2 S ----> S ---->   M ----> X S ----> O S
    """
    def test_4_PGS_mgen_is_less_than_PG_mgen( self ):
        util.print_frame()

        # get gateway info
        ip, port = util.get_rand_gateway( self.cluster )
        gw = gateway_mgmt.Gateway( self.cluster['servers'][0]['id'] )
        ret = gw.connect( ip, port )
        self.assertEqual( ret, 0, 'failed to connect to gateway, %s:%d' % (ip, port) )

        # initial data
        util.put_some_data(self.cluster)

        # shutdown
        server_to_join = util.get_server_by_role( self.cluster['servers'], 'master' )
        ret = testbase.request_to_shutdown_smr( server_to_join )
        self.assertEqual( ret, 0, 'failed to shutdown smr' )
        ret = testbase.request_to_shutdown_redis( server_to_join )
        self.assertEquals( ret, 0, 'failed to shutdown redis' )

        # check state F
        max_try = 20
        expected = 'F'
        for i in range( 0, max_try):
            state = util.get_smr_state( server_to_join, self.leader_cm )
            if expected == state:
                break;
            time.sleep( 1 )
        self.assertEquals( expected , state,
                           'server%d - state:%s, expected:%s' % (server_to_join['id'], state, expected) )

        # set value
        key_base = 'mw'
        for i in range(0, 10000):
            cmd = 'set %s%d %d\r\n' % (key_base, i, i)
            gw.write( cmd )
            res = gw.read_until( '\r\n' )
            self.assertEquals( res, '+OK\r\n' )

        # master failover 1 (master generation + 1)
        util.log('master failover 1')
        server = util.get_server_by_role( self.cluster['servers'], 'master' )
        self.failover( server )

        # check quorum (copy:3, quorum:1, available:2)
        ok = False
        for i in xrange(10):
            ok = util.check_quorum(self.cluster['cluster_name'],
                    self.leader_cm['ip'], self.leader_cm['cm_port'])
            if ok:
                break
            else:
                time.sleep(1)
        self.assertTrue( ok, 'Check quorum fail.' )

        # master failover 2 (master generation + 1)
        util.log('master failover 2')
        server = util.get_server_by_role( self.cluster['servers'], 'master' )
        self.failover( server )

        # recovery
        util.log('master recovery start.')
        ret = testbase.request_to_start_smr( server_to_join )
        self.assertEqual( ret, 0, 'failed to start smr' )

        ret = testbase.request_to_start_redis( server_to_join )
        self.assertEqual( ret, 0, 'failed to start redis' )

        ret = testbase.wait_until_finished_to_set_up_role( server_to_join, 10 )
        self.assertEquals( ret, 0, 'failed to role change. smr_id:%d' % (server_to_join['id']) )
        util.log('master recovery end successfully.')

        # check state N
        max_try = 20
        expected = 'N'
        for i in range( 0, max_try):
            state = util.get_smr_state( server, self.leader_cm )
            if expected == state:
                break;
            time.sleep( 1 )
        role = util.get_role_of_server( server )
        self.assertEquals( expected , state,
                           'server%d - state:%s, expected:%s, role:%s' % (server['id'], state, expected, role) )

        time.sleep( 5 )

        # set value
        for i in range(10000, 20000):
            cmd = 'set %s%d %d\r\n' % (key_base, i, i)
            gw.write( cmd )
            res = gw.read_until( '\r\n' )
            self.assertEquals( res, '+OK\r\n' )

        server = util.get_server_by_role( self.cluster['servers'], 'master' )

        redis = redis_mgmt.Redis( server_to_join['id'] )
        ret = redis.connect( server_to_join['ip'], server_to_join['redis_port'] )
        self.assertEquals( ret, 0, 'failed to connect to redis' )

        # check value
        for i in range(0, 20000):
            cmd = 'get %s%d\r\n' % (key_base, i)
            redis.write( cmd )
            redis.read_until( '\r\n'  )
            response = redis.read_until( '\r\n'  )
            self.assertEqual( response, '%d\r\n' % (i), 'inconsistent %s, %d' % (response[:-2], i) )

        gw.disconnect()
        return 0

    """
    MGEN 2                                3
    PGS0 M   -----------------------> X - - -> O F
    PGS1 S X - - - - - - - - - - - - -> O M ------->
    PGS2 S X - - - - - - - - - - - - -> O S ------->

    deprecated : In this scenario, copy-quorum is transit, 3-1I(MSS) -> 3-1(XMS) -> 3-0(MXX)
                 It try to elect a master with only one available pgs
                 but, it is not able, because copy-quorum is 3-1 and available pgs is only one.
    """
    def deprecated_test_5_PGS_commit_is_greater_than_PG_commit( self ):
        util.print_frame()

        # get gateway info
        ip, port = util.get_rand_gateway( self.cluster )
        gw = gateway_mgmt.Gateway( self.cluster['servers'][0]['id'] )
        ret = gw.connect( ip, port )
        self.assertEqual( ret, 0, 'failed to connect to gateway, %s:%d' % (ip, port) )

        # initial data
        util.put_some_data(self.cluster)

        master, s1, s2 = util.get_mss(self.cluster)

        server_to_join = [s1, s2]
        # shutdown slaves
        for i in range(0, 2):
            ret = testbase.request_to_shutdown_smr( server_to_join[i] )
            self.assertEqual( ret, 0, 'failed to shutdown smr%d' % server_to_join[i]['id'])
            util.log('succeeded to shutdown smr%d' % server_to_join[i]['id'])

            ret = testbase.request_to_shutdown_redis( server_to_join[i] )
            self.assertEquals( ret, 0, 'failed to shutdown redis' )
            util.log('succeeded to shutdown redis%d' % server_to_join[i]['id'])

            # check state F
            max_try = 20
            expected = 'F'
            for j in range( 0, max_try):
                state = util.get_smr_state( server_to_join[i], self.leader_cm )
                if expected == state:
                    break;
                time.sleep( 1 )
            self.assertEquals( expected , state,
                               'server%d - state:%s, expected:%s' % (server_to_join[i]['id'], state, expected) )

        # put more data
        util.put_some_data(self.cluster, 10, 256)

        # bgsave
        ret = util.bgsave(master)
        self.assertTrue(ret, 'failed to bgsave. pgs%d' % master['id'])

        # shutdown master
        ret = testbase.request_to_shutdown_smr( master )
        self.assertEqual( ret, 0, 'failed to shutdown smr' )
        util.log('succeeded to shutdown master smr, id=%d' % master['id'])
        ret = testbase.request_to_shutdown_redis( master )
        self.assertEquals( ret, 0, 'failed to shutdown redis' )
        util.log('succeeded to shutdown master redis, id=%d' % master['id'])

        # check state F
        max_try = 20
        expected = 'F'
        for i in range( 0, max_try):
            state = util.get_smr_state( master, self.leader_cm )
            if expected == state:
                break;
            time.sleep( 1 )
        self.assertEquals( expected , state,
                           'server%d - state:%s, expected:%s' % (master['id'], state, expected) )

        # recovery slaves
        for i in range(0, 2):
            ret = testbase.request_to_start_smr( server_to_join[i] )
            self.assertEqual( ret, 0, 'failed to start smr' )

            ret = testbase.request_to_start_redis( server_to_join[i] )
            self.assertEqual( ret, 0, 'failed to start redis' )

            ret = testbase.wait_until_finished_to_set_up_role( server_to_join[i], 10 )
            self.assertEquals( ret, 0, 'failed to role change. smr_id:%d' % (server_to_join[i]['id']) )

            # check state N
            max_try = 20
            expected = 'N'
            for j in range( 0, max_try):
                state = util.get_smr_state( server_to_join[i], self.leader_cm )
                if expected == state:
                    break;
                time.sleep( 1 )
            role = util.get_role_of_server( server_to_join[i] )
            self.assertEquals( expected , state,
                               'server%d - state:%s, expected:%s, role:%s' % (server_to_join[i]['id'], state, expected, role) )

        # set value
        s = random.choice(server_to_join)
        redis = redis_mgmt.Redis( ['id'] )
        ret = redis.connect( s['ip'], s['redis_port'] )
        self.assertEquals( ret, 0, 'failed to connect to redis' )

        key_base = 'key_test'
        for i in range(0, 10000):
            cmd = 'set %s%d %d\r\n' % (key_base, i, i)
            redis.write( cmd )
            res = redis.read_until( '\r\n' )
            self.assertEquals( res, '+OK\r\n' )
        redis.disconnect()

        for i in range(0, 2):
            redis = redis_mgmt.Redis( server_to_join[i]['id'] )
            ret = redis.connect( server_to_join[i]['ip'], server_to_join[i]['redis_port'] )
            self.assertEquals( ret, 0, 'failed to connect to redis' )

            # check value
            for j in range(0, 10000):
                cmd = 'get %s%d\r\n' % (key_base, j)
                redis.write( cmd )
                redis.read_until( '\r\n'  )
                response = redis.read_until( '\r\n'  )
                self.assertEqual( response, '%d\r\n' % (j), 'inconsistent %s, %d' % (response[:-2], j) )

        # try to recover master, but failed
        ret = testbase.request_to_start_smr( master )
        self.assertEqual( ret, 0, 'failed to start smr' )

        ret = testbase.request_to_start_redis( master, False )
        self.assertEqual( ret, 0, 'failed to start redis' )

        max_try = 3
        expected = 'N'
        for i in range( 0, max_try):
            state = util.get_smr_state( master, self.leader_cm )
            if expected == state:
                break;
            time.sleep( 1 )
        role = util.get_role_of_server( master )
        self.assertNotEqual( expected, state,
                             'server%d - state:%s, expected:not %s, role:%s' % (master['id'], state, expected, role) )
        util.log('success : the old master that has a greater commit-seq than the current master tried to join as a slave, but it is blocked successfully.')

        gw.disconnect()
        return 0

    def test_5_repeated_redis_failure_recovery( self ):
        util.print_frame()
        for i in range( 0, 10 ):
            print 'loop : %d' % i
            role = random.choice(['master', 'slave'])
            self.failure_recovery( role, redis_only=True )
            util.log( 'succeeded to failure_recovery, role:%s, cnt:%d' % (role, i) )

            if role == 'master':
                role = 'slave'
            else:
                role = 'master'
            self.failure_recovery( role, redis_only=True )
            util.log( 'succeeded to failure_recovery, role:%s, cnt:%d' % (role, i) )

    def test_6_redis_or_pgs_failover_but_not_both_at_once( self ):
        util.print_frame()
        for i in range( 0, 10 ):
            print 'loop : %d' % i
            role = random.choice(['master', 'slave'])
            redis_failover = random.choice([True, False])
            util.log( 'start failover, role:%s, redis_failover:%s' % (role, redis_failover) )
            self.failure_recovery( role, redis_only=redis_failover )
            util.log( 'succeeded to failure_recovery, role:%s, cnt:%d' % (role, i) )

            if role == 'master':
                role = 'slave'
            else:
                role = 'master'
            self.failure_recovery( role, redis_only=redis_failover )
            util.log( 'succeeded to failure_recovery, role:%s, cnt:%d' % (role, i) )
