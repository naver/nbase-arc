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
import smr_mgmt
import default_cluster
import config
import load_generator
import telnet
import json
import constant as c


class TestHeartbeatChecker( unittest.TestCase ):
    cluster = config.clusters[0]
    leader_cm = config.clusters[0]['servers'][0]
    max_load_generator = 1
    load_gen_thrd_list = {}
    key_base = 'key_thbc'

    @classmethod
    def setUpClass( cls ):
        return 0

    @classmethod
    def tearDownClass( cls ):
        return 0

    def setUp( self ):
        util.set_process_logfile_prefix( 'TestHeartbeatChecker_%s' % self._testMethodName )
        ret = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
        if ret is not 0:
            default_cluster.finalize( self.cluster )
        self.assertEquals( ret, 0, 'failed to TestHeartbeatChecker.initialize' )

    def tearDown( self ):
        ret = default_cluster.finalize( self.cluster )
        self.assertEquals( ret, 0, 'failed to TestHeartbeatChecker.finalize' )
        return 0

    def getseq_log(self, s):
        smr = smr_mgmt.SMR( s['id'] )
        try:
            ret = smr.connect( s['ip'], s['smr_mgmt_port'] )
            if ret != 0:
                return

            smr.write( 'getseq log\r\n' )
            response = smr.read_until( '\r\n', 1 )
            util.log('getseq log (pgs%d) = %s' % (s['id'], response[:-2]))
            smr.disconnect()
        except IOError:
            pass

    def get_expected_smr_state( self, server, expected, max_try=60 ):
        for i in range( 0, max_try ):
            state = util.get_smr_state( server, self.leader_cm )
            if state == expected:
                break;
            time.sleep( 1 )
        return state

    def state_transition( self ):
        server = util.get_server_by_role( self.cluster['servers'], 'slave' )
        self.assertNotEquals( server, None, 'failed to get_server_by_role-slave' )

        # get gateway info
        ip, port = util.get_rand_gateway( self.cluster )
        gw = gateway_mgmt.Gateway( self.cluster['servers'][0]['id'] )

        # check initial state
        state = self.get_expected_smr_state( server, 'N' )
        role = util.get_role_of_server( server )
        self.assertEquals( 'N', state,
                           'server%d - state:%s, role:%s, expected:N' % (server['id'], state, role) )

        # shutdown
        ret = testbase.request_to_shutdown_smr( server )
        self.assertEquals( ret, 0, 'failed to shutdown smr' )
        ret = testbase.request_to_shutdown_redis( server )
        self.assertEquals( ret, 0, 'failed to shutdown redis' )
        time.sleep( 3 )


        # check state F
        expected = 'F'
        state = self.get_expected_smr_state( server, expected )
        self.assertEquals( expected , state,
                           'server%d - state:%s, but expected:%s' % (server['id'], state, expected) )

        # set value
        ret = gw.connect( ip, port )
        self.assertEquals( ret, 0, 'failed to connect to gateway, %s:%d' % (ip, port) )
        timestamp  = 0.0
        for i in range( 0, 100 ):
            timestamp = time.time()
            key = 'new_key_haha'
            cmd = 'set %s %f\r\n' % (key, timestamp)
            gw.write( cmd )
            res = gw.read_until( '\r\n' )
            self.assertEquals( res, '+OK\r\n' )
        gw.disconnect()

        # recovery
        ret = testbase.request_to_start_smr( server )
        self.assertEquals( ret, 0, 'failed to start smr' )

        ret = testbase.request_to_start_redis( server )
        self.assertEquals( ret, 0, 'failed to start redis' )

        ret = testbase.wait_until_finished_to_set_up_role( server, 10 )
        self.assertEquals( ret, 0, 'failed to role change. smr_id:%d' % (server['id']) )
        time.sleep( 5 )

        redis = redis_mgmt.Redis( server['id'] )
        ret = redis.connect( server['ip'], server['redis_port'] )
        self.assertEquals( ret, 0, 'failed to connect to redis' )

        # check state N
        expected = 'N'
        max_try = 20
        for i in range( 0, max_try ):
            state = self.get_expected_smr_state( server, expected )
            if state == expected:
                break
            time.sleep( 1 )
        role = util.get_role_of_server( server )
        self.assertEquals( expected , state,
                           'server%d - state:%s, role:%s, but expected:%s' % (server['id'], state, role, expected) )

    def test_1_state_transition( self ):
        util.print_frame()
        self.state_transition()

    def get_mss( self ):
        # get master, slave1, and slave2
        master = util.get_server_by_role( self.cluster['servers'], 'master' )
        self.assertNotEquals( master, None, 'failed to get master' )

        slave1 = util.get_server_by_role( self.cluster['servers'], 'slave' )
        self.assertNotEquals( slave1, None, 'failed to get slave1' )

        slave2 = None
        for server in self.cluster['servers']:
            id = server['id']
            if id != master['id'] and id != slave1['id']:
                slave2 = server
                break
        self.assertNotEquals( slave2, None, 'failed to get slave2' )

        return master, slave1, slave2

    def test_2_consistent_after_failover( self ):
        util.print_frame()
        for i in range(3):
            util.log('loop %d' % i)

            self.consistent_after_failover()

    def consistent_after_failover( self ):
        max = 10000
        wait_count = 15
        key = 'caf'

        # get master, slave1, and slave2
        master, slave1, slave2 = self.get_mss()

        # set value
        ip, port = util.get_rand_gateway( self.cluster )
        gw = gateway_mgmt.Gateway( ip )
        gw.connect( ip, port )

        for i in range( 0, max ):
            cmd = 'set %s%d %d\r\n' % (key, i, i)
            gw.write( cmd )
            res = gw.read_until( '\r\n' )
            self.assertEquals( res, '+OK\r\n' )
        time.sleep( 5 )

        # shutdown
        servers = [master, slave1, slave2]
        for server in servers:

            util.log('before shutdown pgs%d' % server['id'])
            for s in servers:
                self.getseq_log(s)

            ret = testbase.request_to_shutdown_smr( server )
            self.assertEqual( ret, 0, 'failed to shutdown smr, server:%d' % server['id'] )
            ret = testbase.request_to_shutdown_redis( server )
            self.assertEquals( ret, 0, 'failed to shutdown redis' )
        time.sleep( 5 )

        # check state F
        for server in servers:
            state = self.get_expected_smr_state( server, 'F' )
            self.assertEquals( 'F', state,
                               'server%d - state:%s' % (server['id'], state) )

        # recovery
        for server in servers:
            ret = testbase.request_to_start_smr( server )
            self.assertEqual( ret, 0, 'failed to start smr, server:%d' % server['id'] )

            ret = testbase.request_to_start_redis( server, False )
            self.assertEqual( ret, 0, 'failed to start redis, server:%d' % server['id']  )

            util.log('after restart pgs%d' % server['id'])
            for s in servers:
                self.getseq_log(s)

        time.sleep( 5 )

        # wait for master election
        for i in xrange(10):
            ret = util.check_cluster( self.cluster['cluster_name'], self.leader_cm['ip'], self.leader_cm['cm_port'] )
            if ret:
                break
            time.sleep(1)

        # check state
        for server in servers:
            ret = testbase.wait_until_finished_to_set_up_role( server, wait_count )
            self.assertEquals( ret, 0, 'failed to role change. server:%d' % (server['id']) )

            state = self.get_expected_smr_state( server, 'N' )
            role = util.get_role_of_server( server )
            self.assertEquals( 'N', state,
                               'server%d - state:%s, role:%s' % (server['id'], state, role) )

        the_number_of_master = 0
        the_number_of_slave = 0
        for server in servers:
            role = util.get_role_of_server( server )
            if role == c.ROLE_MASTER:
                the_number_of_master = the_number_of_master + 1
            elif role == c.ROLE_SLAVE:
                the_number_of_slave = the_number_of_slave + 1
        self.assertTrue( 1 == the_number_of_master and 2 == the_number_of_slave,
                           'failed to set roles, the number of master:%d, the number of slave:%d' %
                           (the_number_of_master, the_number_of_slave) )

        # get master, slave1, and slave2
        master, slave1, slave2 = self.get_mss()

        # connect to a master`s redis and set data
        redis = redis_mgmt.Redis( master['id'] )
        ret = redis.connect( master['ip'], master['redis_port'] )
        self.assertEquals( ret, 0, 'failed to connect to redis, server:%d' % master['id'] )

        for i in range( max, max*2 ):
            cmd = 'set %s%d %d\r\n' % (key, i, i)
            redis.write( cmd )
            res = redis.read_until( '\r\n' )
            self.assertEquals( res, '+OK\r\n',
                               'failed to get response, server:%d' % master['id'] )
        redis.disconnect()

        # check slaves`s data
        slaves = [slave1, slave2]
        for slave in slaves:
            slave_redis = redis_mgmt.Redis( slave['id'] )
            ret = slave_redis .connect( slave['ip'], slave['redis_port'] )
            self.assertEquals( ret, 0, 'failed to connect to redis, server:%d' % slave['id'] )

            for i in range( 0, max*2 ):
                cmd = 'get %s%d\r\n' % (key, i)
                slave_redis.write( cmd )
                trash = slave_redis.read_until( '\r\n' )
                res = slave_redis.read_until( '\r\n' )
                self.assertEquals( res, '%d\r\n' % i,
                                   'inconsistent, server:%d, expected %d but %s' % (slave['id'], i, res)  )
            slave_redis.disconnect()

    def test_3_heartbeat_target_connection_count( self ):
        util.print_frame()

        util.log( 'wait until all connections are established' )
        for i in range(1, 8):
            time.sleep(1)
            util.log( '%d sec' % i )

        # check pgs
        for server in self.cluster['servers']:
            before_cnt_redis = util.get_clients_count_of_redis(server['ip'], server['redis_port'])
            before_cnt_smr = util.get_clients_count_of_smr(server['smr_mgmt_port'])

            cmd = 'pgs_leave %s %d forced' % (self.cluster['cluster_name'], server['id'])
            ret = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(ret)
            self.assertEqual( jobj['state'], 'success', 'failed : cmd="%s", reply="%s"' % (cmd, ret[:-2]) )
            util.log( 'succeeded : cmd="%s", reply="%s"' % (cmd, ret[:-2]) )

            # check redis
            success = False
            for i in range(5):
                after_cnt = util.get_clients_count_of_redis(server['ip'], server['redis_port'])
                if after_cnt <= 2:
                    success = True
                    break
                time.sleep(1)
            self.assertEquals( success, True, 'failed : the number of connections to redis%d(%s:%d) is %d, exptected:n<=2, before=%d' % (server['id'], server['ip'], server['redis_port'], after_cnt, before_cnt_redis) )
            util.log( 'succeeded : the number of connections to redis%d(%s:%d) is %d, exptected=n<=2, before=%d' % (server['id'], server['ip'], server['redis_port'], after_cnt, before_cnt_redis) )

            # check smr
            success = False
            expected = 1
            for i in range(5):
                after_cnt = util.get_clients_count_of_smr(server['smr_mgmt_port'])
                if after_cnt == expected:
                    success = True
                    break
                time.sleep(1)
            self.assertEquals( success, True, 'failed : the number of connections to smr%d(%s:%d) is %d, exptected=%d, before=%d' % (server['id'], server['ip'], server['smr_mgmt_port'], after_cnt, expected, before_cnt_smr) )
            util.log( 'succeeded : the number of connections to smr%d(%s:%d) is %d, exptected=%d, before=%d' % (server['id'], server['ip'], server['smr_mgmt_port'], after_cnt, expected, before_cnt_smr) )

        # check gateway
        for server in self.cluster['servers']:
            before_cnt = util.get_clients_count_of_gw(server['ip'], server['gateway_port'])

            cmd = 'gw_del %s %d' % (self.cluster['cluster_name'], server['id'])
            ret = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(ret)
            self.assertEqual( jobj['state'], 'success', 'failed : cmd="%s", reply="%s"' % (cmd, ret[:-2]) )
            util.log( 'succeeded : cmd="%s", reply="%s"' % (cmd, ret[:-2]) )

            success = False
            expected = 1
            for i in range(5):
                after_cnt = util.get_clients_count_of_gw(server['ip'], server['gateway_port'])
                if after_cnt == expected:
                    success = True
                    break
                time.sleep(1)

            self.assertEquals( success, True, 'failed : the number of connections to gateway%d(%s:%d) is %d, exptected=%d.' % (server['id'], server['ip'], server['gateway_port'], after_cnt, expected) )
            util.log( 'succeeded : the number of connections to gateway%d(%s:%d) is %d, exptected=%d.' % (server['id'], server['ip'], server['gateway_port'], after_cnt, expected) )

    def test_4_elect_master_randomly( self ):
        util.print_frame()
        for i in range(1):
            self.elect_master_randomly()

    def elect_master_randomly( self ):
        # set data
        ip, port = util.get_rand_gateway(self.cluster)
        gw = gateway_mgmt.Gateway( '0' )
        gw.connect( ip, port )
        for i in range( 0, 1000 ):
            cmd = 'set %s%d %d\r\n' % (self.key_base, i, i)
            gw.write( cmd )
            res = gw.read_until( '\r\n' )
            self.assertEqual( res, '+OK\r\n', 'failed to set values to gw(%s:%d). cmd:%s, res:%s' % (ip, port, cmd[:-2], res[:-2]) )

        server_ids = []
        for server in self.cluster['servers']:
            server_ids.append( server['id'] )

        for try_cnt in range( 30 ):
            # get master, slave1, slave2
            m, s1, s2 = util.get_mss( self.cluster )
            self.assertNotEqual( m, None, 'master is None.' )
            self.assertNotEqual( s1, None, 'slave1 is None.' )
            self.assertNotEqual( s2, None, 'slave2 is None.' )
            util.log( 'master id : %d' % m['id'] )

            if try_cnt != 0:
                if m['id'] in server_ids:
                    server_ids.remove( m['id'] )

            smr = smr_mgmt.SMR( m['id'] )
            ret = smr.connect( m['ip'], m['smr_mgmt_port'] )
            self.assertEqual( ret, 0, 'failed to connect to master. %s:%d' % (m['ip'], m['smr_mgmt_port']) )
            cmd = 'role lconn\r\n'
            smr.write( cmd )
            reply = smr.read_until( '\r\n' )
            self.assertEqual( reply, '+OK\r\n', 'failed : cmd="%s", reply="%s"' % (cmd[:-2], reply[:-2]) )
            util.log( 'succeeded : cmd="%s", reply="%s"' % (cmd[:-2], reply[:-2]) )

            # wait until role-change is finished
            for role_change_try_cnt in range( 5 ):
                count_master = 0
                count_slave = 0
                for server in self.cluster['servers']:
                    real_role = util.get_role_of_server( server )
                    real_role = util.roleNumberToChar( real_role )
                    if real_role == 'M':
                        count_master = count_master + 1
                    elif real_role == 'S':
                        count_slave = count_slave + 1
                if count_master == 1 and count_slave == 2:
                    break;
                time.sleep( 1 )

            # check the number of master and slave
            self.assertEqual( count_master, 1, 'failed : the number of master is not 1, count_master=%d, count_slave=%d' % (count_master, count_slave) )
            self.assertEqual( count_slave, 2, 'failed : the number of slave is not 2, count_master=%d, count_slave=%d' % (count_master, count_slave) )
            util.log( 'succeeded : the number of master is 1 and the number of slave is 2' )

            # check states of all pgs in pg
            for try_cnt in range( 3 ):
                ok = True
                for s in self.cluster['servers']:
                    real_role = util.get_role_of_server( s )
                    real_role = util.roleNumberToChar( real_role )
                    smr_info = util.get_smr_info( s, self.leader_cm )
                    cc_role = smr_info['smr_Role']
                    cc_hb = smr_info['hb']

                    if cc_hb != 'Y':
                        ok = False
                    if real_role != cc_role:
                        ok = False

                    if ok:
                        util.log( 'succeeded : a role of real pgs is the same with a role in cc, id=%d, real=%s, cc=%s, hb=%s' % (s['id'], real_role, cc_role, cc_hb) )
                    else:
                        util.log( '\n\n**********************************************************\n\nretry: a role of real pgs is not the same with a role in cc, id=%d, real=%s, cc=%s, hb=%s' % (s['id'], real_role, cc_role, cc_hb) )

                if ok == False:
                    time.sleep( 0.5 )
                else:
                    break

            self.assertTrue( ok, 'failed : role check' )

            if len( server_ids ) == 0:
                util.log( 'succeeded : all smrs have been as a master' )
                return 0

        self.assertEqual( 0, len( server_ids ) , 'failed : remains server ids=[%s]' % (','.join('%d' % id for id in server_ids))  )
        return 0

    def test_5_from_n_to_1_heartbeat_checkers( self ):
        util.print_frame()
        for i in range( 0, len( self.cluster['servers'] ) - 1 ):
            util.log( 'loop %d' % i )
            server = self.cluster['servers'][i]
            self.assertEquals( 0, testbase.request_to_shutdown_cm( server ),
                               'failed to request_to_shutdown_cm, server:%d' % server['id'] )
            time.sleep( 20 )
            self.leader_cm = self.cluster['servers'][i+1]

            self.state_transition()

    def test_6_from_3_to_6_heartbeat_checkers( self ):
        util.print_frame()

        i = 5000 + len( self.cluster['servers'] )
        for server in self.cluster['servers']:
            i = i + 1
            hbc_svr = {}
            hbc_svr['id'] = i
            hbc_svr['ip'] = server['ip']
            hbc_svr['zk_port'] = server['zk_port']

            ret = testbase.setup_cm( i )
            self.assertEquals( 0, ret, 'failed to copy heartbeat checker, server:%d' % hbc_svr['id'] )

            ret = testbase.request_to_start_cm( i, i )
            self.assertEquals( 0, ret,
                               'failed to request_to_start_cm, server:%d' % hbc_svr['id'] )
            self.state_transition()

    def test_7_remaining_hbc_connection( self ):
        util.print_frame()

        # check pgs
        for server in self.cluster['servers']:
            before_cnt_redis = util.get_clients_count_of_redis(server['ip'], server['redis_port'])
            before_cnt_smr = util.get_clients_count_of_smr(server['smr_mgmt_port'])

            cmd = 'pgs_leave %s %d forced\r\npgs_del %s %d' % (self.cluster['cluster_name'], server['id'], self.cluster['cluster_name'], server['id'])
            util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)

        for server in self.cluster['servers']:
            # check redis
            success = False
            for i in range(5):
                after_cnt = util.get_clients_count_of_redis(server['ip'], server['redis_port'])
                if after_cnt <= 2:
                    success = True
                    break
                time.sleep(1)
            self.assertEquals( success, True, 'failed : the number of connections to redis%d(%s:%d) is %d, exptected=n<=2, before=%d' % (server['id'], server['ip'], server['redis_port'], after_cnt, before_cnt_redis) )
            util.log( 'succeeded : the number of connections to redis%d(%s:%d) is %d, exptected=n<=2, before=%d' % (server['id'], server['ip'], server['redis_port'], after_cnt, before_cnt_redis) )

            # check smr
            success = False
            expected = 0
            for i in range(5):
                after_cnt = util.get_clients_count_of_smr(server['smr_mgmt_port'])
                if after_cnt == expected:
                    success = True
                    break
                time.sleep(1)
            self.assertEquals( success, True, 'failed : the number of connections to smr%d(%s:%d) is %d, exptected=%d, before=%d' % (server['id'], server['ip'], server['smr_mgmt_port'], after_cnt, expected, before_cnt_smr) )
            util.log( 'succeeded : the number of connections to smr%d(%s:%d) is %d, exptected=%d, before=%d' % (server['id'], server['ip'], server['smr_mgmt_port'], after_cnt, expected, before_cnt_smr) )

        # check gateway
        for server in self.cluster['servers']:
            before_cnt = util.get_clients_count_of_gw(server['ip'], server['gateway_port'])

            cmd = 'gw_del %s %d' % (self.cluster['cluster_name'], server['id'])
            util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)

        for server in self.cluster['servers']:
            success = False
            expected = 1
            for i in range(5):
                after_cnt = util.get_clients_count_of_gw(server['ip'], server['gateway_port'])
                if after_cnt == expected:
                    success = True
                    break
                time.sleep(1)

            self.assertEquals( success, True, 'failed : the number of connections to gateway%d(%s:%d) is %d, exptected=%d.' % (server['id'], server['ip'], server['gateway_port'], after_cnt, expected) )
            util.log( 'succeeded : the number of connections to gateway%d(%s:%d) is %d, exptected=%d.' % (server['id'], server['ip'], server['gateway_port'], after_cnt, expected) )
