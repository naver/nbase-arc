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
import copy
import gateway_mgmt
import redis_mgmt
import config
import random
import default_cluster
import smr_mgmt
import constant as c
import load_generator
import json

class TestQuorumPolicy( unittest.TestCase ):
    cluster = config.clusters[0]
    leader_cm = config.clusters[0]['servers'][0]

    @classmethod
    def setUpClass( cls ):
        return 0

    @classmethod
    def tearDownClass( cls ):
        return 0

    def setUp( self ):
        util.set_process_logfile_prefix( 'TestQuorumPolicy_%s' % self._testMethodName )
        ret = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
        if ret is not 0:
            default_cluster.finalize( self.cluster )
        self.assertEquals( ret, 0, 'failed to TestQuorumPolicy.initialize' )

    def tearDown( self ):
        ret = default_cluster.finalize( self.cluster )
        self.assertEquals( ret, 0, 'failed to TestQuorumPolicy.finalize' )
        return 0

    def test_quorum( self ):
        util.print_frame()

        master, slave1, slave2 = util.get_mss(self.cluster)

        expected = 2
        max_try = 20
        for i in range( 0, max_try ):
            quorum = util.get_quorum( master )
            if quorum == expected:
                break;
            time.sleep( 1 )
        self.assertEquals( quorum, expected,
                           'quorum:%d, expected:%d' % (quorum, expected) )

        ret = testbase.request_to_shutdown_smr( slave1 )
        self.assertEqual( ret, 0, 'failed to shutdown smr, server:%d' % slave1['id'] )
        time.sleep( 1 )

        expected = 1
        max_try = 20
        for i in range( 0, max_try ):
            master = util.get_server_by_role( self.cluster['servers'], 'master' )
            quorum = util.get_quorum( master )
            if quorum == expected:
                break;
            time.sleep( 1 )
        self.assertEquals( quorum, expected,
                           'quorum:%d, expected:%d' % (quorum, expected) )

        ret = testbase.request_to_shutdown_smr( slave2 )
        self.assertEqual( ret, 0, 'failed to shutdown smr, server:%d' % slave2['id'] )
        time.sleep( 1 )

        expected = 0
        max_try = 20
        for i in range( 0, max_try ):
            master = util.get_server_by_role( self.cluster['servers'], 'master' )
            quorum = util.get_quorum( master )
            if quorum == expected:
                break;
            time.sleep( 1 )
        self.assertEquals( quorum, expected,
                           'quorum:%d, expected:%d' % (quorum, expected) )

        # recovery
        ret = testbase.request_to_start_smr( slave1 )
        self.assertEqual( ret, 0, 'failed to start smr' )

        ret = testbase.request_to_start_redis( slave1 )
        self.assertEqual( ret, 0, 'failed to start redis' )

        ret = testbase.wait_until_finished_to_set_up_role( slave1 )
        self.assertEquals( ret, 0, 'failed to role change. smr_id:%d' % (slave1['id']) )
        time.sleep( 1 )

        expected = 1
        max_try = 20
        for i in range( 0, max_try ):
            quorum = util.get_quorum( master )
            if quorum == expected:
                break;
            time.sleep( 1 )
        self.assertEquals( quorum, expected,
                           'quorum:%d, expected:%d' % (quorum, expected) )

        # recovery
        ret = testbase.request_to_start_smr( slave2 )
        self.assertEqual( ret, 0, 'failed to start smr' )

        ret = testbase.request_to_start_redis( slave2 )
        self.assertEqual( ret, 0, 'failed to start redis' )

        ret = testbase.wait_until_finished_to_set_up_role( slave2 )
        self.assertEquals( ret, 0, 'failed to role change. smr_id:%d' % (slave2['id']) )
        time.sleep( 1 )

        expected = 2
        max_try = 20
        for i in range( 0, max_try ):
            quorum = util.get_quorum( master )
            if quorum == expected:
                break;
            time.sleep( 1 )
        self.assertEquals( quorum, expected,
                           'quorum:%d, expected:%d' % (quorum, expected) )

    def test_quorum_policy_of_hanging_master( self ):
        util.print_frame()

        # get master, slave1, slave2
        m, s1, s2 = util.get_mss( self.cluster )
        self.assertNotEqual( m, None, 'master is None.' )
        self.assertNotEqual( s1, None, 'slave1 is None.' )
        self.assertNotEqual( s2, None, 'slave2 is None.' )

        # hang
        smr = smr_mgmt.SMR( m['id'] )
        ret = smr.connect( m['ip'], m['smr_mgmt_port'] )
        self.assertEqual( ret, 0, 'failed to connect to master. %s:%d' % (m['ip'], m['smr_mgmt_port']) )
        smr.write( 'fi delay sleep 1 15000\r\n' )
        time.sleep( 5 )

        # wait for forced master election
        success = False
        new_master = None
        for i in range( 7 ):
            role = util.get_role_of_server( s1 )
            if role == c.ROLE_MASTER:
                success = True
                new_master = s1
                break
            role = util.get_role_of_server( s2 )
            if role == c.ROLE_MASTER:
                success = True
                new_master = s2
                break
            time.sleep( 1 )
        self.assertEqual( success, True, 'failed to forced master election' )

        # shutdown confmaster
        for server in self.cluster['servers']:
            util.shutdown_cm( server['id'] )

        # wait until hanging master wake up
        time.sleep( 5 )

        # check quorum policy
        quorum_of_haning_master = util.get_quorum( m )
        self.assertEqual( 2, quorum_of_haning_master,
                          'invalid quorum of haning master, expected:%d, but:%d' %(2, quorum_of_haning_master) )
        util.log( 'succeeded : quorum of haning master=%d' % quorum_of_haning_master )

        # check quorum policy
        quorum_of_new_master = util.get_quorum( new_master )
        self.assertNotEqual( None, quorum_of_new_master, 'failed : find new master' )
        self.assertEqual( 1, quorum_of_new_master ,
                          'invalid quorum of new master, expected:%d, but:%d' % (1, quorum_of_new_master) )
        util.log( 'succeeded : quorum of new master=%d' % quorum_of_new_master )

        return 0

    def test_quorum_with_left_pgs( self ):
        util.print_frame()

        # start load generators
        load_gen_list = {}
        for i in range( len(self.cluster['servers']) ):
            server = self.cluster['servers'][i]
            load_gen = load_generator.LoadGenerator(server['id'], server['ip'], server['gateway_port'])
            load_gen.start()
            load_gen_list[i] = load_gen

        # get master, slave1, slave2
        m, s1, s2 = util.get_mss( self.cluster )
        self.assertNotEqual( m, None, 'master is None.' )
        self.assertNotEqual( s1, None, 'slave1 is None.' )
        self.assertNotEqual( s2, None, 'slave2 is None.' )

        # detach pgs from cluster
        cmd = 'pgs_leave %s %d forced\r\n' % (m['cluster_name'], m['id'])
        ret = util.cm_command( self.leader_cm['ip'], self.leader_cm['cm_port'], cmd )
        jobj = json.loads(ret)
        self.assertEqual( jobj['msg'], '+OK', 'failed : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )
        util.log( 'succeeded : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )

        # check quorum policy
        quorum_of_haning_master = util.get_quorum( m )
        self.assertEqual(2, quorum_of_haning_master,
                          'invalid quorum of left master, expected:%d, but:%d' % (2, quorum_of_haning_master) )
        util.log( 'succeeded : quorum of left master=%d' % quorum_of_haning_master )

        # check if pgs is removed
        r = util.get_role_of_server(m)
        if r != c.ROLE_MASTER:
            success = False
            for try_cnt in range( 10 ):
                redis = redis_mgmt.Redis( m['id'] )
                ret = redis.connect( m['ip'], m['redis_port'] )
                self.assertEquals( ret, 0, 'failed : connect to smr%d(%s:%d)' % (m['id'], m['ip'], m['redis_port']) )
                util.log( 'succeeded : connect to smr%d(%s:%d)' % (m['id'], m['ip'], m['redis_port']) )

                redis.write( 'info stats\r\n' )
                for i in range( 6 ):
                    redis.read_until( '\r\n' )
                res = redis.read_until( '\r\n' )
                self.assertNotEqual( res, '', 'failed : get reply of "info stats" from redis%d(%s:%d)' % (m['id'], m['ip'], m['redis_port']) )
                util.log( 'succeeded : get reply of "info stats" from redis%d(%s:%d), reply="%s"' % (m['id'], m['ip'], m['redis_port'], res[:-2]) )
                no = int( res.split(':')[1] )
                if no <= 100:
                    success = True
                    break

                time.sleep( 1 )

            self.assertEquals( success, True, 'failed : pgs does not removed.' )
        util.log( 'pgs is removed' )

        # check states of all pgs in pg
        for i in xrange(10):
            for s in self.cluster['servers']:
                smr_info = util.get_smr_info( s, self.leader_cm )
                cc_role = smr_info['smr_Role']
                cc_hb = smr_info['hb']
                if cc_hb == 'N':
                    continue

                real_role = util.get_role_of_server( s )
                real_role = util.roleNumberToChar( real_role )
                if real_role != cc_role:
                    time.sleep(0.5)
                    continue

        for s in self.cluster['servers']:
            smr_info = util.get_smr_info( s, self.leader_cm )
            cc_role = smr_info['smr_Role']
            cc_hb = smr_info['hb']
            if cc_hb == 'N':
                continue

            real_role = util.get_role_of_server( s )
            real_role = util.roleNumberToChar( real_role )
            self.assertEqual( real_role, cc_role,
                              'failed : each role is difference, real=%s, cc=%s' % (real_role, cc_role) )
            util.log( 'succeeded : a role of real pgs is the same with a role in cc, real=%s, cc=%s' % (real_role, cc_role) )

        # check quorum policy
        quorum_of_haning_master = util.get_quorum( m )
        self.assertEqual(2, quorum_of_haning_master,
                          'invalid quorum of left master, expected:%d, but:%d' % (2, quorum_of_haning_master) )
        util.log( 'succeeded : quorum of left master=%d' % quorum_of_haning_master )

        # 'role lconn' to master
        cmd = 'role lconn\r\n'
        ret = util.cmd_to_smr( m, cmd )
        self.assertEqual( ret, '+OK\r\n', 'failed : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )
        util.log( 'succeeded : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )

        # wait for master election
        success = False
        new_master = None
        for i in range( 10 ):
            role = util.get_role_of_server( s1 )
            if role == c.ROLE_MASTER:
                success = True
                new_master = s1
                break
            role = util.get_role_of_server( s2 )
            if role == c.ROLE_MASTER:
                success = True
                new_master = s2
                break
            time.sleep( 1 )
        self.assertEqual( success, True, 'failed to elect new master' )
        util.log( 'succeeded : elect new master, master_id=%d' % new_master['id'] )

        time.sleep( 1 )
        # check the numbers of master, slave, and lconn
        cnt_master = 0
        cnt_slave = 0
        cnt_lconn = 0
        for s in self.cluster['servers']:
            role = util.get_role_of_server( s )
            if role == c.ROLE_MASTER:
                cnt_master = cnt_master + 1
            elif role == c.ROLE_SLAVE:
                cnt_slave = cnt_slave + 1
            elif role == c.ROLE_LCONN:
                cnt_lconn = cnt_lconn + 1
        self.assertEqual( cnt_master, 1, 'failed : the number of master is %s, expected 1' % cnt_master )
        self.assertEqual( cnt_slave, 1, 'failed : the number of slave is %s, expected 1' % cnt_slave )
        self.assertEqual( cnt_lconn, 1, 'failed : the number of lconn is %s, expected 1' % cnt_lconn )

        # check states of all pgs in pg
        for s in self.cluster['servers']:
            real_role = util.get_role_of_server( s )
            real_role = util.roleNumberToChar( real_role )
            smr_info = util.get_smr_info( s, self.leader_cm )
            cc_role = smr_info['smr_Role']
            cc_hb = smr_info['hb']
            if cc_hb == 'N':
                continue
            self.assertEqual( real_role, cc_role,
                              'failed : each role is difference, real=%s, cc=%s' % (real_role, cc_role) )
            util.log( 'succeeded : a role of real pgs is the same with a role in cc, real=%s, cc=%s' % (real_role, cc_role) )

        # check quorum policy
        quorum_of_new_master = util.get_quorum( new_master )
        self.assertNotEqual( None, quorum_of_new_master, 'failed : find new master' )
        self.assertEqual( 1, quorum_of_new_master ,
                          'invalid quorum of new master, expected:%d, but:%d' % (1, quorum_of_new_master) )
        util.log( 'succeeded : quorum of new master=%d' % quorum_of_new_master )

        # shutdown load generators
        for i in range( len(load_gen_list) ):
            load_gen_list[i].quit()
            load_gen_list[i].join()

        # Go back to initial configuration
        self.assertTrue(util.pgs_join(self.leader_cm['ip'], self.leader_cm['cm_port'], m['cluster_name'], m['id']),
                'failed to recover pgs, (pgs_join)')

        return 0
