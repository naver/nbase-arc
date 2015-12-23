#!/usr/bin/env python

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
import subprocess
import config
import default_cluster
import util
import testbase
import redis_mgmt
import time
import telnet


def block_network(cluster, mgmt_ip, mgmt_port):
    # Block
    out = util.sudo('iptables -A OUTPUT -d 127.0.0.100 -j DROP')
    if out.succeeded == False:
        util.log('add a bloking role to iptables fail. output:%s' % out)
        return False

    for i in range(4):
        util.log('waiting... %d' % (i + 1))
        time.sleep(1)

    # Check cluster state
    for i in range(2):
        util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port)
        time.sleep(1)

    return True

def unblock_network(cluster, mgmt_ip, mgmt_port, final_state):
    # Unblock
    out = util.sudo('iptables -D OUTPUT -d 127.0.0.100 -j DROP')
    if out.succeeded == False:
        util.log('delete a bloking role to iptables fail. output:%s' % out)
        return False

    # Check cluster state
    for i in range(3):
        util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, final_state)
        time.sleep(1)

    return True

class TestNetworkIsolation(unittest.TestCase):
    def setUp(self):
        util.set_process_logfile_prefix( 'TestNetworkIsolation_%s' % self._testMethodName )
        self.cleanup_iptables()
        return 0

    def tearDown(self):
        self.cleanup_iptables()
        return 0

    def cleanup_iptables(self):
        while True:
            out = util.sudo('iptables -t nat -D OUTPUT -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
            util.log(out)
            if out.succeeded == False:
                break

        while True:
            out = util.sudo('iptables -t nat -D PREROUTING -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
            util.log(out)
            if out.succeeded == False:
                break

        while True:
            out = util.sudo('iptables -D OUTPUT -d 127.0.0.100 -j DROP')
            util.log(out)
            if out.succeeded == False:
                break

        while True:
            out = util.sudo('iptables -D OUTPUT -d 127.0.0.101 -j DROP')
            util.log(out)
            if out.succeeded == False:
                break

        while True:
            out = util.sudo('iptables -D OUTPUT -d 127.0.0.102 -j DROP')
            util.log(out)
            if out.succeeded == False:
                break

    def test_1_mgmt_is_isolated(self):
        util.print_frame()

        out = util.sudo('iptables -L')
        util.log('====================================================================')
        util.log('out : %s' % out)
        util.log('out.return_code : %d' % out.return_code)
        util.log('out.stderr : %s' % out.stderr)
        util.log('out.succeeded : %s' % out.succeeded)

        cluster = filter(lambda x: x['cluster_name'] == 'network_isolation_cluster_1', config.clusters)[0]
        util.log(util.json_to_str(cluster))

        # MGMT
        mgmt_ip = cluster['servers'][0]['real_ip']
        mgmt_port = cluster['servers'][0]['cm_port']

        # Create cluster
        ret = default_cluster.initialize_starting_up_smr_before_redis( cluster )
        self.assertEqual(0, ret, 'failed to TestMaintenance.initialize')

        # Print initial state of cluster
        util.log('\n\n\n ### INITIAL STATE OF CLUSTER ### ')
        initial_state = []
        self.assertTrue(util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, initial_state, check_quorum=True), 'failed to check cluster state')

        # Set SMR option (slave_idle_timeout)
        util.log('\n\n\n ### Set SMR option ###')
        for s in cluster['servers']:
            t = telnet.Telnet('SMR%d' % s['id'])
            self.assertEqual(t.connect(s['ip'], s['smr_mgmt_port']), 0,
                    'Failed to connect to smr. ADDR=%s:%d' % (s['ip'], s['smr_mgmt_port']))
            cmd = 'confset slave_idle_timeout_msec 18000'
            util.log('[%s:%d] >> %s' % (s['ip'], s['smr_mgmt_port'], cmd))
            t.write('confset slave_idle_timeout_msec 18000\r\n')
            reply = t.read_until('\r\n').strip()
            util.log('[%s:%d] << %s' % (s['ip'], s['smr_mgmt_port'], reply))
            self.assertEqual(reply, '+OK', 'Failed to set slave_idle_timeout, REPLY=%s' % reply)

        # Network isolation test
        for cnt in range(5):
            # Block network
            util.log('\n\n\n ### BLOCK NETWORK, %d ### ' % cnt)
            for s in cluster['servers']:
                out = util.sudo('iptables -A OUTPUT -d 127.0.0.100 -p tcp --dport %d -j DROP' % (s['smr_mgmt_port']))
                """Loopback Address Range (Reference : RFC3330)

                127.0.0.0/8 - This block is assigned for use as the Internet host
                  loopback address.  A datagram sent by a higher level protocol to an
                  address anywhere within this block should loop back inside the host.
                  This is ordinarily implemented using only 127.0.0.1/32 for loopback,
                  but no addresses within this block should ever appear on any network
                  anywhere [RFC1700, page 5].
                """
                self.assertTrue(out.succeeded, 'add a bloking role to iptables fail. output:%s' % out)

            for i in range(4):
                util.log('waiting... %d' % (i + 1))
                time.sleep(1)

            # Check cluster state
            ok = False
            for i in range(7):
                isolated_states = []
                util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, isolated_states, check_quorum=True)
                time.sleep(1)

                state_transition_done = True
                for s in isolated_states:
                    if s['ip'] != '127.0.0.100':
                        continue

                    if s['active_role'] != '?' or s['mgmt_role'] != 'N':
                        state_transition_done = False

                if state_transition_done :
                    ok = True
                    break
                time.sleep(1)
            self.assertTrue(ok, 'Fail, state transition')

            # Unblock network
            util.log('\n\n\n ### UNBLOCK NETWORK, %d ### ' % cnt)
            for s in cluster['servers']:
                out = util.sudo('iptables -D OUTPUT -d 127.0.0.100 -p tcp --dport %d -j DROP' % (s['smr_mgmt_port']))
                self.assertTrue(out.succeeded, 'delete a bloking role to iptables fail. output:%s' % out)

            # Check cluster state
            ok = False
            for i in range(7):
                final_state = []
                util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, final_state, check_quorum=True)

                state_consistency = True
                for s in final_state:
                    if s['active_role'] != s['mgmt_role']:
                        state_consistency = False

                if state_consistency:
                    ok = True
                    break
                time.sleep(1)
            self.assertTrue(ok, 'Fail, state consistency')

        # Check state
        self.assertNotEqual(initial_state, None, 'initial_state is None')
        self.assertNotEqual(final_state, None, 'final_state is None')

        initial_state = sorted(initial_state, key=lambda x: int(x['pgs_id']))
        final_state = sorted(final_state, key=lambda x: int(x['pgs_id']))
        for i in range(len(final_state)):
            msg = 'ts (%d)%d -> (%d)%d' % (initial_state[i]['pgs_id'], initial_state[i]['active_ts'], final_state[i]['pgs_id'], final_state[i]['active_ts'])
            util.log(msg)
            self.assertEqual(initial_state[i]['active_ts'], final_state[i]['active_ts'], msg)

        # Shutdown cluster
        ret = default_cluster.finalize( cluster )
        self.assertEqual(ret, 0, 'failed to TestMaintenance.finalize')

    def test_2_some_pgs_is_isolated(self):
        util.print_frame()

        out = util.sudo('iptables -L')
        util.log('====================================================================')
        util.log('out : %s' % out)
        util.log('out.return_code : %d' % out.return_code)
        util.log('out.stderr : %s' % out.stderr)
        util.log('out.succeeded : %s' % out.succeeded)

        # Add forwarding role (127.0.0.100 -> 127.0.0.1)
        out = util.sudo('iptables -t nat -A OUTPUT -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)

        out = util.sudo('iptables -t nat -A PREROUTING -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)

        cluster = filter(lambda x: x['cluster_name'] == 'network_isolation_cluster_2', config.clusters)[0]
        util.log(util.json_to_str(cluster))

        # MGMT
        mgmt_ip = cluster['servers'][0]['real_ip']
        mgmt_port = cluster['servers'][0]['cm_port']

        # Create cluster
        ret = default_cluster.initialize_starting_up_smr_before_redis( cluster )
        self.assertEqual(0, ret, 'failed to TestMaintenance.initialize')

        # Place master on virtual ip address in order to cause master election.
        pg_id = 0
        m = util.get_server_by_role_and_pg(cluster['servers'], 'master', pg_id)
        s = util.get_server_by_role_and_pg(cluster['servers'], 'slave', pg_id)
        if m.has_key('ip') == True and m.has_key('real_ip') == False:
            ret = util.role_change(cluster['servers'][0], cluster['cluster_name'], s['id'])
            self.assertNotEquals(ret, -1, 'change %d to a master fail' % s['id'])

        # Print initial state of cluster
        util.log('\n\n\n ### INITIAL STATE OF CLUSTER ### ')
        initial_state = []
        self.assertTrue(util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, initial_state, check_quorum=True), 'failed to check cluster state')

        # Network isolation test
        for cnt in range(3):
            # Block network
            util.log('\n\n\n ### BLOCK NETWORK, %d ### ' % cnt)
            out = util.sudo('iptables -A OUTPUT -d 127.0.0.100 -j DROP')
            self.assertTrue(out.succeeded, 'add a bloking role to iptables fail. output:%s' % out)

            for i in range(4):
                util.log('waiting... %d' % (i + 1))
                time.sleep(1)

            # Check cluster state
            ok = False
            for i in range(7):
                isolated_states = []
                util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, isolated_states, check_quorum=True)
                time.sleep(1)

                state_transition_done = True
                for s in isolated_states:
                    if s['pgs_id'] == 1:
                        continue

                    if s['active_role'] != '?' or s['mgmt_role'] != 'N':
                        state_transition_done = False

                if state_transition_done :
                    ok = True
                    break
                time.sleep(1)
            self.assertTrue(ok, 'Fail, state transition')

            # Unblock network
            util.log('\n\n\n ### UNBLOCK NETWORK, %d ### ' % cnt)
            out = util.sudo('iptables -D OUTPUT -d 127.0.0.100 -j DROP')
            self.assertTrue(out.succeeded, 'delete a bloking role to iptables fail. output:%s' % out)

            # Check cluster state
            ok = False
            for i in range(7):
                final_state = []
                util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, final_state, check_quorum=True)

                state_consistency = True
                for s in final_state:
                    if s['pgs_id'] == 1:
                        continue

                    if s['active_role'] != s['mgmt_role']:
                        state_consistency = False

                if state_consistency:
                    ok = True
                    break
                time.sleep(1)
            self.assertTrue(ok, 'Fail, state consistency')

        # Check state
        self.assertNotEqual(initial_state, None, 'initial_state is None')
        self.assertNotEqual(final_state, None, 'final_state is None')

        initial_state = sorted(initial_state, key=lambda x: int(x['pgs_id']))
        final_state = sorted(final_state, key=lambda x: int(x['pgs_id']))
        for i in range(len(final_state)):
            msg = 'ts (%d)%d -> (%d)%d' % (initial_state[i]['pgs_id'], initial_state[i]['active_ts'], final_state[i]['pgs_id'], final_state[i]['active_ts'])
            util.log(msg)
            self.assertNotEqual(initial_state[i]['active_ts'], final_state[i]['active_ts'], msg)

        # Shutdown cluster
        ret = default_cluster.finalize( cluster )
        self.assertEqual(ret, 0, 'failed to TestMaintenance.finalize')

        # Delete forwarding role (127.0.0.100 -> 127.0.0.1)
        out = util.sudo('iptables -t nat -D OUTPUT -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'delete a forwarding role to iptables fail. output:%s' % out)

        out = util.sudo('iptables -t nat -D PREROUTING -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'delete a forwarding role to iptables fail. output:%s' % out)

    def test_3_some_pgs_is_isolated_2copy(self):
        util.print_frame()

        out = util.sudo('iptables -L')
        util.log('====================================================================')
        util.log('out : %s' % out)
        util.log('out.return_code : %d' % out.return_code)
        util.log('out.stderr : %s' % out.stderr)
        util.log('out.succeeded : %s' % out.succeeded)

        # Add forwarding role (127.0.0.100 -> 127.0.0.1)
        out = util.sudo('iptables -t nat -A OUTPUT -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)

        out = util.sudo('iptables -t nat -A PREROUTING -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)

        cluster = filter(lambda x: x['cluster_name'] == 'network_isolation_cluster_1_2copy', config.clusters)[0]
        util.log(util.json_to_str(cluster))

        # MGMT
        mgmt_ip = cluster['servers'][0]['ip']
        mgmt_port = cluster['servers'][0]['cm_port']

        # Create cluster
        ret = default_cluster.initialize_starting_up_smr_before_redis( cluster )
        self.assertEqual(0, ret, 'failed to TestMaintenance.initialize')

        # Place master on real ip address
        for pg_id in [0, 1]:
            m = util.get_server_by_role_and_pg(cluster['servers'], 'master', pg_id)
            s = util.get_server_by_role_and_pg(cluster['servers'], 'slave', pg_id)
            if m.has_key('ip') and m.has_key('real_ip'):
                if m['ip'] != m['real_ip']:
                    ret = util.role_change(cluster['servers'][0], cluster['cluster_name'], s['id'])
                    self.assertNotEquals(ret, -1, 'change %d to a master fail' % s['id'])

        # Print initial state of cluster
        util.log('\n\n\n ### INITIAL STATE OF CLUSTER ### ')
        initial_state = []
        self.assertTrue(util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, initial_state, check_quorum=True), 'failed to check cluster state')

        # Network isolation test
        for cnt in range(3):
            # Block network
            util.log('\n\n\n ### BLOCK NETWORK, %d ### ' % cnt)
            out = util.sudo('iptables -A OUTPUT -d 127.0.0.100 -j DROP')
            self.assertTrue(out.succeeded, 'add a bloking role to iptables fail. output:%s' % out)

            for i in range(4):
                util.log('waiting... %d' % (i + 1))
                time.sleep(1)

            # Check cluster state
            ok = False
            for i in range(7):
                isolated_states = []
                util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, isolated_states, check_quorum=True)
                time.sleep(1)

                state_transition_done = True
                for s in isolated_states:
                    if s['pgs_id'] == 0 or s['pgs_id'] == 1:
                        continue

                    if s['active_role'] != 'M' or s['mgmt_role'] != 'M':
                        state_transition_done = False

                    if s['quorum'] != 0:
                        state_transition_done = False

                if state_transition_done:
                    ok = True
                    break
                time.sleep(1)
            self.assertTrue(ok, 'Fail, state transition')

            # Unblock network
            util.log('\n\n\n ### UNBLOCK NETWORK, %d ### ' % cnt)
            out = util.sudo('iptables -D OUTPUT -d 127.0.0.100 -j DROP')
            self.assertTrue(out.succeeded, 'delete a bloking role to iptables fail. output:%s' % out)

            # Check cluster state
            ok = False
            for i in range(7):
                final_state = []
                if util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, final_state, check_quorum=True) == False:
                    time.sleep(1)
                    continue

                state_consistency = True
                for s in final_state:
                    if s['pgs_id'] == 1:
                        continue

                    if s['active_role'] != s['mgmt_role']:
                        state_consistency = False

                if state_consistency:
                    ok = True
                    break
                time.sleep(1)
            self.assertTrue(ok, 'Fail, state consistency')

        # Check state
        self.assertNotEqual(initial_state, None, 'initial_state is None')
        self.assertNotEqual(final_state, None, 'final_state is None')

        initial_state = sorted(initial_state, key=lambda x: int(x['pgs_id']))
        final_state = sorted(final_state, key=lambda x: int(x['pgs_id']))
        for i in range(len(final_state)):
            msg = 'ts (%d)%d -> (%d)%d' % (initial_state[i]['pgs_id'], initial_state[i]['active_ts'], final_state[i]['pgs_id'], final_state[i]['active_ts'])
            util.log(msg)
            self.assertEqual(initial_state[i]['active_ts'], final_state[i]['active_ts'], msg)

        # Shutdown cluster
        ret = default_cluster.finalize( cluster )
        self.assertEqual(ret, 0, 'failed to TestMaintenance.finalize')

        # Delete forwarding role (127.0.0.100 -> 127.0.0.1)
        out = util.sudo('iptables -t nat -D OUTPUT -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'delete a forwarding role to iptables fail. output:%s' % out)

        out = util.sudo('iptables -t nat -D PREROUTING -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'delete a forwarding role to iptables fail. output:%s' % out)

    def test_4_mgmt_is_isolated_with_slave_failover(self):
        util.print_frame()

        out = util.sudo('iptables -L')
        util.log('====================================================================')
        util.log('out : %s' % out)
        util.log('out.return_code : %d' % out.return_code)
        util.log('out.stderr : %s' % out.stderr)
        util.log('out.succeeded : %s' % out.succeeded)

        cluster = filter(lambda x: x['cluster_name'] == 'network_isolation_cluster_1', config.clusters)[0]
        util.log(util.json_to_str(cluster))

        self.leader_cm = cluster['servers'][0]

        # MGMT
        mgmt_ip = cluster['servers'][0]['real_ip']
        mgmt_port = cluster['servers'][0]['cm_port']

        # Create cluster
        ret = default_cluster.initialize_starting_up_smr_before_redis( cluster )
        self.assertEqual(0, ret, 'failed to TestMaintenance.initialize')

        util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port)

        # Master must be the first pgs, cluster['servers'][0].
        to_be_master = cluster['servers'][0]
        m = util.get_server_by_role_and_pg(cluster['servers'], 'master', to_be_master['pg_id'])
        master_id = -1
        if m['id'] != to_be_master['id']:
            try_cnt = 0
            while master_id != to_be_master['id'] and try_cnt < 20:
                master_id = util.role_change(cluster['servers'][0], cluster['cluster_name'], to_be_master['id'])
                try_cnt += 1
                time.sleep(1)
            self.assertEquals(master_id, to_be_master['id'], 'change %d to a master fail' % to_be_master['id'])

        # Print initial state of cluster
        util.log('\n\n\n ### INITIAL STATE OF CLUSTER ### ')
        initial_state = []
        self.assertTrue(util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, initial_state, check_quorum=True), 'failed to check cluster state')

        # Set SMR option (slave_idle_timeout)
        util.log('\n\n\n ### Set SMR option ###')
        for s in cluster['servers']:
            t = telnet.Telnet('SMR%d' % s['id'])
            self.assertEqual(t.connect(s['ip'], s['smr_mgmt_port']), 0,
                    'Failed to connect to smr. ADDR=%s:%d' % (s['ip'], s['smr_mgmt_port']))
            cmd = 'confset slave_idle_timeout_msec 18000'
            util.log('[%s:%d] >> %s' % (s['ip'], s['smr_mgmt_port'], cmd))
            t.write('confset slave_idle_timeout_msec 18000\r\n')
            reply = t.read_until('\r\n').strip()
            util.log('[%s:%d] << %s' % (s['ip'], s['smr_mgmt_port'], reply))
            self.assertEqual(reply, '+OK', 'Failed to set slave_idle_timeout, REPLY=%s' % reply)

        # Network isolation test
        for loop_cnt in range(3):
            # Block network
            util.log('\n\n\n ### BLOCK NETWORK, %d ### ' % loop_cnt)
            for s in cluster['servers']:
                out = util.sudo('iptables -A OUTPUT -d 127.0.0.100 -p tcp --dport %d -j DROP' % (s['smr_mgmt_port']))
                self.assertTrue(out.succeeded, 'add a bloking role to iptables fail. output:%s' % out)

            for i in range(4):
                util.log('waiting... %d' % (i + 1))
                time.sleep(1)

            # Check cluster state
            ok = False
            for i in range(7):
                isolated_states = []
                util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, isolated_states, check_quorum=True)
                time.sleep(1)

                state_transition_done = True
                for s in isolated_states:
                    if s['ip'] != '127.0.0.100':
                        continue

                    if s['active_role'] != '?' or s['mgmt_role'] != 'N':
                        state_transition_done = False

                if state_transition_done :
                    ok = True
                    break
                time.sleep(1)
            self.assertTrue(ok, 'Fail, state transition')

            # Shutdown
            server = cluster['servers'][1]
            util.log( 'shutdown pgs%d while hanging.' % server['id'] )
            ret = testbase.request_to_shutdown_smr( server )
            self.assertEqual( ret, 0, 'failed to shutdown smr. id:%d' % server['id'] )
            ret = testbase.request_to_shutdown_redis( server )
            self.assertEqual( ret, 0, 'failed to shutdown redis. id:%d' % server['id'] )

            # Check state F
            max_try = 20
            expected = 'F'
            for i in range( 0, max_try):
                util.log('MGMT_IP:%s, MGMT_PORT:%d' % (mgmt_ip, mgmt_port))
                state = util._get_smr_state( server['id'], cluster['cluster_name'], mgmt_ip, mgmt_port )
                if expected == state:
                    break;
                time.sleep( 1 )
            self.assertEqual( expected , state,
                               'server%d - state:%s, expected:%s' % (server['id'], state, expected) )
            util.log( 'succeeded : pgs%d state changed to F.' % server['id'] )

            # Unblock network
            for s in cluster['servers']:
                out = util.sudo('iptables -D OUTPUT -d 127.0.0.100 -p tcp --dport %d -j DROP' % (s['smr_mgmt_port']))
                self.assertTrue(out.succeeded, 'delete a bloking role to iptables fail. output:%s' % out)

            # Check cluster state
            ok = False
            for i in range(10):
                final_state = []
                util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, final_state, check_quorum=True)

                state_consistency = True
                for s in final_state:
                    if s['pgs_id'] == 1:
                        continue

                    if s['active_role'] != s['mgmt_role']:
                        state_consistency = False

                if state_consistency:
                    ok = True
                    break
                time.sleep(1)
            self.assertTrue(ok, 'Fail, state consistency')

            # Recovery
            util.log( 'restart pgs%d.' % server['id'] )
            ret = testbase.request_to_start_smr( server )
            self.assertEqual( ret, 0, 'failed to start smr. id:%d' % server['id'] )

            ret = testbase.request_to_start_redis( server )
            self.assertEqual( ret, 0, 'failed to start redis. id:%d' % server['id'] )

            wait_count = 20
            ret = testbase.wait_until_finished_to_set_up_role( server, wait_count )
            self.assertEqual( ret, 0, 'failed to role change. smr_id:%d' % (server['id']) )

            redis = redis_mgmt.Redis( server['id'] )
            ret = redis.connect( server['ip'], server['redis_port'] )
            self.assertEqual( ret, 0, 'failed to connect to redis' )

            ok = False
            for i in xrange(5):
                ok = util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, check_quorum=True)
                if ok:
                    break
                else:
                    time.sleep(1)
            self.assertTrue(ok, 'failed to check cluster state')

            # Reset SMR option (slave_idle_timeout)
            t = telnet.Telnet('SMR%d' % server['id'])
            self.assertEqual(t.connect(server['ip'], server['smr_mgmt_port']), 0,
                    'Failed to connect to smr. ADDR=%s:%d' % (server['ip'], server['smr_mgmt_port']))
            cmd = 'confset slave_idle_timeout_msec 18000'
            util.log('[%s:%d] >> %s' % (server['ip'], server['smr_mgmt_port'], cmd))
            t.write('confset slave_idle_timeout_msec 18000\r\n')
            reply = t.read_until('\r\n').strip()
            util.log('[%s:%d] << %s' % (server['ip'], server['smr_mgmt_port'], reply))
            self.assertEqual(reply, '+OK', 'Failed to set slave_idle_timeout, REPLY=%s' % reply)

        # Check state
        self.assertNotEqual(initial_state, None, 'initial_state is None')
        self.assertNotEqual(final_state, None, 'final_state is None')

        initial_state = sorted(initial_state, key=lambda x: int(x['pgs_id']))
        final_state = sorted(final_state, key=lambda x: int(x['pgs_id']))
        for i in range(len(final_state)):
            msg = 'ts (%d)%d -> (%d)%d' % (initial_state[i]['pgs_id'], initial_state[i]['active_ts'], final_state[i]['pgs_id'], final_state[i]['active_ts'])
            util.log(msg)
            if initial_state[i]['pgs_id'] != 1:
                self.assertEqual(initial_state[i]['active_ts'], final_state[i]['active_ts'], msg)

        self.assertTrue(util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, check_quorum=True), 'failed to check cluster state')

        # Shutdown cluster
        ret = default_cluster.finalize( cluster )
        self.assertEqual(ret, 0, 'failed to TestMaintenance.finalize')

    def test_5_mgmt_is_isolated_with_master_failover(self):
        util.print_frame()

        out = util.sudo('iptables -L')
        util.log('====================================================================')
        util.log('out : %s' % out)
        util.log('out.return_code : %d' % out.return_code)
        util.log('out.stderr : %s' % out.stderr)
        util.log('out.succeeded : %s' % out.succeeded)

        # Add forwarding role (127.0.0.100 -> 127.0.0.1)
        out = util.sudo('iptables -t nat -A OUTPUT -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)

        out = util.sudo('iptables -t nat -A PREROUTING -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)

        cluster = filter(lambda x: x['cluster_name'] == 'network_isolation_cluster_1', config.clusters)[0]
        util.log(util.json_to_str(cluster))

        self.leader_cm = cluster['servers'][0]

        # MGMT
        mgmt_ip = cluster['servers'][0]['real_ip']
        mgmt_port = cluster['servers'][0]['cm_port']

        # Create cluster
        ret = default_cluster.initialize_starting_up_smr_before_redis( cluster )
        self.assertEqual(0, ret, 'failed to TestMaintenance.initialize')

        # Print initial state of cluster
        util.log('\n\n\n ### INITIAL STATE OF CLUSTER ### ')
        initial_state = []
        self.assertTrue(util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, initial_state, check_quorum=True), 'failed to check cluster state')

        # Network isolation test
        for loop_cnt in range(3):
            master, slave1, slave2 = util.get_mss(cluster)
            self.assertNotEquals(master, None, 'there is no master')
            self.assertNotEquals(slave1, None, 'there is no slave1')
            self.assertNotEquals(slave2, None, 'there is no slave2')

            # Block network
            util.log('\n\n\n ### BLOCK NETWORK, %d ### ' % loop_cnt)
            out = util.sudo('iptables -A OUTPUT -d 127.0.0.100 -j DROP')
            self.assertTrue(out.succeeded, 'add a bloking role to iptables fail. output:%s' % out)

            for i in range(4):
                util.log('waiting... %d' % (i + 1))
                time.sleep(1)

            # Check cluster state
            ok = False
            for i in range(10):
                isolated_states = []
                util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, isolated_states, check_quorum=True)
                time.sleep(1)

                state_transition_done = True
                for s in isolated_states:
                    if s['ip'] != '127.0.0.100':
                        continue

                    if s['active_role'] != '?' or s['mgmt_role'] != 'N':
                        state_transition_done = False

                if state_transition_done :
                    ok = True
                    break
                time.sleep(1)
            self.assertTrue(ok, 'Fail, state transition')

            # Shutdown master
            util.log( 'shutdown pgs%d while hanging.' % master['id'] )
            ret = testbase.request_to_shutdown_smr( master )
            self.assertEqual( ret, 0, 'failed to shutdown smr. id:%d' % master['id'] )
            ret = testbase.request_to_shutdown_redis( master )
            self.assertEqual( ret, 0, 'failed to shutdown redis. id:%d' % master['id'] )

            # Check state F
            max_try = 20
            expected = 'F'
            for i in range( 0, max_try):
                util.log('MGMT_IP:%s, MGMT_PORT:%d' % (mgmt_ip, mgmt_port))
                state = util._get_smr_state( master['id'], cluster['cluster_name'], mgmt_ip, mgmt_port )
                if expected == state:
                    break;
                time.sleep( 1 )
            self.assertEqual( expected , state,
                               'master%d - state:%s, expected:%s' % (master['id'], state, expected) )
            util.log( 'succeeded : pgs%d state changed to F.' % master['id'] )

            # Unblock network
            util.log('\n\n\n ### UNBLOCK NETWORK, %d ### ' % loop_cnt)
            out = util.sudo('iptables -D OUTPUT -d 127.0.0.100 -j DROP')
            self.assertTrue(out.succeeded, 'delete a bloking role to iptables fail. output:%s' % out)

            # Check cluster state
            ok = False
            for i in range(7):
                final_state = []
                util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, final_state, check_quorum=True)

                state_consistency = True
                for s in final_state:
                    if s['pgs_id'] == master['id']:
                        continue

                    if s['active_role'] != s['mgmt_role']:
                        state_consistency = False

                if state_consistency:
                    ok = True
                    break
                time.sleep(1)
            self.assertTrue(ok, 'Fail, state consistency')

            # Recovery
            util.log( 'restart pgs%d.' % master['id'] )
            ret = testbase.request_to_start_smr( master )
            self.assertEqual( ret, 0, 'failed to start smr. id:%d' % master['id'] )

            ret = testbase.request_to_start_redis( master )
            self.assertEqual( ret, 0, 'failed to start redis. id:%d' % master['id'] )

            wait_count = 20
            ret = testbase.wait_until_finished_to_set_up_role( master, wait_count )
            self.assertEqual( ret, 0, 'failed to role change. smr_id:%d' % (master['id']) )

            redis = redis_mgmt.Redis( master['id'] )
            ret = redis.connect( master['ip'], master['redis_port'] )
            self.assertEqual( ret, 0, 'failed to connect to redis' )

            ok = False
            for i in xrange(5):
                ok = util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, check_quorum=True)
                if ok:
                    break
                else:
                    time.sleep(1)

            self.assertTrue(ok, 'failed to check cluster state')

        # Check state
        self.assertNotEqual(initial_state, None, 'initial_state is None')
        self.assertNotEqual(final_state, None, 'final_state is None')

        initial_state = sorted(initial_state, key=lambda x: int(x['pgs_id']))
        final_state = sorted(final_state, key=lambda x: int(x['pgs_id']))
        for i in range(0, 3):
            msg = 'ts (%d)%d -> (%d)%d' % (initial_state[i]['pgs_id'], initial_state[i]['active_ts'], final_state[i]['pgs_id'], final_state[i]['active_ts'])
            util.log(msg)
            self.assertNotEqual(initial_state[i]['active_ts'], final_state[i]['active_ts'], msg)

        for i in range(3, 6):
            msg = 'ts (%d)%d -> (%d)%d' % (initial_state[i]['pgs_id'], initial_state[i]['active_ts'], final_state[i]['pgs_id'], final_state[i]['active_ts'])
            util.log(msg)
            self.assertEqual(initial_state[i]['active_ts'], final_state[i]['active_ts'], msg)

        self.assertTrue(util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, check_quorum=True), 'failed to check cluster state')

        # Shutdown cluster
        ret = default_cluster.finalize( cluster )
        self.assertEqual(ret, 0, 'failed to TestMaintenance.finalize')

        # Delete forwarding role (127.0.0.100 -> 127.0.0.1)
        out = util.sudo('iptables -t nat -D OUTPUT -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'delete a forwarding role to iptables fail. output:%s' % out)

        out = util.sudo('iptables -t nat -D PREROUTING -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'delete a forwarding role to iptables fail. output:%s' % out)

    def test_6_mgmt_is_isolated_with_lconn(self):
        util.print_frame()

        out = util.sudo('iptables -L')
        util.log('====================================================================')
        util.log('out : %s' % out)
        util.log('out.return_code : %d' % out.return_code)
        util.log('out.stderr : %s' % out.stderr)
        util.log('out.succeeded : %s' % out.succeeded)

        cluster = filter(lambda x: x['cluster_name'] == 'network_isolation_cluster_1', config.clusters)[0]
        util.log(util.json_to_str(cluster))

        self.leader_cm = cluster['servers'][0]

        # MGMT
        mgmt_ip = cluster['servers'][0]['real_ip']
        mgmt_port = cluster['servers'][0]['cm_port']

        # Create cluster
        ret = default_cluster.initialize_starting_up_smr_before_redis( cluster )
        self.assertEqual(0, ret, 'failed to TestMaintenance.initialize')

        # Print initial state of cluster
        util.log('\n\n\n ### INITIAL STATE OF CLUSTER ### ')
        initial_state = []
        self.assertTrue(util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, initial_state, check_quorum=True), 'failed to check cluster state')

        # Set SMR option (slave_idle_timeout)
        util.log('\n\n\n ### Set SMR option ###')
        for s in cluster['servers']:
            t = telnet.Telnet('SMR%d' % s['id'])
            self.assertEqual(t.connect(s['ip'], s['smr_mgmt_port']), 0,
                    'Failed to connect to smr. ADDR=%s:%d' % (s['ip'], s['smr_mgmt_port']))
            cmd = 'confset slave_idle_timeout_msec 18000'
            util.log('[%s:%d] >> %s' % (s['ip'], s['smr_mgmt_port'], cmd))
            t.write('confset slave_idle_timeout_msec 18000\r\n')
            reply = t.read_until('\r\n').strip()
            util.log('[%s:%d] << %s' % (s['ip'], s['smr_mgmt_port'], reply))
            self.assertEqual(reply, '+OK', 'Failed to set slave_idle_timeout, REPLY=%s' % reply)

        # Network isolation test
        for loop_cnt in range(3):
            # Get master
            master = util.get_server_by_role_and_pg( cluster['servers'], 'master', 0 )

            first_slave = None
            for s in cluster['servers']:
                if s == master:
                    continue

                # Skip non-virtual host
                if s.has_key('real_ip') == False:
                    continue

                if first_slave == None:
                    first_slave = s

                # 'role lconn'
                util.log( 'role lconn pgs%d while hanging.' % s['id'] )

                ret = util.role_lconn_addr( s['real_ip'], s['smr_mgmt_port']  )
                self.assertEqual( ret, '+OK\r\n', 'role lconn failed. reply="%s"' % (ret[:-2]) )
                util.log( 'succeeded : cmd="role lconn", reply="%s"' % (ret[:-2]) )
                time.sleep(0.5)

            # Block network
            util.log('\n\n\n ### BLOCK NETWORK, %d ### ' % loop_cnt)
            out = util.sudo('iptables -A OUTPUT -d 127.0.0.100 -p tcp --dport %d -j DROP' % (first_slave['smr_mgmt_port']))
            self.assertTrue(out.succeeded, 'add a bloking role to iptables fail. output:%s' % out)

            for i in range(6):
                util.log('waiting... %d' % (i + 1))
                time.sleep(1)

            # Check cluster state
            ok = False
            for i in range(10):
                isolated_states = []
                util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, isolated_states, check_quorum=True)
                time.sleep(1)

                state_transition_done = True
                for s in isolated_states:
                    if s['pgs_id'] != first_slave['id']:
                        continue

                    if s['active_role'] != '?' or s['mgmt_role'] != 'N':
                        state_transition_done = False

                if state_transition_done :
                    ok = True
                    break
                time.sleep(1)
            self.assertTrue(ok, 'Fail, state transition')

            # Unblock network
            out = util.sudo('iptables -D OUTPUT -d 127.0.0.100 -p tcp --dport %d -j DROP' % (first_slave['smr_mgmt_port']))
            self.assertTrue(out.succeeded, 'delete a bloking role to iptables fail. output:%s' % out)

            # Check cluster state
            ok = False
            for i in range(7):
                final_state = []
                util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, final_state, check_quorum=True)

                state_consistency = True
                for s in final_state:
                    if s['pgs_id'] == 1:
                        continue

                    if s['active_role'] != s['mgmt_role']:
                        state_consistency = False

                if state_consistency:
                    ok = True
                    break
                time.sleep(1)
            self.assertTrue(ok, 'Fail, state consistency')

            ok = False
            for i in xrange(5):
                ok = util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, check_quorum=True)
                if ok:
                    break
                else:
                    time.sleep(1)
            self.assertTrue(ok, 'failed to check cluster state')

        # Check state
        self.assertNotEqual(initial_state, None, 'initial_state is None')
        self.assertNotEqual(final_state, None, 'final_state is None')

        initial_state = sorted(initial_state, key=lambda x: int(x['pgs_id']))
        final_state = sorted(final_state, key=lambda x: int(x['pgs_id']))
        for i in range(len(final_state)):
            msg = 'ts (%d)%d -> (%d)%d' % (initial_state[i]['pgs_id'], initial_state[i]['active_ts'], final_state[i]['pgs_id'], final_state[i]['active_ts'])
            util.log(msg)

        self.assertTrue(util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, check_quorum=True), 'failed to check cluster state')

        # Shutdown cluster
        ret = default_cluster.finalize( cluster )
        self.assertEqual(ret, 0, 'failed to TestMaintenance.finalize')

    def test_7_repeat_isolation_and_no_opinion_linepay(self):
        util.print_frame()

        out = util.sudo('iptables -L')
        util.log('====================================================================')
        util.log('out : %s' % out)
        util.log('out.return_code : %d' % out.return_code)
        util.log('out.stderr : %s' % out.stderr)
        util.log('out.succeeded : %s' % out.succeeded)

        # Add forwarding role
        out = util.sudo('iptables -t nat -A OUTPUT -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)
        out = util.sudo('iptables -t nat -A PREROUTING -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)

        out = util.sudo('iptables -t nat -A OUTPUT -d 127.0.0.101 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)
        out = util.sudo('iptables -t nat -A PREROUTING -d 127.0.0.101 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)

        out = util.sudo('iptables -t nat -A OUTPUT -d 127.0.0.102 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)
        out = util.sudo('iptables -t nat -A PREROUTING -d 127.0.0.102 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)

        cluster_name = 'no_opinion'
        cluster = filter(lambda x: x['cluster_name'] == cluster_name, config.clusters)[0]
        util.log(util.json_to_str(cluster))

        self.leader_cm = cluster['servers'][0]

        # MGMT
        mgmt_ip = cluster['servers'][0]['real_ip']
        mgmt_port = cluster['servers'][0]['cm_port']

        # Create cluster
        ret = default_cluster.initialize_starting_up_smr_before_redis( cluster )
        self.assertEqual(0, ret, 'failed to TestMaintenance.initialize')

        # Print initial state of cluster
        util.log('\n\n\n ### INITIAL STATE OF CLUSTER ### ')
        initial_state = []
        self.assertTrue(util.check_cluster(cluster['cluster_name'], mgmt_ip, mgmt_port, initial_state, check_quorum=True), 'failed to check cluster state')

        # Network isolation test
        loop_cnt = 0
        while (loop_cnt < 20):
            loop_cnt += 1
            # Block network
            util.log('\n\n\n ### BLOCK NETWORK, %d ### ' % loop_cnt)
            out = util.sudo('iptables -A OUTPUT -d 127.0.0.102 -j DROP')
            self.assertTrue(out.succeeded, 'add a bloking role to iptables fail. output:%s' % out)

            for i in range(1):
                util.log('waiting... %d' % (i + 1))
                time.sleep(0.1)

            out = util.sudo('iptables -A OUTPUT -d 127.0.0.100 -j DROP')
            self.assertTrue(out.succeeded, 'add a bloking role to iptables fail. output:%s' % out)

            for i in range(3):
                util.log('waiting... %d' % (i + 1))
                time.sleep(1.2)

            out = util.sudo('iptables -A OUTPUT -d 127.0.0.101 -j DROP')
            self.assertTrue(out.succeeded, 'add a bloking role to iptables fail. output:%s' % out)

            for i in range(1):
                util.log('waiting... %d' % (i + 1))
                time.sleep(1)

            # Unblock network
            util.log('\n\n\n ### UNBLOCK NETWORK, %d ### ' % loop_cnt)
            out = util.sudo('iptables -D OUTPUT -d 127.0.0.102 -j DROP')
            self.assertTrue(out.succeeded, 'delete a bloking role to iptables fail. output:%s' % out)
            for i in range(0):
                util.log('waiting... %d' % (i + 1))
                time.sleep(1)
            out = util.sudo('iptables -D OUTPUT -d 127.0.0.100 -j DROP')
            self.assertTrue(out.succeeded, 'delete a bloking role to iptables fail. output:%s' % out)
            for i in range(0):
                util.log('waiting... %d' % (i + 1))
                time.sleep(1)
            out = util.sudo('iptables -D OUTPUT -d 127.0.0.101 -j DROP')
            self.assertTrue(out.succeeded, 'delete a bloking role to iptables fail. output:%s' % out)
            for i in range(3):
                util.log('waiting... %d' % (i + 1))
                time.sleep(1)

            # Print state of cluster
            util.log('\n ### STATE OF CLUSTER ### ')
            cluster_state = False
            for i in range(10):
                cluster_state = util.check_cluster(cluster_name, mgmt_ip, mgmt_port, initial_state, check_quorum=True)
                if cluster_state == True:
                    break
                else:
                    time.sleep(1)
            self.assertTrue(cluster_state, 'failed to check cluster state')

            all_in_f = True
            for s in cluster['servers']:
                if checkLastState(mgmt_ip, s['cm_port'], cluster_name, 0, 'F') == False:
                    all_in_f = False
                    break

            self.assertFalse(all_in_f, 'PGS0`s last state remains in F')

        # Shutdown cluster
        ret = default_cluster.finalize( cluster )
        self.assertEqual(ret, 0, 'failed to TestMaintenance.finalize')

        # Delete forwarding role
        out = util.sudo('iptables -t nat -D OUTPUT -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'delete a forwarding role to iptables fail. output:%s' % out)
        out = util.sudo('iptables -t nat -D PREROUTING -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'delete a forwarding role to iptables fail. output:%s' % out)

        out = util.sudo('iptables -t nat -D OUTPUT -d 127.0.0.101 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'delete a forwarding role to iptables fail. output:%s' % out)
        out = util.sudo('iptables -t nat -D PREROUTING -d 127.0.0.101 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'delete a forwarding role to iptables fail. output:%s' % out)

        out = util.sudo('iptables -t nat -D OUTPUT -d 127.0.0.102 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'delete a forwarding role to iptables fail. output:%s' % out)
        out = util.sudo('iptables -t nat -D PREROUTING -d 127.0.0.102 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'delete a forwarding role to iptables fail. output:%s' % out)

def checkLastState(mgmt_ip, mgmt_port, cluster_name, pgs_id, state):
    pgs = util.get_pgs_info_all(mgmt_ip, mgmt_port, cluster_name, pgs_id)
    util.log('PGS:%d, LastState:%s, LastTimestamp:%d' % (pgs_id, pgs['HBC']['lastState'], pgs['HBC']['lastStateTimestamp']))
    if state == pgs['HBC']['lastState']:
        return True
    else:
        return False
