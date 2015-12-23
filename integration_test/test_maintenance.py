import subprocess
import util
import unittest
import testbase
import default_cluster
import os
import smr_mgmt
import redis_mgmt
import time
import random
import load_generator
import config
import demjson
import gateway_mgmt

class TestMaintenance(unittest.TestCase):
    cluster = config.clusters[0]
    max_load_generator = 128
    leader_cm = config.clusters[0]['servers'][0]
    load_gen_list = {}

    @classmethod
    def setUpClass( cls ):
        return 0

    @classmethod
    def tearDownClass( cls ):
        return 0

    def setUp(self):
        util.set_process_logfile_prefix( 'TestMaintenance_%s' % self._testMethodName )
        if default_cluster.initialize_starting_up_smr_before_redis( self.cluster ) is not 0:
            util.log('failed to TestMaintenance.initialize')
            return -1
        return 0

    def tearDown(self):
        # shutdown load generators
        for load_gen_id, load_gen in self.load_gen_list.items():
            load_gen.quit()
            load_gen.join()
            self.load_gen_list.pop(load_gen_id, None)

        if default_cluster.finalize( self.cluster ) is not 0:
            util.log('failed to TestMaintenance.finalize')

        return 0

    def __del_server(self, server_to_del):
        # backup data
        redis = redis_mgmt.Redis( server_to_del['id'] )
        ret = redis.connect( server_to_del['ip'], server_to_del['redis_port'] )
        self.assertEquals( ret, 0, 'failed : connect to smr%d(%s:%d)' % (server_to_del['id'], server_to_del['ip'], server_to_del['redis_port']) )

        # bgsave
        ret = util.bgsave(server_to_del)
        self.assertTrue(ret, 'failed to bgsave. pgs%d' % server_to_del['id'])

        # detach pgs from cluster
        cmd = 'pgs_leave %s %d\r\n' % (server_to_del['cluster_name'], server_to_del['id'])
        ret = util.cm_command( self.leader_cm['ip'], self.leader_cm['cm_port'], cmd )
        json = demjson.decode(ret)
        self.assertEqual( json['msg'], '+OK', 'failed : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )
        util.log( 'succeeded : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )

        # check if pgs is removed
        success = False
        for try_cnt in range( 10 ):
            redis = redis_mgmt.Redis( server_to_del['id'] )
            ret = redis.connect( server_to_del['ip'], server_to_del['redis_port'] )
            self.assertEquals( ret, 0, 'failed : connect to smr%d(%s:%d)' % (server_to_del['id'], server_to_del['ip'], server_to_del['redis_port']) )
            util.log( 'succeeded : connect to smr%d(%s:%d)' % (server_to_del['id'], server_to_del['ip'], server_to_del['redis_port']) )

            redis.write( 'info stats\r\n' )
            for i in range( 6 ):
                redis.read_until( '\r\n' )
            res = redis.read_until( '\r\n' )
            self.assertNotEqual( res, '', 'failed : get reply of "info stats" from redis%d(%s:%d)' % (server_to_del['id'], server_to_del['ip'], server_to_del['redis_port']) )
            util.log( 'succeeded : get reply of "info stats" from redis%d(%s:%d), reply="%s"' % (server_to_del['id'], server_to_del['ip'], server_to_del['redis_port'], res[:-2]) )
            no = int( res.split(':')[1] )
            if no <= 100:
                success = True
                break
            time.sleep( 1 )

        self.assertEquals( success, True, 'failed : pgs does not removed.' )
        util.log( 'succeeded : pgs is removed' )

        # change state of pgs to lconn
        cmd = 'pgs_lconn %s %d\r\n' % (server_to_del['cluster_name'], server_to_del['id'])
        ret = util.cm_command( self.leader_cm['ip'], self.leader_cm['cm_port'], cmd )
        json = demjson.decode(ret)
        self.assertEqual( json['msg'], '+OK', 'failed : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )
        util.log( 'succeeded : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )

        # shutdown
        ret = testbase.request_to_shutdown_smr( server_to_del )
        self.assertEqual( ret, 0, 'failed : shutdown smr. id:%d' % server_to_del['id'] )
        ret = testbase.request_to_shutdown_redis( server_to_del )
        self.assertEquals( ret, 0, 'failed : shutdown redis. id:%d' % server_to_del['id'] )
        util.log('succeeded : shutdown pgs%d.' % server_to_del['id'] )

        # delete pgs from cluster
        cmd = 'pgs_del %s %d\r\n' % (server_to_del['cluster_name'], server_to_del['id'])
        ret = util.cm_command( self.leader_cm['ip'], self.leader_cm['cm_port'], cmd )
        json = demjson.decode(ret)
        self.assertEqual( json['msg'], '+OK', 'failed : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )
        util.log( 'succeeded : cmd="%s", reply="%s"' % (cmd[:-2], ret[:-2]) )

    def __check_quorum(self, m, expected):
        time.sleep(1)
        for try_cnt in range(10):
            quorum = util.get_quorum(m)
            if quorum == expected:
                util.log('quorum: %d, master: %d, try_cnt: %d, OK' % (quorum, m['id'], try_cnt))
                return True
            else:
                util.log('quorum: %d, master: %d, try_cnt: %d' % (quorum, m['id'], try_cnt))
            time.sleep(0.5)
        return False

    def test_1_role_change(self):
        util.print_frame()

        self.load_gen_list = {}

        # Start load generator
        util.log("Start load_generator")
        for i in range(self.max_load_generator):
            ip, port = util.get_rand_gateway(self.cluster)
            load_gen = load_generator.LoadGenerator(i, ip, port)
            load_gen.start()
            self.load_gen_list[i] = load_gen

        # Loop (smr: 3 copy)
        target_server = util.get_server_by_role(self.cluster['servers'], 'slave')
        self.assertNotEquals(target_server, None, 'Get slave fail.')
        target = target_server['id']
        for i in range(30):
            print ''
            util.log("(3 copy) Loop:%d, target pgs:%d" % (i, target))

            # Get old timestamp
            util.log_server_state( self.cluster )
            old_timestamp_list = []
            for s in self.cluster['servers']:
                ts = util.get_timestamp_of_pgs( s )
                old_timestamp_list.append(ts)

            # Role change
            master = util.role_change(self.leader_cm, self.cluster['cluster_name'], target)
            self.assertNotEqual(master, -1, 'role_change error.')
            while target == master:
                target = (target + 1) % 3
            util.log('Change role success.')

            # Wait until role change finished
            for s in self.cluster['servers']:
                max_try_cnt = 20
                ok = False
                for try_cnt in range(max_try_cnt):
                    pong = util.pingpong(s['ip'], s['redis_port'])
                    if pong != None and pong == '+PONG\r\n':
                        ok = True
                        break
                    time.sleep(0.1)
                self.assertTrue(ok, 'redis state error.')

            # Get new timestamp
            util.log_server_state( self.cluster )
            new_timestamp_list = []
            for s in self.cluster['servers']:
                ts = util.get_timestamp_of_pgs( s )
                new_timestamp_list.append(ts)

            # Compare old timestamps and new timestamps
            for i in range(3):
                self.assertNotEqual(old_timestamp_list[i], new_timestamp_list[i],
                    'Timestamp is not changed. %d->%d' % (old_timestamp_list[i], new_timestamp_list[i]))

            # Cheeck Consistency
            for load_gen_id, load_gen in self.load_gen_list.items():
                self.assertTrue(load_gen.isConsistent(), 'Data inconsistency after role_change')

        # Loop (smr: 2 copy)
        self.__del_server(self.cluster['servers'][0])
        servers = [self.cluster['servers'][1], self.cluster['servers'][2]]
        s = util.get_server_by_role(servers, 'slave')
        target = s['id']
        for i in range(30):
            print ''
            util.log("(2 copy) Loop:%d, target pgs:%d" % (i, target))

            # Get old timestamp
            util.log_server_state( self.cluster )
            old_timestamp_list = []
            for s in servers:
                ts = util.get_timestamp_of_pgs( s )
                old_timestamp_list.append(ts)

            # Role change
            master = util.role_change(self.leader_cm, self.cluster['cluster_name'], target)
            self.assertNotEqual(master, -1, 'role_change error.')
            while target == master:
                target = (target) % 2 + 1
            util.log('Change role success.')

            # Wait until role change finished
            for s in servers:
                max_try_cnt = 20
                ok = False
                for try_cnt in range(max_try_cnt):
                    pong = util.pingpong(s['ip'], s['redis_port'])
                    if pong != None and pong == '+PONG\r\n':
                        ok = True
                        break
                    time.sleep(0.1)
                self.assertTrue(ok, 'redis state error.')

            # Get new timestamp
            util.log_server_state( self.cluster )
            new_timestamp_list = []
            for s in servers:
                ts = util.get_timestamp_of_pgs( s )
                new_timestamp_list.append(ts)

            # Compare old timestamps and new timestamps
            for i in range(2):
                self.assertNotEqual(old_timestamp_list[i], new_timestamp_list[i],
                    'Timestamp is not changed. %d->%d' % (old_timestamp_list[i], new_timestamp_list[i]))

            # Cheeck Consistency
            for load_gen_id, load_gen in self.load_gen_list.items():
                self.assertTrue(load_gen.isConsistent(), 'Data inconsistency after role_change')

    def role_change_with_hanging_pgs(self, hanging_servers, running_servers, target_id, master):
        util.log('hanging_servers:%s' % hanging_servers)
        util.log('running_servers:%s' % running_servers)
        util.log('target_id:%s' % target_id)

        # Initial data
        util.put_some_data(self.cluster, 3, 10)

        util.log("States (before role change)")
        util.log_server_state(self.cluster)

        # Get old timestamp
        old_timestamps = {}
        for s in self.cluster['servers']:
            ts = util.get_timestamp_of_pgs(s)
            old_timestamps[s['id']] = ts

        # hang
        for s in hanging_servers:
            smr = smr_mgmt.SMR(s['id'])
            ret = smr.connect(s['ip'], s['smr_mgmt_port'])
            self.assertEqual(ret, 0, 'failed to connect to master. %s:%d' % (s['ip'], s['smr_mgmt_port']))
            util.log("PGS '%d' hang" % s['id'])

            smr.write('fi delay sleep 1 13000\r\n')
            reply = smr.read_until('\r\n', 1)
            if reply != None and reply.find('-ERR not supported') != -1:
                self.assertEqual(0, 1, 'make sure that smr has compiled with gcov option.')
            smr.disconnect()

        # Role change
        master_id = util.role_change(self.leader_cm, self.cluster['cluster_name'], target_id)
        self.assertEqual(master_id, -1, 'We expected that role_change failed, but success')

        # Check rollback - check quorum
        if master not in hanging_servers:
            expected = 1
            ok = self.__check_quorum(master, expected)
            self.assertTrue(ok, 'rollback quorum fail. expected:%s' % (expected))

        # Check rollback - get new timestamp
        new_timestamps_in_hang = {}
        for s in running_servers:
            ts = util.get_timestamp_of_pgs( s )
            new_timestamps_in_hang[s['id']] = ts

        # Check rollback - compare old timestamps and new timestamps
        for s in running_servers:
            old_ts = old_timestamps[s['id']]
            new_ts = new_timestamps_in_hang[s['id']]
            self.assertEqual(old_ts, new_ts, 'Timestamp of a running server has changed. %d->%d' % (old_ts, new_ts))

        time.sleep(16)
        util.log("States (after role change)")
        util.log_server_state( self.cluster )

        self.load_gen_list = {}

        # Start load generator
        for i in range(self.max_load_generator):
            ip, port = util.get_rand_gateway(self.cluster)
            load_gen = load_generator.LoadGenerator(i, ip, port)
            load_gen.start()
            self.load_gen_list[i] = load_gen

        # Check quorum
        if master in hanging_servers:
            m, s1, s2 = util.get_mss(self.cluster)
            self.assertNotEqual(m, None, 'master is None.')
            self.assertNotEqual(s1, None, 'slave1 is None.')
            self.assertNotEqual(s2, None, 'slave2 is None.')

            expected = 1
            ok = self.__check_quorum(m, expected)
            self.assertTrue(ok, 'rollback quorum fail. expected:%s' % (expected))

        # Get new timestamp
        new_timestamps = {}
        for s in self.cluster['servers']:
            ts = util.get_timestamp_of_pgs( s )
            new_timestamps[s['id']] = ts

        # Compare old timestamps and new timestamps
        for s in self.cluster['servers']:
            old_ts = old_timestamps[s['id']]
            new_ts = new_timestamps[s['id']]
            if master in hanging_servers and len(running_servers) != 0:
                self.assertNotEqual(old_ts, new_ts, 'Timestamp of a hanging server has not changed. %d->%d' % (old_ts, new_ts))
            else:
                self.assertEqual(old_ts, new_ts, 'Timestamp of a running server has changed. %d->%d' % (old_ts, new_ts))

        # Cheeck Consistency
        for i in range(self.max_load_generator):
            self.load_gen_list[i].quit()
        for i in range(self.max_load_generator):
            self.load_gen_list[i].join()
            self.assertTrue(self.load_gen_list[i].isConsistent(), 'Inconsistent after migration')
            self.load_gen_list.pop(i, None)

    def test_2_role_change_with_hanging_pgs(self):
        util.print_frame()

        i = 0
        while i < 5:
            util.log('')
            util.log('Loop:%d' % i)

            # get master, slave1, slave2
            m, s1, s2 = util.get_mss(self.cluster)
            self.assertNotEqual(m, None, 'master is None.')
            self.assertNotEqual(s1, None, 'slave1 is None.')
            self.assertNotEqual(s2, None, 'slave2 is None.')

            hang = random.choice(self.cluster['servers'])
            if hang == m:
                hanging_servers = [m]
                running_servers = [s1, s2]
                type = 'master'
            else:
                hanging_servers = [s1]
                running_servers = [m, s2]
                type = 'slave'
            s = random.choice([s1, s2])

            util.log('hanging pgs(id:%d, type:%s), expected_master_id:%d' % (hang['id'], type, s['id']))
            self.role_change_with_hanging_pgs(hanging_servers, running_servers, s['id'], m)

            i += 1

    def test_3_role_change_while_all_pgs_hanging(self):
        util.print_frame()

        # get master, slave1, slave2
        m, s1, s2 = util.get_mss(self.cluster)
        self.assertNotEqual(m, None, 'master is None.')
        self.assertNotEqual(s1, None, 'slave1 is None.')
        self.assertNotEqual(s2, None, 'slave2 is None.')

        hanging_servers = [m, s1, s2]
        running_servers = []
        s = random.choice([s1, s2])

        self.role_change_with_hanging_pgs(hanging_servers, running_servers, s['id'], m)
        return 0

    def test_4_role_change_with_failover(self):
        util.print_frame()

        loop_cnt = 0
        while loop_cnt < 5:
            util.log('')
            util.log('Loop:%d' % loop_cnt)

            util.log("States (before role change)")
            util.log_server_state(self.cluster)

            target = random.choice(self.cluster['servers'])

            # bgsave
            ret = util.bgsave(target)
            self.assertTrue(ret, 'failed to bgsave. pgs:%d' % target['id'])

            # shutdown
            util.log('shutdown pgs%d(%s:%d)' % (target['id'], target['ip'], target['smr_base_port']))
            ret = testbase.request_to_shutdown_smr( target )
            self.assertEqual( ret, 0, 'failed to shutdown smr' )

            ret = testbase.request_to_shutdown_redis( target )
            self.assertEquals( ret, 0, 'failed to shutdown redis' )

            running_servers = []
            for s in self.cluster['servers']:
                if s != target:
                    running_servers.append(s)

            # Get old timestamp
            old_timestamps = {}
            for s in running_servers:
                ts = util.get_timestamp_of_pgs(s)
                old_timestamps[s['id']] = ts

            # Start load generator
            self.load_gen_list = {}
            util.log('start load generator')
            for i in range(self.max_load_generator):
                ip, port = util.get_rand_gateway(self.cluster)
                load_gen = load_generator.LoadGenerator(i, ip, port)
                load_gen.start()
                self.load_gen_list[i] = load_gen

            m, s1, s2 = util.get_mss(self.cluster)
            self.assertNotEqual(m, None, 'master is None.')
            self.assertNotEqual(s1, None, 'slave1 is None.')

            # Role change
            master_id = util.role_change(self.leader_cm, self.cluster['cluster_name'], s1['id'])
            self.assertNotEqual(master_id, -1, 'role_change failed')

            util.log("States (after role change)")
            util.log_server_state(self.cluster)

            # Check - get new timestamp
            new_timestamps= {}
            for s in running_servers:
                ts = util.get_timestamp_of_pgs( s )
                new_timestamps[s['id']] = ts

            # Check - compare old timestamps and new timestamps
            for s in running_servers:
                old_ts = old_timestamps[s['id']]
                new_ts = new_timestamps[s['id']]
                self.assertNotEqual(old_ts, new_ts, 'Timestamp of a running server has not changed. %d->%d' % (old_ts, new_ts))

            # Check quorum
            m = self.cluster['servers'][master_id]
            expected = 1
            ok = self.__check_quorum(m, expected)
            self.assertTrue(ok, 'unexpected quorum(after role change). expected:%s' % (expected))

            # recovery
            util.log('recovery pgs%d(%s:%d)' % (target['id'], target['ip'], target['smr_base_port']))
            ret = testbase.request_to_start_smr( target )
            self.assertEqual( ret, 0, 'failed to start smr' )
            util.log('start smr-replicator done')

            ret = testbase.request_to_start_redis( target, 60 )
            self.assertEqual( ret, 0, 'failed to start redis' )
            util.log('start redis-arc done')

            ret = testbase.wait_until_finished_to_set_up_role( target, max_try=300)
            self.assertEquals( ret, 0, 'failed to role change. smr_id:%d' % (target['id']) )

            util.log("States (after recovery)")
            util.log_server_state(self.cluster)

            # Check quorum
            expected = 1
            ok = self.__check_quorum(m, expected)
            self.assertTrue(ok, 'unexpected quorum(after recovery). expected:%s' % (expected))

            # Cheeck Consistency
            util.log('stop load generator')
            for i in range(self.max_load_generator):
                self.load_gen_list[i].quit()
            for i in range(self.max_load_generator):
                self.load_gen_list[i].join()
                self.assertTrue(self.load_gen_list[i].isConsistent(), 'Inconsistent after migration')
                self.load_gen_list.pop(i, None)

            loop_cnt += 1

        return 0

    def test_5_transfer_pgs_to_another_machine(self):
        util.print_frame()

        self.load_gen_list = {}

        # get gateway info
        ip, port = util.get_rand_gateway( self.cluster )
        gw = gateway_mgmt.Gateway( self.cluster['servers'][0]['id'] )
        ret = gw.connect( ip, port )
        self.assertEqual( ret, 0, 'failed to connect to gateway, %s:%d' % (ip, port) )

        # incrase master generation number
        util.log('failover in order to increase master generation number.')
        max = 0
        for i in range(5):
            key_base = 'key'
            for i in range(max, max+10000):
                cmd = 'set %s%d %d\r\n' % (key_base, i, i)
                gw.write( cmd )
                res = gw.read_until( '\r\n' )
                self.assertEquals( res, '+OK\r\n' )
            max = max + 10000

            m = util.get_server_by_role(self.cluster['servers'], 'master')
            util.log('failover pgs%d' %  m['id'])
            ret = util.failover(m, self.leader_cm)
            self.assertTrue(ret, 'failed to failover pgs%d' % m['id'])

        # start load generator
        util.log("start load_generator")
        for i in range(self.max_load_generator):
            ip, port = util.get_rand_gateway(self.cluster)
            self.load_gen_list[i] = load_generator.LoadGenerator(i, ip, port)
            self.load_gen_list[i].start()

        time.sleep(5) # generate load for 5 sec
        util.log("started load_generator")

        m, s1, s2 = util.get_mss(self.cluster)
        servers = [m, s1, s2]

        # bgsave
        for s in servers:
            ret = util.bgsave(s)
            self.assertTrue(ret, 'failed to bgsave. pgs%d' % s['id'])

        new_servers = [config.server4, config.server5]

        # add new slaves
        for s in new_servers:
            util.log('delete pgs%d`s check point.' % s['id'])
            util.del_dumprdb(s['id'])

            ret = util.cluster_util_getdump(s['id'], m['ip'], m['redis_port'], 'dump.rdb', 0, 8191)
            self.assertEqual(True, ret,
                'failed : util.cluster_util_getdump returns false, src=%s:%d dest_pgsid=%d' % (
                m['ip'], m['redis_port'], s['id']))

            ret = util.pgs_add(self.cluster, s, self.leader_cm, 0, rm_ckpt=False)
            self.assertEqual(True, ret, 'failed : util.pgs_add returns false, pgsid=%d' % s['id'])
            util.log('succeeeded : add a new slave, pgsid=%d' % s['id'])

            # check consistency
            ok = True
            for j in range(self.max_load_generator):
                if self.load_gen_list[j].isConsistent() == False:
                    ok = False
                    break
            if not ok:
                break;

        for server_to_del in servers:
            for s in servers:
                util.pingpong( s['ip'], s['smr_mgmt_port'] )
            for s in new_servers:
                util.pingpong( s['ip'], s['smr_mgmt_port'] )
            self.__del_server(server_to_del)
            util.log('succeeded : delete pgs%d' % server_to_del['id'])

        new_m = util.get_server_by_role(new_servers, 'master')
        new_s = util.get_server_by_role(new_servers, 'slave')
        self.assertNotEqual( new_m, None, 'master is None.' )
        self.assertNotEqual( new_s, None, 'slave is None.' )

        for s in new_servers:
            util.pingpong( s['ip'], s['smr_mgmt_port'] )

        time.sleep(5) # generate load for 5 sec
        # check consistency of load_generator
        for i in range(self.max_load_generator):
            self.load_gen_list[i].quit()
        for i in range(self.max_load_generator):
            self.load_gen_list[i].join()
            self.assertTrue(self.load_gen_list[i].isConsistent(), 'Inconsistent after migration')
            self.load_gen_list.pop(i, None)
