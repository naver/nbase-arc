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
import telnetlib
import os

class TestCheckPointAndLog( unittest.TestCase ):
    cluster = config.clusters[0]

    @classmethod
    def setUpClass( cls ):
        return 0

    @classmethod
    def tearDownClass( cls ):
        return 0

    def setUp( self ):
        util.set_process_logfile_prefix( 'TestCheckPointAndLog_%s' % self._testMethodName )
        self.conf_checker = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
        self.assertIsNotNone(self.conf_checker, 'failed to initialize cluster')

    def tearDown( self ):
        testbase.defaultTearDown(self)

    def put_some_data( self ):
        # start load generator
        max_load_generator = 100
        load_gen_thrd_list = {}
        util.log('start load_generator')
        for i in range(max_load_generator):
            ip, port = util.get_rand_gateway(self.cluster)
            load_gen_thrd_list[i] = load_generator.LoadGenerator(i, ip, port)
            load_gen_thrd_list[i].start()

        time.sleep(10) # generate some load

        util.log('end load_generator')

        # check consistency of load_generator
        for i in range(len(load_gen_thrd_list)):
            load_gen_thrd_list[i].quit()
        for i in range(len(load_gen_thrd_list)):
            load_gen_thrd_list[i].join()
            self.assertTrue(load_gen_thrd_list[i].isConsistent(), 'Data are inconsistent.')
        return 0

    def recovery_with_local_checkpoint_and_remote_log( self, role ):
        server = util.get_server_by_role( self.cluster['servers'], role )

        # set initial data in order to make an elapsed time for bgsave longer
        self.put_some_data()

        # set value
        ip, port = util.get_rand_gateway( self.cluster )
        gw = gateway_mgmt.Gateway( server['id'] )
        ret = gw.connect( ip, port )
        self.assertEqual( ret, 0, 'failed to connect to gateway, id:%d' % server['id'] )
        timestamp = {}
        key_base = 'key0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999'
        for i in range (0, 50000):
            timestamp[i] = time.time()
            k = '%s_%d' % (key_base, i)
            cmd = 'set %s %f\r\n' % (k, timestamp[i])
            gw.write( cmd )
            response = gw.read_until( '\r\n' )
            self.assertNotEqual( response.find( '+OK' ), -1, 'failed to set key value through gateway' )

        # generate a check point
        bgsave_ret = util.bgsave( server )
        self.assertTrue( bgsave_ret, 'failed to bgsave. pgs%d' % server['id'] )

        # shutdown
        ret = testbase.request_to_shutdown_smr( server )
        self.assertEqual( ret, 0, 'failed to shutdown smr' )
        ret = testbase.request_to_shutdown_redis( server )
        self.assertEqual( ret, 0, 'failed to shutdown redis' )
        util.log('succeeded : shutdown pgs%d' % (server['id']))

        # delete smr_logs
        ret = util.delete_smr_logs( server['id'] )
        self.assertEqual( ret, 0, 'failed to delete smr log, id:%d' % server['id'] )
        util.log('succeeded : delete replication logs')

        time.sleep( 5 )

        # set value
        ret = gw.connect( ip, port )
        self.assertEqual( ret, 0, 'failed to connect to gateway' )
        for i in range (50000, 100000):
            timestamp[i] = time.time()
            k = '%s_%d' % (key_base, i)
            cmd = 'set %s %f\r\n' % (k, timestamp[i])
            gw.write( cmd )
            response = gw.read_until( '\r\n' )
            self.assertNotEqual( response.find( '+OK' ), -1, 'failed to set key value through gateway' )

        # recovery
        ret = testbase.request_to_start_smr( server )
        self.assertEqual( ret, 0, 'failed to start smr' )

        ret = testbase.request_to_start_redis( server )
        self.assertEqual( ret, 0, 'failed to start redis' )
        time.sleep( 5 )

        ret = testbase.wait_until_finished_to_set_up_role( server )
        self.assertEquals( ret, 0, 'failed to role change. smr_id:%d' % (server['id']) )
        util.log('succeeded : recover pgs%d' % server['id'])

        # check value
        recovered_redis = redis_mgmt.Redis( server['id'] )
        ret = recovered_redis .connect( server['ip'], server['redis_port'] )
        self.assertEquals( ret, 0, 'failed to connect to redis' )

        for i in range (0, 100000):
            k = '%s_%d' % (key_base, i)
            cmd = 'get %s\r\n' % (k)
            recovered_redis .write( cmd )
            recovered_redis.read_until( '\r\n'  )
            response = recovered_redis.read_until( '\r\n'  )
            self.assertEqual( response, '%f\r\n' % (timestamp[i]), 'inconsistent %s, %f' % (response, timestamp[i]) )

    def test_restart_recovery_with_local_checkpoint_and_remote_log_about_slave( self ):
        util.print_frame()
        for i in range( 0, 5 ):
            self.recovery_with_local_checkpoint_and_remote_log( 'slave' )

    def test_restart_recovery_with_local_checkpoint_and_remote_log_about_master( self ):
        util.print_frame()
        for i in range( 0, 5 ):
            self.recovery_with_local_checkpoint_and_remote_log( 'master' )

    def test_restart_recovery_with_local_checkpoint_and_local_log( self ):
        util.print_frame()
        key_base = 'key'
        target = util.get_server_by_role( self.cluster['servers'], 'slave' )
        master = util.get_server_by_role( self.cluster['servers'], 'master' )

        ip, port = util.get_rand_gateway( self.cluster )
        gw = gateway_mgmt.Gateway( master['id'] )
        ret = gw.connect( ip, port )
        self.assertEqual( ret, 0, 'failed to connect to gateway' )

        # set initial data in order to make an elapsed time for bgsave longer
        self.put_some_data()

        # generate some data
        for i in range( 0, 100 ):
            key = '%s%d' % (key_base, i)
            cmd = 'set %s %d\r\n' % (key, i)
            gw.write( cmd )
            res = gw.read_until( '\r\n' )
            self.assertEquals( res, '+OK\r\n' )
        gw.disconnect()

        # generate a local check point
        bgsave_ret = util.bgsave( target )
        self.assertTrue( bgsave_ret, 'failed to bgsave. pgs%d' % target['id'] )

        # generate some data
        ret = gw.connect( ip, port )
        self.assertEqual( ret, 0, 'failed to connect to gateway' )
        for i in range( 100, 200 ):
            key = '%s%d' % (key_base, i)
            cmd = 'set %s %d\r\n' % (key, i)
            gw.write( cmd )
            res = gw.read_until( '\r\n' )
            self.assertEquals( res, '+OK\r\n' )
        gw.disconnect()

        # shutdown
        util.log('shutdown target')
        ret = testbase.request_to_shutdown_smr( target )
        self.assertEqual( ret, 0, 'failed to shutdown smr' )

        time.sleep( 5 )

        # set value
        ret = gw.connect( ip, port )
        self.assertEqual( ret, 0, 'failed to connect to gateway' )
        for i in range( 200, 300 ):
            key = '%s%d' % (key_base, i)
            cmd = 'set %s %d\r\n' % (key, i)
            gw.write( cmd )
            res = gw.read_until( '\r\n' )
            self.assertEquals( res, '+OK\r\n' )
        gw.disconnect()

        # recovery
        util.log('recovery target')
        ret = testbase.request_to_start_smr( target )
        self.assertEqual( ret, 0, 'failed to start smr' )

        ret = testbase.request_to_start_redis( target )
        self.assertEqual( ret, 0, 'failed to start redis' )
        time.sleep( 5 )

        ret = testbase.wait_until_finished_to_set_up_role( target)
        self.assertEquals( ret, 0, 'failed to role change. smr_id:%d' % (target['id']) )

        # check value
        recovered_redis = redis_mgmt.Redis( target['id'] )
        ret = recovered_redis .connect( target['ip'], target['redis_port'] )
        self.assertEquals( ret, 0, 'failed to connect to redis' )

        for i in range (0, 300):
            key = '%s%d' % (key_base, i)
            cmd = 'get %s\r\n' % (key)
            recovered_redis .write( cmd )
            recovered_redis.read_until( '\r\n'  )
            response = recovered_redis.read_until( '\r\n'  )
            self.assertEqual( response, '%d\r\n' % i, 'inconsistent %s, %d' % (response, i) )

    def test_restart_recovery_with_remote_checkpoint_and_remote_log( self ):
        util.print_frame()
        key_base = 'key'
        target = util.get_server_by_role( self.cluster['servers'], 'slave' )
        master = util.get_server_by_role( self.cluster['servers'], 'master' )

        ip, port = util.get_rand_gateway( self.cluster )
        gw = gateway_mgmt.Gateway( master['id'] )
        ret = gw.connect( ip, port )
        self.assertEqual( ret, 0, 'failed to connect to gateway' )

        # set initial data in order to make an elapsed time for bgsave longer
        self.put_some_data()

        # generate some data
        for i in range( 0, 100 ):
            key = '%s%d' % (key_base, i)
            cmd = 'set %s %d\r\n' % (key, i)
            gw.write( cmd )
            res = gw.read_until( '\r\n' )
            self.assertEquals( res, '+OK\r\n' )
        gw.disconnect()

        # delete a local checkpoint
        util.log('delete pgs%d`s check point.' % target['id'])
        util.del_dumprdb( target['id'] )

        # generate a remote check point
        bgsave_ret = util.bgsave( master )
        self.assertTrue( bgsave_ret, 'failed to bgsave. pgs%d' % master['id'] )

        # shutdown
        util.log('shutdown target')
        ret = testbase.request_to_shutdown_smr( target )
        self.assertEqual( ret, 0, 'failed to shutdown smr' )

        time.sleep( 10 )

        # generate some data
        ret = gw.connect( ip, port )
        self.assertEqual( ret, 0, 'failed to connect to gateway' )
        for i in range( 100, 200 ):
            key = '%s%d' % (key_base, i)
            cmd = 'set %s %d\r\n' % (key, i)
            gw.write( cmd )
            res = gw.read_until( '\r\n' )
            self.assertEquals( res, '+OK\r\n' )
        gw.disconnect()

        # recovery
        util.log('recovery target')
        ret = testbase.request_to_start_smr( target )
        self.assertEqual( ret, 0, 'failed to start smr' )

        ret = testbase.request_to_start_redis( target )
        self.assertEqual( ret, 0, 'failed to start redis' )
        time.sleep( 5 )

        ret = testbase.wait_until_finished_to_set_up_role( target)
        self.assertEquals( ret, 0, 'failed to role change. smr_id:%d' % (target['id']) )

        # check value
        recovered_redis = redis_mgmt.Redis( target['id'] )
        ret = recovered_redis .connect( target['ip'], target['redis_port'] )
        self.assertEquals( ret, 0, 'failed to connect to redis' )

        for i in range (0, 200):
            key = '%s%d' % (key_base, i)
            cmd = 'get %s\r\n' % (key)
            recovered_redis .write( cmd )
            recovered_redis.read_until( '\r\n'  )
            response = recovered_redis.read_until( '\r\n'  )
            self.assertEqual( response, '%d\r\n' % i, 'inconsistent %s, %d' % (response, i) )

    def bgsave(self, redis):
        redis.write('time\r\n')
        redis.read_until('\r\n', 1)
        redis.read_until('\r\n', 1)
        ret = redis.read_until('\r\n', 1)
        before_save_time = int(ret.strip())
        redis.read_until('\r\n', 1)
        redis.read_until('\r\n', 1)

        time.sleep(1.1)

        redis.write('bgsave\r\n')
        ret = redis.read_until('\r\n', 1)
        self.assertEqual(ret, '+Background saving started\r\n')

        # Wait finishing bgsave
        while True:
            redis.write('lastsave\r\n')
            ret = redis.read_until('\r\n', 1)
            lastsave_time = int(ret[1:].strip())
            if lastsave_time > before_save_time: break
            time.sleep(0.1)

    def test_delete_smrlog_after_redis_restart(self):
        util.print_frame()

        server = self.cluster['servers'][0]
        redis = telnetlib.Telnet(server['ip'], server['redis_port'])

        val = 'x' * 1048576
        cmd = '*3\r\n$3\r\nset\r\n$4\r\ntest\r\n$1048576\r\n%s\r\n' % val

        # create smr log file
        for i in xrange(640):
            redis.write(cmd)
            ret = redis.read_until('\r\n', 3)
            self.assertEquals(ret, '+OK\r\n')

        # wait until synced
        if config.opt_use_memlog:
            time.sleep(3)

        loglist = [f for f in os.listdir('%s/log0' % util.smr_dir(0)) if '.log' in f]
        util.log('before log delete')
        util.log(loglist)

        self.assertTrue(len(loglist) > 10)

        self.bgsave(redis)

        testbase.request_to_shutdown_redis(server)
        testbase.request_to_shutdown_smr(server)

        testbase.request_to_start_smr(server, log_delete_delay=1)
        testbase.request_to_start_redis(server)

        time.sleep(30)
        loglist = [f for f in os.listdir('%s/log0' % util.smr_dir(0)) if '.log' in f]
        util.log('after log delete')
        util.log(loglist)

        # wait until synced
        if config.opt_use_memlog:
            time.sleep(3)

        self.assertTrue(len(loglist) < 5)
