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

import subprocess
import util
import unittest
import testbase
import default_cluster
import os
import smr_mgmt
import redis_mgmt
import time
import load_generator
import config
import telnetlib
import random

class TestRedisMgmt(unittest.TestCase):
    cluster = config.clusters[1]
    max_load_generator = 128
    load_gen_thrd_list = {}

    @classmethod
    def setUpClass( cls ):
        return 0

    @classmethod
    def tearDownClass( cls ):
        return 0

    def setUp(self):
        util.set_process_logfile_prefix( 'TestRedisMgmt_%s' % self._testMethodName )
        self.conf_checker = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
        self.assertIsNotNone(self.conf_checker, 'failed to initialize cluster')

    def tearDown(self):
        testbase.defaultTearDown(self)

    def pgs_del_server(self, gw, server, n):
        gw.write("pgs_del %d %d\r\n" % (server['id']+n*6, server['pg_id']+n*2))
        gw.read_until("+OK\r\n")

    def pgs_add_server(self, gw, server, n):
        gw.write("pgs_add %d %d %s %d\r\n" % (server['id']+n*6, server['pg_id']+n*2, server['ip'], server['redis_port']))
        gw.read_until("+OK\r\n")

    def test_rdb_backups(self):
        util.print_frame()
        bgsave_count = 50
        org_path = os.getcwd()
        os.chdir(util.redis_dir(0))
        server0 = self.cluster['servers'][0]
        redis0 = telnetlib.Telnet(server0['ip'], server0['redis_port'])

        util.log("Starting load generator")
        for i in range(self.max_load_generator):
            ip, port = util.get_rand_gateway(self.cluster)
            self.load_gen_thrd_list[i] = load_generator.LoadGenerator(i, ip, port)
            self.load_gen_thrd_list[i].start()

        util.log("Set the number of rdb backups = 24")
        redis0.write("config set number-of-rdb-backups 24\r\n")
        redis0.read_until("+OK\r\n")

        util.log("Clear old rdb backups\r\n")
        for f in os.listdir('.'):
            if (f.endswith('.rdb')):
                os.remove(f)

        util.log("Bgsaving continuously and counting the number of rdb backups")
        for i in range(bgsave_count):
            # Save current time before Bgsaving
            redis0.write('time\r\n')
            redis0.read_until('\r\n', 1)
            redis0.read_until('\r\n', 1)
            ret = redis0.read_until('\r\n', 1)
            redis_server_time = int(ret.strip())
            redis0.read_until('\r\n', 1)
            redis0.read_until('\r\n', 1)

            time.sleep(1.1)

            redis0.write('time\r\n')
            redis0.read_until('\r\n', 1)
            redis0.read_until('\r\n', 1)
            ret = redis0.read_until('\r\n', 1)
            self.assertNotEqual(redis_server_time, int(ret.strip()))
            redis0.read_until('\r\n', 1)
            redis0.read_until('\r\n', 1)

            util.log("%d ~ %d" % (redis_server_time, int(ret.strip())))

            # Bgsave
            redis0.write("bgsave\r\n")
            ret = redis0.read_until('\r\n', 1)
            self.assertEqual('+Background saving started\r\n', ret)

            # Wait finishing bgsave
            while True:
                redis0.write('lastsave\r\n')
                ret = redis0.read_until('\r\n', 1)
                lastsave_time = int(ret[1:].strip())
                if lastsave_time > redis_server_time: break
                time.sleep(0.1)

            # Count the number of rdb backups
            rdb_list = [name for name in os.listdir('.')
                if os.path.isfile(name)
                    and name.startswith('dump') and name.endswith('.rdb')]

            util.log(rdb_list)
            util.log("Iteration:%d, rdb Backups:%d" % (i+1, len(rdb_list)))

            self.assertTrue(i+1 > 24 and len(rdb_list) == 25 or len(rdb_list) == i+1)
            self.assertTrue('dump.rdb' in rdb_list)


        util.log("\nSet the number of rdb backups = 5")
        redis0.write("config set number-of-rdb-backups 5\r\n")
        redis0.read_until("+OK\r\n")

        for i in range(3):
            # Save current time before Bgsaving
            redis0.write('time\r\n')
            redis0.read_until('\r\n', 1)
            redis0.read_until('\r\n', 1)
            ret = redis0.read_until('\r\n', 1)
            redis_server_time = int(ret.strip())
            redis0.read_until('\r\n', 1)
            redis0.read_until('\r\n', 1)

            time.sleep(1.1)

            # Bgsave
            redis0.write("bgsave\r\n")
            ret = redis0.read_until('\r\n', 1)
            self.assertEqual('+Background saving started\r\n', ret)

            # Wait finishing bgsave
            while True:
                redis0.write('lastsave\r\n')
                ret = redis0.read_until('\r\n', 1)
                lastsave_time = int(ret[1:].strip())
                if lastsave_time > redis_server_time: break
                time.sleep(0.1)

            # Count the number of rdb backups
            rdb_list = [name for name in os.listdir('.')
                if os.path.isfile(name)
                    and name.startswith('dump') and name.endswith('.rdb')]

            util.log(rdb_list)
            util.log("Iteration:%d, rdb Backups:%d" % (i+1, len(rdb_list)))

            self.assertTrue(len(rdb_list) == 6)
            self.assertTrue('dump.rdb' in rdb_list)


        # check consistency of load_generator
        for i in range(len(self.load_gen_thrd_list)):
            self.load_gen_thrd_list[i].quit()
        for i in range(len(self.load_gen_thrd_list)):
            self.load_gen_thrd_list[i].join()
            self.assertTrue(self.load_gen_thrd_list[i].isConsistent(), 'Inconsistent after gateway_mgmt test')
        os.chdir(org_path)

    def checkOOM(self, redis):
        redis.write("set oom_check value\r\n")
        ret = redis.read_until("\r\n");
        if ret == '+OK\r\n':
            return False
        elif ret == "-OOM command not allowed when used memory > 'maxmemory'.\r\n":
            return True
        util.log("checkOOM result = %s" % ret)
        return False

    def start_load_generator(self, num):
        for i in range(num):
            ip, port = util.get_rand_gateway(self.cluster)
            self.load_gen_thrd_list[i] = load_generator.LoadGenerator(i, ip, port)
            self.load_gen_thrd_list[i].start()

    def end_load_generator(self):
        for i in range(len(self.load_gen_thrd_list)):
            self.load_gen_thrd_list[i].quit()
        for i in range(len(self.load_gen_thrd_list)):
            self.load_gen_thrd_list[i].join()

    def test_memory_limit(self):
        util.print_frame()
        server0 = self.cluster['servers'][0]
        server1 = self.cluster['servers'][1]
        server2 = self.cluster['servers'][2]
        redis0 = telnetlib.Telnet(server0['ip'], server0['redis_port'])
        redis1 = telnetlib.Telnet(server1['ip'], server1['redis_port'])
        redis2 = telnetlib.Telnet(server2['ip'], server2['redis_port'])

        util.log("Set memory limit config")
        redis0.write("config set memory-limit-activation-percentage 50\r\n")
        redis0.read_until("+OK\r\n")
        redis0.write("config set memory-max-allowed-percentage 70\r\n")
        redis0.read_until("+OK\r\n")
        redis0.write("config set memory-hard-limit-percentage 80\r\n")
        redis0.read_until("+OK\r\n")

        self.assertFalse(self.checkOOM(redis2))
        self.assertFalse(self.checkOOM(redis1))
        self.assertFalse(self.checkOOM(redis0))

        util.log("Injecting memory stats")

        self.start_load_generator(5)
        redis0.write("debug memory 1000000 499999 0 100000\r\n")
        redis0.read_until("+OK\r\n")
        time.sleep(11)
        self.end_load_generator()

        self.assertFalse(self.checkOOM(redis2))
        self.assertFalse(self.checkOOM(redis1))
        self.assertFalse(self.checkOOM(redis0))

        self.start_load_generator(5)
        redis0.write("debug memory 1000000 499999 0 140001\r\n")
        redis0.read_until("+OK\r\n")
        time.sleep(11)
        self.end_load_generator()

        self.assertTrue(self.checkOOM(redis2))
        self.assertTrue(self.checkOOM(redis1))
        self.assertTrue(self.checkOOM(redis0))

        self.start_load_generator(5)
        redis0.write("debug memory 1000000 499999 0 139999\r\n")
        redis0.read_until("+OK\r\n")
        time.sleep(11)
        self.end_load_generator()

        self.assertFalse(self.checkOOM(redis2))
        self.assertFalse(self.checkOOM(redis1))
        self.assertFalse(self.checkOOM(redis0))

        self.start_load_generator(5)
        redis0.write("debug memory 1000000 500001 0 200000\r\n")
        redis0.read_until("+OK\r\n")
        time.sleep(11)
        self.end_load_generator()

        self.assertFalse(self.checkOOM(redis2))
        self.assertFalse(self.checkOOM(redis1))
        self.assertFalse(self.checkOOM(redis0))

        self.start_load_generator(5)
        redis0.write("debug memory 1000000 199999 0 100000\r\n")
        redis0.read_until("+OK\r\n")
        time.sleep(11)
        self.end_load_generator()

        self.assertTrue(self.checkOOM(redis2))
        self.assertTrue(self.checkOOM(redis1))
        self.assertTrue(self.checkOOM(redis0))

        self.start_load_generator(5)
        redis0.write("debug memory 1000000 500001 0 100000\r\n")
        redis0.read_until("+OK\r\n")
        time.sleep(11)
        self.end_load_generator()

        self.assertFalse(self.checkOOM(redis2))
        self.assertFalse(self.checkOOM(redis1))
        self.assertFalse(self.checkOOM(redis0))

        redis0.write("debug memory 1000000 199999 0 100000\r\n")
        redis0.read_until("+OK\r\n")
        time.sleep(11)

        redis0.write("debug memory 1000000 500001 0 100000\r\n")
        redis0.read_until("+OK\r\n")
        time.sleep(11)

        self.assertFalse(self.checkOOM(redis0))
        self.assertFalse(self.checkOOM(redis1))
        self.assertFalse(self.checkOOM(redis2))

        util.log("Check digest value of each redis server")
        redis2.write("debug digest\r\n")
        redis1.write("debug digest\r\n")
        redis0.write("debug digest\r\n")

        ret = redis0.read_until("\r\n")
        self.assertEqual(ret, redis1.read_until("\r\n"))
        self.assertEqual(ret, redis2.read_until("\r\n"))

    def test_memory_limit_during_migration(self):
        pass
#        TODO
#        util.print_frame()
#
#        util.log("Set memory limit config")
#        redis0.write("config set memory-limit-activation-percentage 50\r\n")
#        redis0.read_until("+OK\r\n")
#        redis0.write("config set memory-max-allowed-percentage 70\r\n")
#        redis0.read_until("+OK\r\n")
#        redis0.write("config set memory-hard-limit-percentage 80\r\n")
#        redis0.read_until("+OK\r\n")
#
#        self.start_load_generator(5)
#
#        ret = util.migration(self.cluster, 0 1, 4096, 8191, 40000)
#        self.assertEqual(True, ret, 'Migration Fail')
#
#        self.end_load_generator()

    def expect(self, conn, query, reply):
        conn.write(query)
        ret = conn.read_until('\r\n')
        self.assertEquals(ret, reply)

    def get_used_memory(self, conn):
        used_memory = -1

        conn.write("INFO memory\r\n")
        ret = conn.read_until("\r\n\r\n")
        lines = ret.split('\r\n')
        for l in lines:
            if 'used_memory:' in l:
                used_memory = int(l[len('used_memory:'):])
        self.assertNotEquals(used_memory, -1)

        return used_memory

    def test_s3_gc_interval(self):
        util.print_frame()
        server = self.cluster['servers'][0]
        redis = telnetlib.Telnet(server['ip'], server['redis_port'])

        # Wrong arguments
        wrong_args = ['50', '-1', '0', 'xxx', '2147483648']
        for arg in wrong_args:
            self.expect(redis, "config set sss-gc-interval %s\r\n" % arg
                             , "-ERR Invalid argument '%s' for CONFIG SET 'sss-gc-interval'\r\n" % arg)

        # Wrong number of arguments
        self.expect(redis, "config set sss-gc-interval \r\n"
                         , "-ERR Wrong number of arguments for CONFIG set\r\n")

        self.expect(redis, "config set sss-gc-interval arg1 arg2\r\n"
                         , "-ERR Wrong number of arguments for CONFIG set\r\n")

        # Right arguments
        right_args = ['100', '1000', '999', '500', '999999999', '2147483647']
        for arg in right_args:
            self.expect(redis, "config set sss-gc-interval %s\r\n" % arg
                             , "+OK\r\n")

            redis.write("config get sss-gc-interval\r\n")
            redis.read_until('\r\n')
            redis.read_until('\r\n')
            redis.read_until('\r\n')
            redis.read_until('\r\n')
            self.assertEquals(redis.read_until('\r\n').strip(), arg)

        ## Check memory decrease after decreasing interval
        # First, increase interval very big and make sure that triggering GC is impossible.
        self.expect(redis, "config set sss-gc-interval 100000000\r\n"
                         , "+OK\r\n")

        # Insert some bulk data.
        bulk = 'x' * (1024*4)
        for i in xrange(0, 100000):
            # TTL is set to 1ms. If backgroud GC is triggered, the value will be removed immediately.
            self.expect(redis, "s3ladd ks uuid%d svc name %s%d 1\r\n" % (i, bulk, i)
                             , ":1\r\n")

        # Save current used memory
        used_memory_before = self.get_used_memory(redis)
        util.log("Before triggering GC, used_memory = %d" % used_memory_before)

        # Decrease gc interval
        self.expect(redis, "config set sss-gc-interval 100\r\n"
                         , "+OK\r\n")

        # Wait some time
        time.sleep(60)

        # Compare used memory
        used_memory_after = self.get_used_memory(redis)
        util.log("After triggering GC, used_memory = %d" % used_memory_after)
        util.log("Subtract of before and after used_memory = %d" % (used_memory_before - used_memory_after))

        # Check if used_memory is decreased at least 40 megabytes.
        self.assertTrue(used_memory_before - used_memory_after > 40*1024*1024)
