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

import telnetlib
import util
import time
import os
import unittest
import config
import default_cluster
import subprocess
import string
import random
import testbase

class TestClusterUtil(unittest.TestCase):
    cluster = config.clusters[0]

    getdump_proxy_port = 8105
    getdump_proxy_log = "getdump.log"
    getdump_proxy_proc = None

    playdump_proxy_port = 8106
    playdump_proxy_log = "playdump.log"
    playdump_proxy_proc = None

    def setUp(self):
        util.set_process_logfile_prefix('TestClusterUtil_%s' % self._testMethodName)
        self.conf_checker = default_cluster.initialize_starting_up_smr_before_redis(self.cluster)
        self.assertIsNotNone(self.conf_checker, 'failed to initialize cluster')

        # Setup proxy server
        self.getdump_proxy_proc = self.setup_proxy_server(self.getdump_proxy_port, self.cluster['servers'][0]['redis_port'],
                                                          "/dev/null", self.getdump_proxy_log, "getdump.fifo")
        self.playdump_proxy_proc = self.setup_proxy_server(self.playdump_proxy_port, self.cluster['servers'][0]['redis_port'],
                                                           self.playdump_proxy_log, "/dev/null", "playdump.fifo")
        return 0

    def tearDown(self):
        # Release proxy server
        self.release_proxy_server(self.getdump_proxy_proc, "getdump.fifo")
        self.release_proxy_server(self.playdump_proxy_proc, "playdump.fifo")

        testbase.defaultTearDown(self)

    def insertLargeKey(self, redis, key):
        # Make random values
        vals = []
        for i in xrange(0, 1000):
            val = ''.join(random.choice(string.ascii_letters) for x in range(1000))
            val_str = "$%d\r\n%s\r\n" % (len(val), val)
            vals.append(val_str)

        key_str = "$%d\r\n%s\r\n" % (len(key), key)
        for i in xrange(0, 100000):
            hkey_str = "$%d\r\n%s\r\n" % (len(str(i)), str(i))
            hval_str = vals[random.randint(0, len(vals)-1)]

            ret = redis.write("*4\r\n$4\r\nHSET\r\n")
            redis.write(key_str)
            redis.write(hkey_str)
            redis.write(hval_str)

            # Read response every 100 commands for pipelining
            if (i+1) % 100 == 0:
                redis.write("PING\r\n")
                ret = redis.read_until("+PONG\r\n", 30)
                self.assertTrue("+PONG" in ret)

    def setup_proxy_server(self, in_port, out_port, in_log, out_log, fifo_name):
        try:
            os.remove("%s/%s" % (util.cluster_util_dir(0), fifo_name))
        except Exception as e:
            pass
        fifo_cmd = "mkfifo %s" % fifo_name
        fifo_proc = util.exec_proc_async(util.cluster_util_dir(0), fifo_cmd, True, None, subprocess.PIPE, None)
        ret = fifo_proc.wait()
        self.assertEqual(0, ret)

        proxy_cmd = "nc -l %d < %s | tee %s | nc 127.0.0.1 %d | tee %s > %s" % (
            in_port, fifo_name, in_log, out_port, out_log, fifo_name)
        return  util.exec_proc_async(util.cluster_util_dir(0), proxy_cmd, True, None, subprocess.PIPE, None)

    def release_proxy_server(self, proc, fifo_name):
        proc.terminate()
        try:
            os.remove("%s/%s" % (util.cluster_util_dir(0), fifo_name))
        except Exception as e:
            pass

    def sum_of_filesize(self, *paths):
        sum = 0
        for path in paths:
            sum += os.path.getsize(path)
        return sum

    def monitor_filesize_diff(self, poll_proc, limit_mb, *paths):
        first = True
        while True:
            last = self.sum_of_filesize(*paths)
            if first != True:
                mb = (last - prev) / 1024 / 1024
                util.log("Network usage: %dMB/s, Limit: %dMB/s" % (mb, limit_mb))
                self.assertTrue(mb <= limit_mb*1.3)
            first = False
            prev = last

            # Check termination of process generating workloads
            if poll_proc.poll() != None:
                break;
            time.sleep(1)

    def test_getdump_and_playdump(self):
        util.print_frame()

        test_limit_mb = 10

        server = self.cluster['servers'][0]
        redis = telnetlib.Telnet(server['ip'], server['redis_port'])

        util.log("Insert large key about 100MB")
        self.insertLargeKey(redis, "test_key")

        # Test getdump
        start_time = time.time()
        util.log("Start getdump, start ts:%d" % start_time)

        cmd = "./cluster-util --getdump %s %d getdump.rdb 0-8191 %d" % (server['ip'], self.getdump_proxy_port, test_limit_mb)
        proc = util.exec_proc_async(util.cluster_util_dir(0), cmd, True, None, subprocess.PIPE, None)

        monitor_file = "%s/%s" % (util.cluster_util_dir(0), self.getdump_proxy_log)
        self.monitor_filesize_diff(proc, test_limit_mb, monitor_file)

        ret = proc.wait()
        self.assertEqual(0, ret)

        elapse_time = time.time() - start_time
        util.log("End getdump, elapsed:%d" % elapse_time)

        file_size = os.path.getsize("%s/%s" % (util.cluster_util_dir(0), self.getdump_proxy_log))
        util.log("File Size:%d, elapsed:%d, limit:%dMB/s, actual:%dMB/s" % (file_size,
                 elapse_time, test_limit_mb, file_size / elapse_time / 1024 / 1024))
        self.assertTrue(file_size / (10 * 1024 * 1024) < elapse_time)

        # Test playdump
        start_time = time.time()
        util.log("Start playdump, start ts:%d" % start_time)

        cmd = "./cluster-util --playdump getdump.rdb %s %d 30000 %d" % (server['ip'], self.playdump_proxy_port, test_limit_mb)
        proc = util.exec_proc_async(util.cluster_util_dir(0), cmd, True, None, subprocess.PIPE, None)

        monitor_file = "%s/%s" % (util.cluster_util_dir(0), self.playdump_proxy_log)
        self.monitor_filesize_diff(proc, test_limit_mb, monitor_file)

        ret = proc.wait()
        self.assertEqual(0, ret)

        elapse_time = time.time() - start_time
        util.log("End playdump, elapsed:%d" % elapse_time)

        file_size = os.path.getsize("%s/%s" % (util.cluster_util_dir(0), self.playdump_proxy_log))
        util.log("File Size:%d, elapsed:%d, limit:%dMB/s, actual:%dMB/s" % (file_size,
                 elapse_time, test_limit_mb, file_size / elapse_time / 1024 / 1024))
        self.assertTrue(file_size / (10 * 1024 * 1024) < elapse_time)

    def test_getandplay(self):
        util.print_frame()

        test_limit_mb = 10

        server = self.cluster['servers'][0]
        redis = telnetlib.Telnet(server['ip'], server['redis_port'])

        util.log("Insert large key about 100MB")
        self.insertLargeKey(redis, "test_key")

        # Test getandplay
        start_time = time.time()
        util.log("Start getandplay, start ts:%d" % start_time)

        cmd = "./cluster-util --getandplay %s %d %s %d 0-8191 30000 %d" % (server['ip'], self.getdump_proxy_port,
                                                                        server['ip'], self.playdump_proxy_port, test_limit_mb)
        proc = util.exec_proc_async(util.cluster_util_dir(0), cmd, True, None, subprocess.PIPE, None)

        monitor_file1 = "%s/%s" % (util.cluster_util_dir(0), self.getdump_proxy_log)
        monitor_file2 = "%s/%s" % (util.cluster_util_dir(0), self.playdump_proxy_log)
        self.monitor_filesize_diff(proc, test_limit_mb, monitor_file1, monitor_file2)

        ret = proc.wait()
        self.assertEqual(0, ret)

        elapse_time = time.time() - start_time
        util.log("End getandplay, elapsed:%d" % elapse_time)

        dump_file_size = os.path.getsize("%s/%s" % (util.cluster_util_dir(0), self.getdump_proxy_log))
        play_file_size = os.path.getsize("%s/%s" % (util.cluster_util_dir(0), self.playdump_proxy_log))
        util.log("Dump File Size:%d, Play File Size:%d, elapsed:%d, limit:%dMB/s, actual:%dMB/s"
                 % (dump_file_size, play_file_size, elapse_time, test_limit_mb,
                    (dump_file_size + play_file_size) / elapse_time / 1024 / 1024))
        self.assertTrue((dump_file_size + play_file_size) / (10 * 1024 * 1024) < elapse_time)
