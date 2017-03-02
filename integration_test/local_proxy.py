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
import unittest
import util
import load_generator
import default_cluster
import config
import os
import time
import telnetlib
import signal
import constant
import testbase

class TestLocalProxy(unittest.TestCase):
    cluster = config.clusters[2]
    max_load_generator = 128

    @classmethod
    def setUpClass(cls):
        return 0

    @classmethod
    def tearDownClass(cls):
        return 0

    def setUp(self):
        util.set_process_logfile_prefix('TestLocalProxy_%s' % self._testMethodName)
        self.conf_checker = default_cluster.initialize_starting_up_smr_before_redis(self.cluster, arch=self.arch)
        self.assertIsNotNone(self.conf_checker, 'failed to initialize cluster')

    def tearDown(self):
        testbase.defaultTearDown(self)

    def run_test_server(self):
        # run test server
        _capi_server_conf = """
zookeeper 127.0.0.1:2181
cluster_name %s
port 6200
daemonize no
num_conn_per_gw 2
init_timeout_millis 10000
log_level INFO
log_file_prefix "capi_server"
max_fd 4096
conn_reconnect_millis 1000
zk_reconnect_millis 1000
zk_session_timeout_millis 10000
local_proxy_query_timeout_millis 1000
        """ % self.cluster['cluster_name']
        old_cwd = os.path.abspath( os.getcwd() )
        os.chdir(util.capi_dir(0))
        f = open('capi_server.conf', 'w')
        f.write(_capi_server_conf)
        f.close()
        os.chdir(old_cwd)

        if self.arch is 32:
            cmd = "./%s capi_server.conf" % constant.CAPI32_TEST_SERVER
        else:
            cmd = "./%s capi_server.conf" % constant.CAPI_TEST_SERVER

        capi_server = util.exec_proc_async(util.capi_dir(0),
                            cmd, True, None, subprocess.PIPE, None)

        return capi_server

    def stop_test_server(self, capi_server):
        # Terminate test server
        capi_server.send_signal(signal.SIGTERM)
        capi_server.wait()

    def test_local_proxy(self):
        util.print_frame()

        # Clean server log file
        p = util.exec_proc_async(util.capi_dir(0),
                'rm capi_server-*',
                True, None, subprocess.PIPE, None)

        p.wait()

        # run test server
        capi_server = self.run_test_server()

        # ping check
        while True:
            try:
                t = telnetlib.Telnet('127.0.0.1', 6200)
                break
            except:
                time.sleep(1)
                continue

        t.write("ping\r\n")
        t.read_until('+PONG\r\n')
        t.close()

        # Start load generator
        load_gen_thrd_list = {}
        for i in range(self.max_load_generator):
            load_gen_thrd_list[i] = load_generator.LoadGenerator(i, 'localhost', 6200)
            load_gen_thrd_list[i].start()

        time.sleep(5)

        # Check reconfiguration by SIGHUP
        p = util.exec_proc_async(util.capi_dir(0),
                'grep "Connected to the zookeeper" capi_server-* | wc -l',
                True, None, subprocess.PIPE, None)

        p.wait()
        wc = p.stdout.readline()
        print 'grep "Connected to the zookeeper" result : ' + wc
        self.assertEquals(wc.strip(), '1')

        capi_server.send_signal(signal.SIGHUP)
        time.sleep(5)

        p = util.exec_proc_async(util.capi_dir(0),
                'grep "Connected to the zookeeper" capi_server-* | wc -l',
                True, None, subprocess.PIPE, None)

        p.wait()
        wc = p.stdout.readline()
        print 'grep "Connected to the zookeeper" result : ' + wc
        self.assertEquals(wc.strip(), '2')

        p = util.exec_proc_async(util.capi_dir(0),
                'grep "Graceful shutdown caused by API" capi_server-* | wc -l',
                True, None, subprocess.PIPE, None)

        p.wait()
        wc = p.stdout.readline()
        print 'grep "Graceful shutdown caused by API" result : ' + wc
        self.assertEquals(wc.strip(), '1')

        # Check consistency after sending many SIGHUP signal
        for i in range(50):
            capi_server.send_signal(signal.SIGHUP)
            time.sleep(0.1)

        # check consistency of load_generator
        for i in range(len(load_gen_thrd_list)):
            load_gen_thrd_list[i].quit()
        for i in range(len(load_gen_thrd_list)):
            load_gen_thrd_list[i].join()
            self.assertTrue(load_gen_thrd_list[i].isConsistent(), 'Inconsistent after sending signal')

        # Terminate test server
        self.stop_test_server(capi_server)

    def test_local_proxy_be_timeout(self):
        util.print_frame()

        # run test server
        capi_server = self.run_test_server()

        # ping check
        while True:
            try:
                t = telnetlib.Telnet('127.0.0.1', 6200)
                break
            except:
                time.sleep(1)
                continue

        server = self.cluster['servers'][0]
        redis = telnetlib.Telnet(server['ip'], server['redis_port'])
        redis.write('debug sleep 5\r\n')

        startTime = time.time()
        t.write("INFO\r\nPING\r\n")
        ret = t.read_until('\r\n', 10)
        endTime = time.time()

        # Terminate test server
        self.stop_test_server(capi_server)

        self.assertTrue('-ERR Redis Timeout' in ret)
        self.assertTrue(endTime-startTime < 3)
