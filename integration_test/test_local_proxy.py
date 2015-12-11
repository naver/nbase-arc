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
        ret = default_cluster.initialize_starting_up_smr_before_redis(self.cluster)
        if ret is not 0:
            util.log('failed to test_local_proxy.initialize')
            default_cluster.finalize(self.cluster)

    def tearDown(self):
        if default_cluster.finalize(self.cluster) is not 0:
            util.log('failed to test_local_proxy.finalize')

    def test_local_proxy64(self):
        util.print_frame()
        self.__test_local_proxy(64)

    def test_local_proxy32(self):
        util.print_frame()
        self.__test_local_proxy(32)

    def __test_local_proxy(self, arch=64):
        util.print_frame()

        # Clean server log file
        p = util.exec_proc_async(util.capi_dir(0), 
                'rm capi_server-*', 
                True, None, subprocess.PIPE, None)

        p.wait()

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
        """ % self.cluster['cluster_name']
        old_cwd = os.path.abspath( os.getcwd() )
        os.chdir(util.capi_dir(0))
        f = open('capi_server.conf', 'w')
        f.write(_capi_server_conf)
        f.close()
        os.chdir(old_cwd)

        if arch is 32:
            cmd = "./%s capi_server.conf" % constant.CAPI32_TEST_SERVER
        else:
            cmd = "./%s capi_server.conf" % constant.CAPI_TEST_SERVER

        capi_server = util.exec_proc_async(util.capi_dir(0), 
                            cmd, True, None, subprocess.PIPE, None)

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
        capi_server.send_signal(signal.SIGTERM)
        capi_server.wait()
