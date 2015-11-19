import subprocess
import unittest
import test_base
import default_cluster
import util
import os
import constant
import config
import time
import telnetlib
import signal

class TestBasicOp(unittest.TestCase):
    cluster = config.clusters[2]

    @classmethod
    def setUpClass(cls):
        return 0
       
    @classmethod
    def tearDownClass(cls):
        return 0

    def setUp(self):
        util.set_remote_process_logfile_prefix( self.cluster, 'TestBasicOp_%s' % self._testMethodName )
        ret = default_cluster.initialize_starting_up_smr_before_redis(self.cluster)
        if ret is not 0:
            util.log('failed to test_basic_op.initialize')
            default_cluster.finalize(self.cluster)
        self.assertEquals( ret, 0, 'failed to test_basic_op.initialize' )

    def tearDown(self):
        if default_cluster.finalize(self.cluster) is not 0:
            util.log('failed to test_basic_op.finalize')

    def test_basic_op(self):
        util.print_frame()
        f = open("%s/test_basicop_output_redis" % constant.logdir, 'w')
        p = util.exec_proc_async("../redis-2.8.8", 
                            "./runtest --accurate",
                            True, None, f, None)

        ret = p.wait()
        f.close()
        self.assertEquals(0, ret)

    def test_basic_op_smr(self):
        util.print_frame()
        f = open("%s/test_basicop_output_smr" % constant.logdir, 'w')
        p = util.exec_proc_async("../redis-2.8.8", 
                            "./runtest_smr --accurate",
                            True, None, f, None)

        ret = p.wait()
        f.close()
        self.assertEquals(0, ret)

    def test_basic_op_gateway(self):
        util.print_frame()
        ip, port = util.get_rand_gateway(self.cluster)
        f = open("%s/test_basicop_output_gw" % constant.logdir, 'w')
        p = util.exec_proc_async("../redis-2.8.8", 
                            "./runtest_gw --accurate --gw-port "+str(port),
                            True, None, f, None)

        ret = p.wait()
        f.close()
        self.assertEquals(0, ret)

    def run_capi_server(self, arch=64):
        # run capi test server
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
            cmd = "./capi-server32 capi_server.conf"
        else:
            cmd = "./capi-server capi_server.conf"

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

        return capi_server

    def stop_process(self, capi_server):
        capi_server.send_signal(signal.SIGTERM)
        capi_server.wait()

    def __test_basic_op_capi(self, arch = 64):

        capi_server = self.run_capi_server(arch)

        f = open("%s/test_basicop_output_capi%d" % (constant.logdir, arch), 'w')
        p = util.exec_proc_async("../redis-2.8.8", 
                            "./runtest_gw --accurate --gw-port 6200",
                            True, None, f, None)

        ret = p.wait()
        f.close()
        self.assertEquals(0, ret)

        self.stop_process(capi_server)

    def test_basic_op_capi64(self):
        util.print_frame()
        self.__test_basic_op_capi(64)
    
    def test_basic_op_capi32(self):
        util.print_frame()
        self.__test_basic_op_capi(32)
