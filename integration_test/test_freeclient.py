import unittest
import config
import redis_mgmt
import smr_mgmt
import default_cluster
import util
import subprocess
import copy
import time

class TestFreeClient(unittest.TestCase):
    cluster = config.clusters[0]
    
    def setUp(self):
        util.set_process_logfile_prefix( 'TestFreeClient_%s' % self._testMethodName )
        ret = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
        if ret is not 0:
            default_cluster.finalize( self.cluster )
        return 0

    def tearDown(self):
        if default_cluster.finalize( self.cluster ) is not 0:
            util.log('failed to TestFreeClient.finalize')
        return 0

    def numOpenFds(self, pid):
        p = util.exec_proc_async(util.cluster_util_dir(0), 'ls /proc/%s/fd | wc -l' % pid, 
                                 True, None, subprocess.PIPE, None)
        ret = p.wait()
        return p.stdout.read()[:-1]

    def test_fd_leak(self):
        util.print_frame()

        server = util.get_server_by_role_and_pg(self.cluster['servers'], 'master', 0)
        redis = redis_mgmt.Redis(server['id'])
        ret = redis.connect(server['ip'], server['redis_port'])
        self.assertEquals(ret, 0, 'failed to connect to redis')
        smr = smr_mgmt.SMR(server['id'])
        ret = smr.connect(server['ip'], server['smr_mgmt_port'])
        self.assertEquals(ret, 0, 'failed to connect to smr')

        redis.write('info server\r\n')
        res = redis.read_until('process_id:')
        res = redis.read_until('\r\n')
        redis.write('quit\r\n')

        pid = copy.copy(res[:-2])
        num1 = self.numOpenFds(pid)
        print "Initial : Open Fds: %s" % self.numOpenFds(pid)

        smr.write('fi delay sleep 1 1000000\r\n')
        smr.read_until('\r\n')

        for i in range(5):
            ret = redis.connect(server['ip'], server['redis_port'])
            self.assertEquals(ret, 0, 'failed to connect to redis')
            redis.write('ping\r\n')
            res = redis.read_until('\r\n', 1)
            print "Try Ping : Open Fds: %s" % self.numOpenFds(pid)
            redis.disconnect()
            print "Disconnect : Open Fds: %s" % self.numOpenFds(pid)

            ret = redis.connect(server['ip'], server['redis_port'])
            self.assertEquals(ret, 0, 'failed to connect to redis')
            redis.write('*1\r\nasdf\r\n')
            time.sleep(1)
            res = redis.read_until('\r\n', 1)
            print "Protocol Error : Open Fds: %s" % self.numOpenFds(pid)
            redis.disconnect()
            print "Disconnect : Open Fds: %s" % self.numOpenFds(pid)

        print "End : Open Fds: %s" % self.numOpenFds(pid)

        num2 = self.numOpenFds(pid)
        self.assertEquals(num1, num2)
