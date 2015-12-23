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

class TestGatewayMgmt(unittest.TestCase):
    cluster = config.clusters[2]
    max_load_generator = 128
    load_gen_thrd_list = {}

    @classmethod
    def setUpClass( cls ):
        return 0

    @classmethod
    def tearDownClass( cls ):
        return 0

    def setUp(self):
        util.set_process_logfile_prefix( 'TestGatewayMgmt_%s' % self._testMethodName )
        if default_cluster.initialize_starting_up_smr_before_redis( self.cluster ) is not 0:
            util.log('failed to TestScaleout.initialize')
            return -1
        return 0

    def tearDown(self):
        if default_cluster.finalize( self.cluster ) is not 0:
            util.log('failed to TestScaleout.finalize')
        return 0

    def pgs_del_server(self, gw, server, n):
        gw.write("pgs_del %d %d\r\n" % (server['id']+n*6, server['pg_id']+n*2))
        gw.read_until("+OK\r\n")

    def pgs_add_server(self, gw, server, n):
        gw.write("pgs_add %d %d %s %d\r\n" % (server['id']+n*6, server['pg_id']+n*2, server['ip'], server['redis_port']))
        gw.read_until("+OK\r\n")

    def test_moving_pgs(self):
        util.print_frame()

        # start load generator
        util.log("start load_generator")
        for i in range(self.max_load_generator):
            ip, port = util.get_rand_gateway(self.cluster)
            self.load_gen_thrd_list[i] = load_generator.LoadGenerator(i, ip, port)
            self.load_gen_thrd_list[i].start()

        util.log("started load_generator")

        servers = self.cluster['servers']

        gw_list = []
        for server in servers:
            gw = {}
            gw['mgmt'] = telnetlib.Telnet(server['ip'], server['gateway_port']+1)
            gw['normal'] = telnetlib.Telnet(server['ip'], server['gateway_port'])
            gw_list.append(gw)

        n = 0
        step = 0
        iter = 30
        while iter > 0:
            if n == 0 or random.randint(0, 1) == 0:
                step = random.randint(1, 10)
            else:
                step = -1 * random.randint(1, n)

            print "<<< ITER = %d, PG%d -> PG%d, PG%d -> PG%d >>>" % (iter, n*2, (n+step)*2, n*2+1, (n+step)*2+1)
            gw = gw_list[0]
            self.pgs_del_server(gw['mgmt'], servers[0], n)
            self.pgs_del_server(gw['mgmt'], servers[1], n)
            self.pgs_del_server(gw['mgmt'], servers[5], n)

            gw['mgmt'].write("pg_add %d\r\n" % ((n+step)*2))
            gw['mgmt'].read_until("+OK\r\n")
            gw['mgmt'].write("pg_add %d\r\n" % ((n+step)*2+1))
            gw['mgmt'].read_until("+OK\r\n")

            self.pgs_add_server(gw['mgmt'], servers[0], n+step)
            self.pgs_add_server(gw['mgmt'], servers[1], n+step)
            self.pgs_add_server(gw['mgmt'], servers[5], n+step)

            while True:
                gw['normal'].write("info gateway\r\n")
                ret = gw['normal'].read_until("\r\n", 1)
                if "-ERR" in ret:
                    continue
                ret = gw['normal'].read_until("\r\n\r\n", 1)
                #print ret
                if "gateway_disconnected_redis:0\r\n" in ret:
                    break

            gw['mgmt'].write("delay 0 4095\r\n")
            gw['mgmt'].read_until("+OK\r\n")
            gw['mgmt'].write("delay 4096 8191\r\n")
            gw['mgmt'].read_until("+OK\r\n")

            gw['mgmt'].write("redirect 0 4095 %d\r\n" % ((n+step)*2))
            gw['mgmt'].read_until("+OK\r\n")
            gw['mgmt'].write("redirect 4096 8191 %d\r\n" % ((n+step)*2+1))
            gw['mgmt'].read_until("+OK\r\n")

            gw_list[0]['mgmt'].write("cluster_info\r\nping\r\n")
            print gw_list[0]['mgmt'].read_until("+PONG\r\n")

            self.pgs_del_server(gw['mgmt'], servers[2], n)
            self.pgs_del_server(gw['mgmt'], servers[3], n)
            self.pgs_del_server(gw['mgmt'], servers[4], n)

            self.pgs_add_server(gw['mgmt'], servers[2], n+step)
            self.pgs_add_server(gw['mgmt'], servers[3], n+step)
            self.pgs_add_server(gw['mgmt'], servers[4], n+step)

            while True:
                gw['normal'].write("info gateway\r\n")
                ret = gw['normal'].read_until("\r\n", 1)
                if "-ERR" in ret:
                    continue
                ret = gw['normal'].read_until("\r\n\r\n", 1)
                #print ret
                if "gateway_disconnected_redis:0\r\n" in ret:
                    break

            gw['mgmt'].write("pg_del %d\r\n" % (n*2))
            gw['mgmt'].read_until("+OK\r\n")
            gw['mgmt'].write("pg_del %d\r\n" % (n*2+1))
            gw['mgmt'].read_until("+OK\r\n")

            n += step

            gw_list[0]['mgmt'].write("cluster_info\r\nping\r\n")
            print gw_list[0]['mgmt'].read_until("+PONG\r\n")
            iter -= 1

        # check consistency of load_generator
        for i in range(len(self.load_gen_thrd_list)):
            self.load_gen_thrd_list[i].quit()
        for i in range(len(self.load_gen_thrd_list)):
            self.load_gen_thrd_list[i].join()
            self.assertTrue(self.load_gen_thrd_list[i].isConsistent(), 'Inconsistent after gateway_mgmt test')

    def test_random_pgs_del_and_add(self):
        util.print_frame()

        # start load generator
        util.log("start load_generator")
        for i in range(self.max_load_generator):
            ip, port = util.get_rand_gateway(self.cluster)
            self.load_gen_thrd_list[i] = load_generator.LoadGenerator(i, ip, port)
            self.load_gen_thrd_list[i].start()

        util.log("started load_generator")

        servers = self.cluster['servers']
        gw_list = []
        for server in servers:
            gw = {}
            gw['mgmt'] = telnetlib.Telnet(server['ip'], server['gateway_port']+1)
            gw['normal'] = telnetlib.Telnet(server['ip'], server['gateway_port'])
            gw_list.append(gw)

        count = 10
        while count > 0:
            c = random.choice(servers)
            for gw in gw_list:
                gw['mgmt'].write("pgs_del %d %d\r\n" % (c['id'], c['pg_id']))
                gw['mgmt'].read_until("+OK\r\n")

            gw_list[0]['mgmt'].write("cluster_info\r\nping\r\n")
            print gw_list[0]['mgmt'].read_until("+PONG\r\n")

            for gw in gw_list:
                gw['mgmt'].write("pgs_add %d %d %s %d\r\n" % (c['id'], c['pg_id'], c['ip'], c['redis_port']))
                gw['mgmt'].read_until("+OK\r\n")

            for gw in gw_list:
                while True:
                    gw['normal'].write("info gateway\r\n")
                    ret = gw['normal'].read_until("\r\n\r\n")
                    if "gateway_disconnected_redis:0\r\n" in ret:
                        break

            count -= 1

        # check consistency of load_generator
        for i in range(len(self.load_gen_thrd_list)):
            self.load_gen_thrd_list[i].quit()
        for i in range(len(self.load_gen_thrd_list)):
            self.load_gen_thrd_list[i].join()
            self.assertTrue(self.load_gen_thrd_list[i].isConsistent(), 'Inconsistent after gateway_mgmt test')

    def test_info_and_dbsize_command(self):
        util.print_frame()

        servers = self.cluster['servers']

        gw_list = []
        for server in servers:
            gw = telnetlib.Telnet(server['ip'], server['gateway_port'])
            gw_list.append(gw)

        for i in range(10000):
            gw_list[0].write("set key_%d value_%d\r\n" %(i, i))
            gw_list[0].read_until("+OK\r\n")

        for i in range(1000):
            gw_list[0].write("expire key_%d 10000000\r\n" % (i))
            gw_list[0].read_until(":1\r\n")

        for gw in gw_list:
            gw.write("info all\r\n")
            gw.write("ping\r\n")
            ret = gw.read_until("+PONG\r\n")

            if "cluster_name:testCluster0" not in ret:
                util.log(ret)
                self.assertFalse(True, "Incorrect result of info commands, cluster_name")

            if "total_partition_groups:2" not in ret:
                util.log(ret)
                self.assertFalse(True, "Incorrect result of info commands, partition_groups")

            if "partition_groups_available:2" not in ret:
                util.log(ret)
                self.assertFalse(True, "Incorrect result of info commands, partition_groups")

            if "total_redis_instances:6" not in ret:
                util.log(ret)
                self.assertFalse(True, "Incorrect result of info commands, redis_instances_reachable")

            if "redis_instances_available:6" not in ret:
                util.log(ret)
                self.assertFalse(True, "Incorrect result of info commands, redis_instances_unreachable")

            if "gateway_disconnected_redis:0" not in ret:
                util.log(ret)
                self.assertFalse(True, "Incorrect result of info commands, inactive_connections")

            if "db0:keys=10000,expires=1000,avg_ttl=9999" not in ret:
                util.log(ret)
                self.assertFalse(True, "Incorrect result of info commands, keys")

    def test_redis_hang(self):
        util.print_frame()

        server = self.cluster['servers'][0]
        gw = telnetlib.Telnet(server['ip'], server['gateway_port'])
        redis = telnetlib.Telnet(server['ip'], server['redis_port'])

        redis.write("debug sleep 1000\r\n")

        gw.write("dbsize\r\n")
        gw.read_until("\r\n")

        ts = time.time()
        while (time.time() - ts < 6):
            gw.write("dbsize\r\n")
            time.sleep(0.1)

        gw.write("ping\r\n")
        gw.read_until("+PONG\r\n")

        gw.write("info cluster\r\n")
        ret = gw.read_until("\r\n\r\n", 3)
        util.log(ret)

        if "redis_instances_available:5" not in ret:
            self.assertFalse(True, "Disconnection of timed-out redis is not processed in gateway")
