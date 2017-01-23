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
import gateway_mgmt
import config
import default_cluster
import json
import random
import threading
import telnetlib
import constant
import time
import sys
import load_generator
import functools
import zookeeper
from arcci.arcci import *
from ctypes import *
from operator import *

cluster_2_name = 'test_lock'
cluster_2_pg_count = 4
cluster_2_pgs_per_pg = 2

class CresteClusterThread(threading.Thread):
    def __init__(self, cluster_name, ip, pm_name, pg_max, pgs_per_pg, leader_cm):
        threading.Thread.__init__(self)
        self.cluster_name = cluster_name
        self.ip = ip
        self.pm_name = pm_name
        self.leader_cm = leader_cm
        self.pg_max = pg_max
        self.pgs_per_pg = pgs_per_pg
        self.result = False
        self.cluster = None
        pass

    def run(self):
        # Make cluster configuration
        pgs_id = 10
        cluster = {
                'cluster_name' : self.cluster_name,
                'keyspace_size' : 8192,
                'quorum_policy' : '0:1',
                'slots' : [],
                'pg_id_list' : [],
                'servers' : []
                }

        for pg_id in range(self.pg_max):
            cluster['pg_id_list'].append(pg_id)
            cluster['slots'].append(8192 / self.pg_max * pg_id)
            if pg_id == self.pg_max - 1:
                cluster['slots'].append(8191)
            else:
                cluster['slots'].append(8192 / self.pg_max * (pg_id + 1) - 1)

            for pgs in range(self.pgs_per_pg):
                smr_base_port = 15000 + pgs_id * 20
                smr_mgmt_port = smr_base_port + 3
                gateway_port = smr_base_port + 10
                redis_port = smr_base_port + 9

                server = {}
                server['id'] = pgs_id
                pgs_id = pgs_id + 1
                server['cluster_name'] = self.cluster_name
                server['ip'] = self.ip
                server['pm_name'] = self.pm_name
                server['cm_port'] = None
                server['pg_id'] = pg_id
                server['smr_base_port'] = smr_base_port
                server['smr_mgmt_port'] = smr_mgmt_port
                server['gateway_port'] = gateway_port
                server['redis_port'] = redis_port
                server['zk_port'] = 2181

                cluster['servers'].append(server)

        # Send cluster information to MGMT-CC
        testbase.initialize_cluster(cluster, self.leader_cm)

        # Set up pgs binaries
        try:
            for server in cluster['servers']:
                id = server['id']
                util.log('copy binaries, server_id=%d' % id)

                util.copy_smrreplicator(id)
                util.copy_gw(id)
                util.copy_redis_server(id)
                util.copy_cluster_util(id)

        except IOError as e:
            util.log(e)
            util.log('Error: can not find file or read data')
            return

        except:
            util.log('Error: file copy error.')
            util.log(sys.exc_info()[0])
            raise

        # Cleanup servers`s directories
        for server in cluster['servers']:
            if testbase.cleanup_pgs_log_and_ckpt(cluster['cluster_name'], server) != 0:
                util.log('failed to cleanup_test_environment, id=%d' % server['id'])
                return

        # Start pgs
        for server in cluster['servers']:
            if testbase.request_to_start_smr(server) != 0:
                util.log('failed to request_to_start_smr, id=%d' % server['id'])
                return

        for server in cluster['servers']:
            if testbase.request_to_start_redis(server, check=False) != 0:
                util.log('failed to request_to_start_redis, id=%d' % server['id'])
                return

        for server in cluster['servers']:
            if testbase.wait_until_finished_to_set_up_role(server) != 0:
                util.log('failed to role set up, id=%d' % server['id'])
                return

        for i in range(self.pg_max):
            server = cluster['servers'][i]
            if testbase.request_to_start_gateway(cluster['cluster_name'], server, self.leader_cm) != 0:
                util.log('failed to request_to_start_gateway, id=%d' % server['id'])
                return

        if util.check_cluster(self.cluster_name, self.leader_cm['ip'], self.leader_cm['cm_port']):
            self.result = True
        else:
            self.result = False

        self.cluster = cluster

    def get_result(self):
        return self.result

    def get_cluster(self):
        return self.cluster

class CommandThread(threading.Thread):
    """
        method : 'random' or 'sequencial'
    """
    def __init__(self, leader_cm, commands, method):
        threading.Thread.__init__(self)
        self.leader_cm = leader_cm
        self.commands = commands
        self.method = method

        self.term = False
        self.result = False
        self.error_msg = 'No error message'
        self.error_cmd = 'No error command'
        self.total_rqst = 0
        self.total_resp = 0

        self.cm_conn = telnetlib.Telnet(leader_cm['ip'], leader_cm['cm_port'])

    def quit(self):
        self.term = True

    def run(self):
        self.result = True
        sequence = 0
        while True:
            if self.term:
                if self.method == 'random' or self.method == 'sequencial':
                    break
                elif self.method == 'round' and sequence == 0:
                    break

            if self.method == 'random':
                cmd = random.choice(self.commands)
            elif self.method == 'sequencial' or self.method == 'round':
                cmd = self.commands[sequence]
                sequence += 1
                if sequence >= len(self.commands):
                    sequence = 0

            self.total_rqst += 1
            self.cm_conn.write(cmd + '\r\n')

            try:
                reply = self.cm_conn.read_until('\r\n', 3)
                jobj = json.loads(reply)
                if jobj['state'] != "success":
                    self.set_error(cmd, reply)
                else:
                    self.total_resp += 1
            except:
                self.set_error(cmd, sys.exc_info()[0])
        pass

    def set_error(self, cmd, msg):
        self.result = False
        self.error_cmd = cmd
        self.error_msg = msg
        util.log('command error, cmd:"%s", error:"%s"' % (self.error_cmd, self.error_msg))

    def get_result(self):
        return self.result

    def get_error_cmd(self):
        return self.error_cmd

    def get_error_msg(self):
        return self.error_msg

    def get_total_rqst(self):
        return self.total_rqst

    def get_total_resp(self):
        return self.total_resp

class RestartSMRThread(threading.Thread):
    def __init__(self, cluster, leader_cm):
        threading.Thread.__init__(self)
        self.cluster = cluster
        self.leader_cm = leader_cm
        self.result = False
        pass

    def run(self):
        servers = []
        servers += self.cluster['servers']

        self.result = True

        while len(servers) > 0:
            server = random.choice(servers)
            servers.remove(server)

            active_role = util.num_to_role(util.get_role_of_server(server))
            if util.failover(server, self.leader_cm) == False:
                self.result = False
        pass

    def get_result(self):
        return self.result

class TestConfMaster(unittest.TestCase):
    cluster = config.clusters[0]
    leader_cm = config.clusters[0]['servers'][0]

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        util.set_process_logfile_prefix('TestConfMaster_%s' % self._testMethodName)
        pass

    def tearDown(self):
        pass

    def create_load_gens(self, cluster, leader_cm, optype):
        load_gens = []
        ZK_ADDR = "%s:%d" % (leader_cm['ip'], leader_cm['zk_port'])
        i = 0
        while i < 3:
            i += 1
            log_prefix = 'bin/log/arcci_log_%d' % i

            util.log('create arcci')
            arcci = ARC_API(ZK_ADDR, cluster['cluster_name'], logFilePrefix = log_prefix,
                    so_path = constant.ARCCI_SO_PATH)
            load_gen = load_generator.LoadGenerator_ARCCI_Affinity(arcci, optype)
            load_gen.start()
            load_gens.append(load_gen)

        return load_gens

    def destroy_load_gens(self, load_gens):
        for load_gen in load_gens:
            load_gen.quit()

        for load_gen in load_gens:
            load_gen.join()


    def test_configuration_master_commands(self):
        util.print_frame()
        self.configuration_master_commands()

    def configuration_master_commands(self):
        util.print_frame()

        ret = default_cluster.initialize_for_test_confmaster(self.cluster)
        if ret is not 0:
            default_cluster.finalize(self.cluster)

        try:
            # Cluster commands
            cmd = 'cluster_add %s %s' % (self.cluster['cluster_name'] , self.cluster['quorum_policy'])
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            cmd = 'cluster_ls'
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))
            self.assertEquals(jobj['data']['list'][0], self.cluster['cluster_name'], 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            cmd = 'cluster_info %s' % (self.cluster['cluster_name'])
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            # Physical machine commands
            cmd = 'pm_add %s %s' % (self.leader_cm['pm_name'], self.leader_cm['ip'])
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            cmd = 'pm_ls'
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))
            self.assertEquals(jobj['data']['list'][0], self.leader_cm['pm_name'], 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            cmd = 'pm_info %s' % (self.leader_cm['pm_name'])
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            # Partition group commands
            cmd = 'pg_add %s %d' % (self.cluster['cluster_name'], self.leader_cm['id'])
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            cmd = 'slot_set_pg %s %d:%d %d' % (    self.cluster['cluster_name']
                                                                                    , self.cluster['slots'][0]
                                                                                    , self.cluster['slots'][0+1]
                                                                                    , self.cluster['pg_id_list'][0])
            cmd = 'pg_ls %s' % (self.cluster['cluster_name'])
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))
            self.assertEquals(jobj['data']['list'][0], '0', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            cmd = 'pg_info %s %d' % (self.cluster['cluster_name'], self.leader_cm['id'])
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            # Partition group server commands
            for i in range(5):
                self.commands_about_partition_group_server()

            # Gateway commands
            for i in range(5):
                self.commands_about_gateway()

            # Ping command
            cmd = 'ping'
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))
            self.assertEquals(jobj['msg'], '+PONG', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            # Help command
            cmd = 'help'
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd, 5)
            self.assertTrue(res.find('Type help <command> for command specific information') is not -1,
                                             'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            # Del partition group
            cmd = 'pg_del %s %d' % (self.cluster['cluster_name'], self.leader_cm['id'])
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            # Del physical machine
            cmd = 'pm_del %s' % (self.leader_cm['pm_name'])
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            # Del cluster
            cmd = 'cluster_del %s' % (self.cluster['cluster_name'])
            res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
            jobj = json.loads(res)
            self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

            # Leader election of Configuration master
            util.log('Leader election test')
            leader = util.get_cm_by_role(self.cluster['servers'], constant.CC_LEADER)
            self.assertNotEquals(None, leader, 'failed to get_cm_by_role(leader)')
            follower1 = util.get_cm_by_role(self.cluster['servers'], constant.CC_FOLLOWER)
            self.assertNotEquals(None, follower1, 'failed to get_cm_by_role(follower)')

            for server in self.cluster['servers']:
                if server['id'] != leader['id'] and server['id'] != follower1['id']:
                    follower2 = server
                    break
            self.assertNotEquals(None, follower2, 'failed to get follower2')

            ret = testbase.request_to_shutdown_cm(leader)
            self.assertEquals(0, ret, 'failed to request_to_shutdown_cm. server:%d' % leader['id'])
            time.sleep(5)

            there_is_a_leader = False
            followers = [follower1, follower2]
            for i in range(0, 20):
                follower = followers[i%2]

                cmd = 'not_exist_cmd'
                res = util.cm_command(follower['ip'], follower['cm_port'], cmd)
                jobj = json.loads(res)
                if jobj['state'] == 'error':
                    there_is_a_leader = True
                    break
                time.sleep(1)

            self.assertTrue(there_is_a_leader, 'failed to get a new leader')

        finally:
            default_cluster.finalize(self.cluster)

    def commands_about_partition_group_server(self):
        cmd = 'pgs_add %s %d %d %s %s %d %d' % (    self.cluster['cluster_name']
                                                                                         , self.leader_cm['id']
                                                                                         , self.leader_cm['pg_id']
                                                                                         , self.leader_cm['pm_name']
                                                                                         , self.leader_cm['ip']
                                                                                         , self.leader_cm['smr_base_port']
                                                                                         , self.leader_cm['redis_port'])
        res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
        jobj = json.loads(res)
        self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))
        util.log('success : cmd:%s\r\nres:%s' % (cmd, res))

        cmd = 'pgs_ls %s' % (self.cluster['cluster_name'])
        res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
        jobj = json.loads(res)
        self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))
        self.assertEquals(jobj['data']['list'][0], '0', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

        cmd = 'pgs_info %s %d' % (self.cluster['cluster_name'], self.leader_cm['id'])
        res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
        jobj = json.loads(res)
        self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

        cmd = 'pgs_del %s %d' % (self.cluster['cluster_name'], self.leader_cm['id'])
        res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
        jobj = json.loads(res)
        self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))
        util.log('success : cmd:%s\r\nres:%s' % (cmd, res))

    def commands_about_gateway(self):
        cmd = 'gw_add %s %d %s %s %d' % (    self.cluster['cluster_name']
                                                                                 , self.leader_cm['id']
                                                                                 , self.leader_cm['pm_name']
                                                                                 , self.leader_cm['ip']
                                                                                 , self.leader_cm['gateway_port'])
        res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
        jobj = json.loads(res)
        self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))
        util.log('success : cmd:%s\r\nres:%s' % (cmd, res))

        cmd = 'gw_ls %s' % (self.cluster['cluster_name'])
        res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
        jobj = json.loads(res)
        self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))
        self.assertEquals(jobj['data']['list'][0], '0', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

        cmd = 'gw_info %s %d' % (self.cluster['cluster_name'], self.leader_cm['id'])
        res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
        jobj = json.loads(res)
        self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))

        cmd = 'gw_del %s %d' % (self.cluster['cluster_name'], self.leader_cm['id'])
        res = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
        jobj = json.loads(res)
        self.assertEquals(jobj['state'], 'success', 'failed to execute the cmd:%s\r\nres:%s' % (cmd, res))
        util.log('success : cmd:%s\r\nres:%s' % (cmd, res))

    def test_check_deadlock(self):
        util.print_frame()
        self.check_deadlock()


    """
    Goal : Check deadlock.
    It keeps request read-operations and write-operations to MGMT-CC and checks results,
    while creating a clsuter and restarting PGS.
    """
    def check_deadlock(self):
        util.print_frame()

        for i in range(1):
            #print '\n\n==========================================================Loop %d\n\n' % i

            try:
                # Start default cluster
                ret = default_cluster.initialize_starting_up_smr_before_redis(self.cluster)
                self.assertEquals(ret, 0, 'failed to TestConfMaster.initialize')

                # Start ReadCommandThread
                read_cmds = []
                read_cmds.append('cluster_ls')
                read_cmds.append('cluster_info %s' % self.cluster['cluster_name'])

                read_cmds.append('pm_ls')
                for server in self.cluster['servers']:
                    read_cmds.append('pm_info %s' % server['pm_name'])

                read_cmds.append('pg_ls %s' % self.cluster['cluster_name'])
                for pg_id in self.cluster['pg_id_list']:
                    read_cmds.append('pg_info %s %d' % (self.cluster['cluster_name'], pg_id))

                read_cmds.append('pgs_ls %s' % self.cluster['cluster_name'])
                for server in self.cluster['servers']:
                    read_cmds.append('pgs_info %s %d' % (self.cluster['cluster_name'], server['id']))

                read_cmds.append('gw_ls %s' % self.cluster['cluster_name'])
                for server in self.cluster['servers']:
                    read_cmds.append('gw_info %s %d' % (self.cluster['cluster_name'], server['id']))

                read_cmd_thrd = CommandThread(self.leader_cm, read_cmds, 'random')
                read_cmd_thrd.start()

                # Start WriteCommanndThread 1
                write_cmds_1 = []
                write_cmds_1.append('cluster_add lock_test_write_thread 0:1')
                write_cmds_1.append('pg_add lock_test_write_thread 0')
                write_cmds_1.append('pg_add lock_test_write_thread 1')
                write_cmds_1.append('pg_add lock_test_write_thread 2')
                write_cmds_1.append('pg_add lock_test_write_thread 3')
                write_cmds_1.append('slot_set_pg lock_test_write_thread 0:2047 0')
                write_cmds_1.append('slot_set_pg lock_test_write_thread 2048:4095 1')
                write_cmds_1.append('slot_set_pg lock_test_write_thread 4095:6143 2')
                write_cmds_1.append('slot_set_pg lock_test_write_thread 6143:8191 3')
                write_cmds_1.append('pgs_add lock_test_write_thread 0 0 localhost 127.0.0.1 7000 7009')
                write_cmds_1.append('pgs_add lock_test_write_thread 1 0 localhost 127.0.0.1 7010 7019')
                write_cmds_1.append('pgs_add lock_test_write_thread 10 1 localhost 127.0.0.1 7020 7029')
                write_cmds_1.append('pgs_add lock_test_write_thread 11 1 localhost 127.0.0.1 7030 7039')
                write_cmds_1.append('pgs_add lock_test_write_thread 20 2 localhost 127.0.0.1 7040 7049')
                write_cmds_1.append('pgs_add lock_test_write_thread 21 2 localhost 127.0.0.1 7050 7059')
                write_cmds_1.append('pgs_add lock_test_write_thread 30 3 localhost 127.0.0.1 7060 7069')
                write_cmds_1.append('pgs_add lock_test_write_thread 31 3 localhost 127.0.0.1 7070 7079')
                write_cmds_1.append('gw_add lock_test_write_thread 1 localhost 127.0.0.1 6000')
                write_cmds_1.append('gw_add lock_test_write_thread 2 localhost 127.0.0.1 6010')

                write_cmds_1.append('gw_del lock_test_write_thread 1')
                write_cmds_1.append('gw_del lock_test_write_thread 2')
                write_cmds_1.append('pgs_del lock_test_write_thread 0')
                write_cmds_1.append('pgs_del lock_test_write_thread 1')
                write_cmds_1.append('pgs_del lock_test_write_thread 10')
                write_cmds_1.append('pgs_del lock_test_write_thread 11')
                write_cmds_1.append('pgs_del lock_test_write_thread 20')
                write_cmds_1.append('pgs_del lock_test_write_thread 21')
                write_cmds_1.append('pgs_del lock_test_write_thread 30')
                write_cmds_1.append('pgs_del lock_test_write_thread 31')
                write_cmds_1.append('pg_del lock_test_write_thread 0')
                write_cmds_1.append('pg_del lock_test_write_thread 1')
                write_cmds_1.append('pg_del lock_test_write_thread 2')
                write_cmds_1.append('pg_del lock_test_write_thread 3')
                write_cmds_1.append('cluster_del lock_test_write_thread')
                write_cmd_thrd = CommandThread(self.leader_cm, write_cmds_1, 'sequencial')
                write_cmd_thrd.start()

                # Test CresteClusterThread (additioal cluster)
                create_cluster_thrd = CresteClusterThread(cluster_2_name,
                        self.cluster['servers'][0]['ip'], self.cluster['servers'][0]['pm_name'],
                        cluster_2_pg_count, cluster_2_pgs_per_pg, self.leader_cm)

                create_cluster_thrd.start()
                create_cluster_thrd.join()
                self.assertTrue(create_cluster_thrd.get_result(), 'failed to create cluster')

                # Start to write commands for cluster_2
                pg_id = cluster_2_pg_count * 100
                pgs_id = pg_id
                gw_id = cluster_2_pg_count * 100
                write_cmds_2 = []
                write_cmds_2.append('pg_add %s %d' % (cluster_2_name, pg_id))
                write_cmds_2.append('pgs_add %s %d %d localhost 127.0.0.1 7100 7109' % (cluster_2_name, pgs_id, pg_id))
                write_cmds_2.append('gw_add %s %d localhost 127.0.0.1 6100' % (cluster_2_name, gw_id))
                write_cmds_2.append('gw_del %s %d' % (cluster_2_name, gw_id))
                write_cmds_2.append('pgs_del %s %d' % (cluster_2_name, pgs_id))
                write_cmds_2.append('pg_del %s %d' % (cluster_2_name, pg_id))
                write_cmd_thrd_2 = CommandThread(self.leader_cm, write_cmds_2, 'round')
                write_cmd_thrd_2.start()

                # Test RestartSMRThread for cluster 1
                restart_smr_thrd1 = RestartSMRThread(self.cluster, self.leader_cm)
                restart_smr_thrd1.start()
                restart_smr_thrd1.join()
                self.assertTrue(restart_smr_thrd1.get_result(), 'failed to restart smr')

                # Test RestartSMRThread for cluster 2
                restart_smr_thrd2 = RestartSMRThread(create_cluster_thrd.get_cluster(), self.leader_cm)
                restart_smr_thrd2.start()
                restart_smr_thrd2.join()
                self.assertTrue(restart_smr_thrd2.get_result(), 'failed to restart smr')

                # Stop CommandThreads
                read_cmd_thrd.quit()
                read_cmd_thrd.join()
                self.assertTrue(read_cmd_thrd.get_result(), 'failed to read, command:%s, error:%s' % (read_cmd_thrd.get_error_cmd(), read_cmd_thrd.get_error_msg()))

                write_cmd_thrd.quit()
                write_cmd_thrd.join()
                self.assertTrue(write_cmd_thrd.get_result(), 'failed to write, command:%s, error:%s' % (write_cmd_thrd.get_error_cmd(), write_cmd_thrd.get_error_msg()))

                write_cmd_thrd_2.quit()
                write_cmd_thrd_2.join()
                self.assertTrue(write_cmd_thrd_2.get_result(), 'failed to write, command:%s, error:%s' % (write_cmd_thrd_2.get_error_cmd(), write_cmd_thrd_2.get_error_msg()))

                # Check results
                self.assertTrue(read_cmd_thrd.get_total_rqst() > 0, 'failed to send read-commands, total_command_request:%d' % read_cmd_thrd.get_total_rqst())
                self.assertTrue(read_cmd_thrd.get_total_resp() > 0, 'failed to receive read-commands, total_command_response:%d' % read_cmd_thrd.get_total_resp())

                self.assertTrue(write_cmd_thrd.get_total_rqst() > 0, 'failed to send write-commands, total_command_request:%d' % write_cmd_thrd.get_total_rqst())
                self.assertTrue(write_cmd_thrd.get_total_resp() > 0, 'failed to receive write-commands, total_command_response:%d' % write_cmd_thrd.get_total_resp())

                self.assertTrue(write_cmd_thrd_2.get_total_rqst() > 0, 'failed to send write-commands, total_command_request:%d' % write_cmd_thrd_2.get_total_rqst())
                self.assertTrue(write_cmd_thrd_2.get_total_resp() > 0, 'failed to receive write-commands, total_command_response:%d' % write_cmd_thrd_2.get_total_resp())

                # Check cluster
                self.assertTrue(util.check_cluster(cluster_2_name, self.leader_cm['ip'], self.leader_cm['cm_port']), 'failed to check cluster status')
                self.assertTrue(util.check_cluster(self.cluster['cluster_name'], self.leader_cm['ip'], self.leader_cm['cm_port']), 'failed to check cluster status')

                # Check commands status
                util.log('total_read_command_request:%d' % read_cmd_thrd.get_total_rqst())
                util.log('total_read_command_response:%d' % read_cmd_thrd.get_total_resp())

                util.log('total_write_command_request:%d' % write_cmd_thrd.get_total_rqst())
                util.log('total_write_command_response:%d' % write_cmd_thrd.get_total_resp())

                util.log('total_write_command_request (for test_lock cluster):%d' % write_cmd_thrd_2.get_total_rqst())
                util.log('total_write_command_response (for test_lock cluster):%d' % write_cmd_thrd_2.get_total_resp())

            finally:
                ret = default_cluster.finalize( self.cluster )
                self.assertEqual(ret, 0, 'failed to TestMaintenance.finalize')
                pass

    def test_check_redis_hang_after_role_change(self):
        util.print_frame()
        load_gen_list = {}
        try:
            ret = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
            self.assertEquals( ret, 0, 'failed to TestConfMaster.initialize' )

            # start load generator
            for i in range( len(self.cluster['servers']) ):
                server = self.cluster['servers'][i]
                load_gen = load_generator.LoadGenerator(server['id'], server['ip'], server['gateway_port'], timeout=10)
                load_gen.start()
                load_gen_list[i] = load_gen

            for i in range(100):
                # get master, slave1, slave2
                m, s1, s2 = util.get_mss( self.cluster )
                self.assertNotEqual( m, None, 'master is None.' )
                self.assertNotEqual( s1, None, 'slave1 is None.' )
                self.assertNotEqual( s2, None, 'slave2 is None.' )

                util.log( 'Loop:%d, role_change, master:%d' % (i, m['id']) )
                self.role_change(m)
        finally:
            # shutdown load generators
            for i in range(len(load_gen_list)):
                load_gen_list[i].quit()
                load_gen_list[i].join()
                load_gen_list.pop(i, None)

            # shutdown cluster
            ret = default_cluster.finalize( self.cluster )
            self.assertEquals( ret, 0, 'failed to TestConfMaster.finalize' )

    def role_change(self, target):
        #util.log_server_state( self.cluster )

        # ping check
        for s in self.cluster['servers']:
            res = util.pingpong(s['ip'], s['redis_port'], logging=False)
            self.assertEqual(res, '+PONG\r\n', 'ping fail. redis_id=%d, redis_addr%s:%d, res:%s' % (s['id'], s['ip'], s['redis_port'], res))

        # print log seq
        for s in self.cluster['servers']:
            res = util.getseq_log(s)
            self.assertEqual(res, 0, 'getseq log fail. smr_id=%d, smr_addr%s:%d, res:%s' % (s['id'], s['ip'], s['redis_port'], res))

        # change master
        master = util.role_change(self.leader_cm, self.cluster['cluster_name'], (target['id'] + 1) % len(self.cluster['servers']))
        self.assertNotEqual(master, -1, 'failed : role_change')
        util.log('succeeded : role_change, new master:%d' % master)

        for s in self.cluster['servers']:
            ret, cm_role, active_role = util.check_role_consistency(s, self.leader_cm)
            self.assertTrue(ret, 'failed : role consistency, %s:%d m(%s) != a(%s)' % (s['ip'], s['smr_mgmt_port'], cm_role, active_role))

        # print log seq
        logseqs = []
        for s in self.cluster['servers']:
            res, logseq = util.getseq_log(s, getlogseq=True)
            logseqs.append(logseq)
            self.assertEqual(res, 0, 'getseq log fail. smr_id=%d, smr_addr%s:%d, res:%s' % (s['id'], s['ip'], s['redis_port'], res))

        start_time = time.time()
        # ping check
        for s in self.cluster['servers']:
            #util.log('check - replication ping to redis%d(%s:%d)' % (s['id'], s['ip'], s['redis_port']))
            while True:
                res = util.pingpong(s['ip'], s['redis_port'], logging=False)
                if res != '+PONG\r\n':
                    for svr in self.cluster['servers']:
                        util.getseq_log(svr)

                    if res == '':
                        self.assertEqual(0, 1, 'after role_change, redis does not respond from replication ping. redis_id=%d, redis_addr%s:%d' % (s['id'], s['ip'], s['redis_port']))
                    else:
                        self.assertEqual(0, 1, 'after role_change, invalid reponse of replication ping from redis. redis_id=%d, redis_addr%s:%d, res:%s' % (s['id'], s['ip'], s['redis_port'], res))
                else:
                    break

        #util.log_server_state( self.cluster )

    def test_gateway_affinity_znode(self):
        util.print_frame()
        load_gens = []

        try:
            cluster = config.clusters[5]
            leader_cm = cluster['servers'][0]
            gw_servers = map(lambda s: util.deepcopy_server(s), cluster['servers'])

            ret = util.nic_add('eth1:arc', '127.0.0.100')
            self.assertTrue(ret, 'failed to add virtual network interface.')

            ret = default_cluster.initialize_starting_up_smr_before_redis(cluster)
            self.assertEquals(ret, 0, 'failed to TestConfMaster.initialize')

            """
                +-------+----------------+------+-------+
                | GW_ID |       IP       | PORT | STATE |
                +-------+----------------+------+-------+
                |     0 |    127.0.0.100 | 8200 |  N(N) |
                |     1 |    127.0.0.100 | 9200 |  N(N) |
                |     2 |    127.0.0.100 |10200 |  N(N) |
                |     3 |      127.0.0.1 | 8210 |  N(N) |
                |     4 |      127.0.0.1 | 9210 |  N(N) |
                |     5 |      127.0.0.1 |10210 |  N(N) |
                +-------+----------------+------+-------+

                +-------+---------------------+--------+----------------+------+------+
                | PG_ID |         SLOT        | PGS_ID |       IP       | PORT | ROLE |
                +-------+---------------------+--------+----------------+------+------+
                |     0 |              0:4095 |      0 |    127.0.0.100 | 8100 |   M  |
                |       |                     |      1 |    127.0.0.100 | 9100 |   S  |
                |       |                     |      2 |    127.0.0.100 |10100 |   S  |
                +-------+---------------------+--------+----------------+------+------+
                |     1 |           4096:8191 |      3 |      127.0.0.1 | 8110 |   M  |
                |       |                     |      4 |      127.0.0.1 | 9110 |   S  |
                |       |                     |      5 |      127.0.0.1 |10110 |   S  |
                +-------+---------------------+--------+----------------+------+------+
            """

            ##########################
            # Check initial affinity #
            ##########################
            util.log('Check initial gateway affinity')
            for i in range(3):
                util.log('wait... %d' % i)
                time.sleep(1)

            expected_affinity = json.loads('[{"affinity":"A4096N4096","gw_id":0},{"affinity":"A4096N4096","gw_id":1},{"affinity":"A4096N4096","gw_id":2},{"affinity":"N4096A4096","gw_id":3},{"affinity":"N4096A4096","gw_id":4},{"affinity":"N4096A4096","gw_id":5}]')
            expected_affinity = sorted(expected_affinity, key=lambda x: int(x['gw_id']))
            ok = util.await(10)(
                lambda real_affinity : real_affinity == expected_affinity, 
                lambda : sorted(json.loads(util.get_gateway_affinity(cluster['cluster_name'])), key=lambda x: int(x['gw_id'])))
            self.assertTrue(ok, '[INITIALIZATION] check gateway affinity fail.')

            ###################
            # Start C clients #
            ###################
            load_gens = self.create_load_gens(cluster, leader_cm, 'singlekey')

            for i in xrange(10):
                util.log('perform some I/O to the test cluster. %d second' % (i+1))
                time.sleep(1)

            #############
            # Check OPS #
            #############
            condition = (lambda s: s['ops'] > 100)
            ok = util.check_ops(cluster['servers'], 'gw', condition)
            self.assertTrue(ok, 'No request to gateways. Check whether arcci sends request to gateway.')

            ############################
            # Check affinity hit ratio #
            ############################
            ret, hit_ratio = util.get_cluster_affinity_hit_ratio(gw_servers)
            self.assertTrue(ret, '[AFFINITY_HIT_RATIO] failed to get hit ratio')
            util.log('affinity-hit-ratio:%0.3f' % hit_ratio)
            self.assertGreater(hit_ratio, 0.90, '[AFFINITY_HIT_RATIO] too low affinity-hit-ratio:%0.3f' % hit_ratio)

            ##################################
            # Add gateway and check affinity #
            ##################################
            util.log('Add gateway and check gateway affinity')
            gw_server = util.deepcopy_server(leader_cm)
            gw_id = 100
            gw_server['id'] = gw_id
            gw_server['gateway_port'] = 8230

            ret = util.deploy_gateway(gw_id)
            self.assertTrue(ret, '[ADD GW] deploy gateway fail. gw_id:%d' % gw_id)

            ret = testbase.request_to_start_gateway(cluster['cluster_name'], gw_server, leader_cm)
            self.assertEqual(ret, 0, '[ADD GW] start gateway fail. gw_id:%d' % gw_id)

            for i in range(2):
                util.log('wait... %d' % i)
                time.sleep(1)

            expected_affinity = json.loads('[{"affinity":"A4096N4096","gw_id":0},{"affinity":"A4096N4096","gw_id":1},{"affinity":"A4096N4096","gw_id":2},{"affinity":"N4096A4096","gw_id":3},{"affinity":"N4096A4096","gw_id":4},{"affinity":"N4096A4096","gw_id":5},{"affinity":"A4096N4096","gw_id":100}]')
            expected_affinity = sorted(expected_affinity, key=lambda x: int(x['gw_id']))
            ok = util.await(10)(
                lambda real_affinity : real_affinity == expected_affinity, 
                lambda : sorted(json.loads(util.get_gateway_affinity(cluster['cluster_name'])), key=lambda x: int(x['gw_id'])))
            self.assertTrue(ok, '[ADD GATEWAY] check gateway affinity fail.')

            #####################################
            # Delete gateway and check affinity #
            #####################################
            util.log('Delete gateway and check gateway affinity')
            ret = testbase.request_to_shutdown_gateway(cluster['cluster_name'], gw_server, leader_cm)
            self.assertEqual(ret, 0, '[DELETE GW] shutdown gateway fail. gw_id:%d' % gw_id)

            for i in range(2):
                util.log('wait... %d' % i)
                time.sleep(1)

            expected_affinity = json.loads('[{"affinity":"A4096N4096","gw_id":0},{"affinity":"A4096N4096","gw_id":1},{"affinity":"A4096N4096","gw_id":2},{"affinity":"N4096A4096","gw_id":3},{"affinity":"N4096A4096","gw_id":4},{"affinity":"N4096A4096","gw_id":5}]')
            expected_affinity = sorted(expected_affinity, key=lambda x: int(x['gw_id']))
            ok = util.await(10)(
                lambda real_affinity : real_affinity == expected_affinity, 
                lambda : sorted(json.loads(util.get_gateway_affinity(cluster['cluster_name'])), key=lambda x: int(x['gw_id'])))
            self.assertTrue(ok, '[DELETE GATEWAY] check gateway affinity fail.')

            #########################################
            # Migration - Add PG and check affinity #
            #########################################
            server1 = util.deepcopy_server(leader_cm)
            server1['id'] = 100
            server1['ip'] = '127.0.0.100'
            server1['real_ip'] = '127.0.0.1'
            server1['pm_name'] = 'virtual_localhost'
            server1['pg_id'] = 2
            server1['smr_base_port'] = 8120
            server1['smr_mgmt_port'] = 8123
            server1['gateway_port'] = 8220
            server1['redis_port'] = 8129
            server1['cluster_name'] = 'network_isolation_cluster_1'

            server2 = util.deepcopy_server(leader_cm)
            server2['id'] = 101
            server2['ip'] = '127.0.0.1'
            server2['real_ip'] = '127.0.0.1'
            server2['pm_name'] = 'localhost'
            server2['pg_id'] = 2
            server2['smr_base_port'] = 9120
            server2['smr_mgmt_port'] = 9123
            server2['gateway_port'] = 9220
            server2['redis_port'] = 9129
            server2['cluster_name'] = 'network_isolation_cluster_1'

            servers = [server1, server2]

            ret = util.deploy_pgs(server1['id'])
            self.assertTrue(ret, '[ADD PG] deploy pgs fail. pgs_id:%d' % server1['id'])
            ret = util.deploy_pgs(server2['id'])
            self.assertTrue(ret, '[ADD PG] deploy pgs fail. pgs_id:%d' % server2['id'])

            ret = util.pg_add(cluster, servers, leader_cm, start_gw=False)
            self.assertTrue(ret, '[ADD PG] add pg fail.')
            cluster['servers'].append(server1)
            cluster['servers'].append(server2)

            for i in range(5):
                util.log('wait... %d' % i)
                time.sleep(1)

            ret = util.migration(cluster, 0, 2, 2000, 4095, 50000)
            self.assertTrue(ret, '[ADD PG] migration fail 0 -> 2')

            ret = util.migration(cluster, 1, 2, 6096, 8191, 50000)
            self.assertTrue(ret, '[ADD PG] migration fail 1 -> 2')

            """
                +-------+----------------+------+-------+
                | GW_ID |       IP       | PORT | STATE |
                +-------+----------------+------+-------+
                |     0 |    127.0.0.100 | 8200 |  N(N) |
                |     1 |    127.0.0.100 | 9200 |  N(N) |
                |     2 |    127.0.0.100 |10200 |  N(N) |
                |     3 |      127.0.0.1 | 8210 |  N(N) |
                |     4 |      127.0.0.1 | 9210 |  N(N) |
                |     5 |      127.0.0.1 |10210 |  N(N) |
                +-------+----------------+------+-------+

                +-------+---------------------+--------+----------------+------+-----------+
                | PG_ID |         SLOT        | PGS_ID |       IP       | PORT |    ROLE   |
                +-------+---------------------+--------+----------------+------+-----------+
                |     0 |              0:1999 |      0 |    127.0.0.100 | 8100 | M         |
                |       |                     |      1 |    127.0.0.100 | 9100 | S         |
                |       |                     |      2 |    127.0.0.100 |10100 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     1 |           4096:6095 |      3 |      127.0.0.1 | 8110 | M         |
                |       |                     |      4 |      127.0.0.1 | 9110 | S         |
                |       |                     |      5 |      127.0.0.1 |10110 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     2 | 2000:4095 6096:8191 |    100 |    127.0.0.100 | 8120 | M server1 |
                |       |                     |    101 |      127.0.0.1 | 9120 | S server2 |
                +-------+---------------------+--------+----------------+------+-----------+
                or
                +-------+---------------------+--------+----------------+------+-----------+
                |     2 | 2000:4095 6096:8191 |    100 |    127.0.0.100 | 8120 | S server1 |
                |       |                     |    101 |      127.0.0.1 | 9120 | M server2 |
                +-------+---------------------+--------+----------------+------+-----------+
            """

            expected_affinity = []
            expected_affinity.append(
                sorted(
                    json.loads('[{"affinity":"A4096N2000A2096","gw_id":0},{"affinity":"A4096N2000A2096","gw_id":1},{"affinity":"A4096N2000A2096","gw_id":2},{"affinity":"N2000R2096A2000R2096","gw_id":3},{"affinity":"N2000R2096A2000R2096","gw_id":4},{"affinity":"N2000R2096A2000R2096","gw_id":5}]'),
                    key=lambda x: int(x['gw_id'])))
            expected_affinity.append(
                sorted(
                    json.loads('[{"affinity":"A2000R2096N2000R2096","gw_id":0},{"affinity":"A2000R2096N2000R2096","gw_id":1},{"affinity":"A2000R2096N2000R2096","gw_id":2},{"affinity":"N2000A6192","gw_id":3},{"affinity":"N2000A6192","gw_id":4},{"affinity":"N2000A6192","gw_id":5}]'),
                    key=lambda x: int(x['gw_id'])))
            ok = util.await(10)(
                lambda real_affinity : (expected_affinity.remove(real_affinity) or True) if (real_affinity in expected_affinity) else False, 
                lambda : sorted(json.loads(util.get_gateway_affinity(cluster['cluster_name'])), key=lambda x: int(x['gw_id'])))
            self.assertTrue(ok, '[ADD PG] check gateway affinity fail.')

            ##################################
            # Role change and check affinity #
            ##################################
            util.log('Check affinity after role change')
            if util.get_smr_role_of_cm(server2, leader_cm) == 'S':
                util.role_change(leader_cm, cluster['cluster_name'], server2['id'])
            else:
                util.role_change(leader_cm, cluster['cluster_name'], server1['id'])

            """
                +-------+----------------+------+-------+
                | GW_ID |       IP       | PORT | STATE |
                +-------+----------------+------+-------+
                |     0 |    127.0.0.100 | 8200 |  N(N) |
                |     1 |    127.0.0.100 | 9200 |  N(N) |
                |     2 |    127.0.0.100 |10200 |  N(N) |
                |     3 |      127.0.0.1 | 8210 |  N(N) |
                |     4 |      127.0.0.1 | 9210 |  N(N) |
                |     5 |      127.0.0.1 |10210 |  N(N) |
                +-------+----------------+------+-------+

                +-------+---------------------+--------+----------------+------+-----------+
                | PG_ID |         SLOT        | PGS_ID |       IP       | PORT |    ROLE   |
                +-------+---------------------+--------+----------------+------+-----------+
                |     0 |              0:1999 |      0 |    127.0.0.100 | 8100 | M         |
                |       |                     |      1 |    127.0.0.100 | 9100 | S         |
                |       |                     |      2 |    127.0.0.100 |10100 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     1 |           4096:6095 |      3 |      127.0.0.1 | 8110 | M         |
                |       |                     |      4 |      127.0.0.1 | 9110 | S         |
                |       |                     |      5 |      127.0.0.1 |10110 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     2 | 2000:4095 6096:8191 |    100 |    127.0.0.100 | 8120 | S server1 |
                |       |                     |    101 |      127.0.0.1 | 9120 | M server2 |
                +-------+---------------------+--------+----------------+------+-----------+
                or
                +-------+---------------------+--------+----------------+------+-----------+
                |     2 | 2000:4095 6096:8191 |    100 |    127.0.0.100 | 8120 | S server1 |
                |       |                     |    101 |      127.0.0.1 | 9120 | M server2 |
                +-------+---------------------+--------+----------------+------+-----------+
            """

            ok = util.await(10)(
                lambda real_affinity : real_affinity == expected_affinity[0], 
                lambda : sorted(json.loads(util.get_gateway_affinity(cluster['cluster_name'])), key=lambda x: int(x['gw_id'])))
            self.assertTrue(ok, '[ROLE CHANGE] check gateway affinity fail.')

            ##########################################
            # Master PGS Failover and check affinity #
            ##########################################
            util.log('Check affinity after Master PGS failure')
            server = server2

            # shutdown
            ret = testbase.request_to_shutdown_smr(server)
            self.assertEqual(ret, 0, '[MASTER PGS FAILOVER] failed to shutdown smr')
            ret = testbase.request_to_shutdown_redis(server)
            self.assertEquals(ret, 0, '[MASTER PGS FAILOVER] failed to shutdown redis')

            max_try = 20
            expected = 'F'
            for i in range(0, max_try):
                state = util.get_smr_state(server, leader_cm)
                if expected == state:
                    break;
                time.sleep( 1 )
            self.assertEquals( expected, state,
                               '[MASTER PGS FAILOVER] server%d - state:%s, expected:%s' % (server['id'], state, expected) )

            """
                +-------+----------------+------+-------+
                | GW_ID |       IP       | PORT | STATE |
                +-------+----------------+------+-------+
                |     0 |    127.0.0.100 | 8200 |  N(N) |
                |     1 |    127.0.0.100 | 9200 |  N(N) |
                |     2 |    127.0.0.100 |10200 |  N(N) |
                |     3 |      127.0.0.1 | 8210 |  N(N) |
                |     4 |      127.0.0.1 | 9210 |  N(N) |
                |     5 |      127.0.0.1 |10210 |  N(N) |
                +-------+----------------+------+-------+

                +-------+---------------------+--------+----------------+------+-----------+
                | PG_ID |         SLOT        | PGS_ID |       IP       | PORT |    ROLE   |
                +-------+---------------------+--------+----------------+------+-----------+
                |     0 |              0:1999 |      0 |    127.0.0.100 | 8100 | M         |
                |       |                     |      1 |    127.0.0.100 | 9100 | S         |
                |       |                     |      2 |    127.0.0.100 |10100 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     1 |           4096:6095 |      3 |      127.0.0.1 | 8110 | M         |
                |       |                     |      4 |      127.0.0.1 | 9110 | S         |
                |       |                     |      5 |      127.0.0.1 |10110 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     2 | 2000:4095 6096:8191 |    100 |    127.0.0.100 | 8120 | M server1 |
                |       |                     |    101 |      127.0.0.1 | 9120 | N server2 |
                +-------+---------------------+--------+----------------+------+-----------+
            """

            # check affinity
            expected_affinity = json.loads('[{"affinity":"A4096N2000A2096","gw_id":0},{"affinity":"A4096N2000A2096","gw_id":1},{"affinity":"A4096N2000A2096","gw_id":2},{"affinity":"N4096A2000N2096","gw_id":3},{"affinity":"N4096A2000N2096","gw_id":4},{"affinity":"N4096A2000N2096","gw_id":5}]')
            expected_affinity = sorted(expected_affinity, key=lambda x: int(x['gw_id']))
            ok = util.await(10)(
                lambda real_affinity : real_affinity == expected_affinity, 
                lambda : sorted(json.loads(util.get_gateway_affinity(cluster['cluster_name'])), key=lambda x: int(x['gw_id'])))
            self.assertTrue(ok, '[MASTER PGS FAILOVER] check gateway affinity fail.')

            # recovery
            ret = testbase.request_to_start_smr(server)
            self.assertEqual(ret, 0, '[MASTER PGS FAILOVER] failed to start smr')
            ret = testbase.request_to_start_redis(server)
            self.assertEqual(ret, 0, '[MASTER PGS FAILOVER] failed to start redis')

            ret = testbase.wait_until_finished_to_set_up_role(server, max_try)
            self.assertEquals(ret, 0, '[MASTER PGS FAILOVER] failed to role change. smr_id:%d' % (server['id']))
            """
                +-------+----------------+------+-------+
                | GW_ID |       IP       | PORT | STATE |
                +-------+----------------+------+-------+
                |     0 |    127.0.0.100 | 8200 |  N(N) |
                |     1 |    127.0.0.100 | 9200 |  N(N) |
                |     2 |    127.0.0.100 |10200 |  N(N) |
                |     3 |      127.0.0.1 | 8210 |  N(N) |
                |     4 |      127.0.0.1 | 9210 |  N(N) |
                |     5 |      127.0.0.1 |10210 |  N(N) |
                +-------+----------------+------+-------+

                +-------+---------------------+--------+----------------+------+-----------+
                | PG_ID |         SLOT        | PGS_ID |       IP       | PORT |    ROLE   |
                +-------+---------------------+--------+----------------+------+-----------+
                |     0 |              0:1999 |      0 |    127.0.0.100 | 8100 | M         |
                |       |                     |      1 |    127.0.0.100 | 9100 | S         |
                |       |                     |      2 |    127.0.0.100 |10100 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     1 |           4096:6095 |      3 |      127.0.0.1 | 8110 | M         |
                |       |                     |      4 |      127.0.0.1 | 9110 | S         |
                |       |                     |      5 |      127.0.0.1 |10110 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     2 | 2000:4095 6096:8191 |    100 |    127.0.0.100 | 8120 | M server1 |
                |       |                     |    101 |      127.0.0.1 | 9120 | S server2 |
                +-------+---------------------+--------+----------------+------+-----------+
            """

            # check affinity
            expected_affinity = json.loads('[{"affinity":"A4096N2000A2096","gw_id":0},{"affinity":"A4096N2000A2096","gw_id":1},{"affinity":"A4096N2000A2096","gw_id":2},{"affinity":"N2000R2096A2000R2096","gw_id":3},{"affinity":"N2000R2096A2000R2096","gw_id":4},{"affinity":"N2000R2096A2000R2096","gw_id":5}]')
            expected_affinity = sorted(expected_affinity, key=lambda x: int(x['gw_id']))
            ok = util.await(10)(
                lambda real_affinity : real_affinity == expected_affinity, 
                lambda : sorted(json.loads(util.get_gateway_affinity(cluster['cluster_name'])), key=lambda x: int(x['gw_id'])))
            self.assertTrue(ok, '[MASTER PGS FAILOVER] check gateway affinity fail.')

            #########################################
            # Slave PGS Failover and check affinity #
            #########################################
            util.log('Check affinity after Slave PGS failure')
            server = server2

            # shutdown
            ret = testbase.request_to_shutdown_smr(server)
            self.assertEqual(ret, 0, '[SLAVE PGS FAILOVER] failed to shutdown smr')
            ret = testbase.request_to_shutdown_redis(server)
            self.assertEquals(ret, 0, '[SLAVE PGS FAILOVER] failed to shutdown redis')

            max_try = 20
            expected = 'F'
            for i in range(0, max_try):
                state = util.get_smr_state(server, leader_cm)
                if expected == state:
                    break;
                time.sleep( 1 )
            self.assertEquals( expected, state,
                               '[SLAVE PGS FAILOVER] server%d - state:%s, expected:%s' % (server['id'], state, expected) )
            """
                +-------+----------------+------+-------+
                | GW_ID |       IP       | PORT | STATE |
                +-------+----------------+------+-------+
                |     0 |    127.0.0.100 | 8200 |  N(N) |
                |     1 |    127.0.0.100 | 9200 |  N(N) |
                |     2 |    127.0.0.100 |10200 |  N(N) |
                |     3 |      127.0.0.1 | 8210 |  N(N) |
                |     4 |      127.0.0.1 | 9210 |  N(N) |
                |     5 |      127.0.0.1 |10210 |  N(N) |
                +-------+----------------+------+-------+

                +-------+---------------------+--------+----------------+------+-----------+
                | PG_ID |         SLOT        | PGS_ID |       IP       | PORT |    ROLE   |
                +-------+---------------------+--------+----------------+------+-----------+
                |     0 |              0:1999 |      0 |    127.0.0.100 | 8100 | M         |
                |       |                     |      1 |    127.0.0.100 | 9100 | S         |
                |       |                     |      2 |    127.0.0.100 |10100 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     1 |           4096:6095 |      3 |      127.0.0.1 | 8110 | M         |
                |       |                     |      4 |      127.0.0.1 | 9110 | S         |
                |       |                     |      5 |      127.0.0.1 |10110 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     2 | 2000:4095 6096:8191 |    100 |    127.0.0.100 | 8120 | M server1 |
                |       |                     |    101 |      127.0.0.1 | 9120 | N server2 |
                +-------+---------------------+--------+----------------+------+-----------+
            """

            # check affinity
            expected_affinity = json.loads('[{"affinity":"A4096N2000A2096","gw_id":0},{"affinity":"A4096N2000A2096","gw_id":1},{"affinity":"A4096N2000A2096","gw_id":2},{"affinity":"N4096A2000N2096","gw_id":3},{"affinity":"N4096A2000N2096","gw_id":4},{"affinity":"N4096A2000N2096","gw_id":5}]')
            expected_affinity = sorted(expected_affinity, key=lambda x: int(x['gw_id']))
            ok = util.await(10)(
                lambda real_affinity : (util.log("real_affinity:%s" % real_affinity) or True) if (real_affinity == expected_affinity) else False, 
                lambda : sorted(json.loads(util.get_gateway_affinity(cluster['cluster_name'])), key=lambda x: int(x['gw_id'])))
            util.log("expected_affinity:%s" % expected_affinity)
            self.assertTrue(ok, '[SLAVE PGS FAILOVER] check gateway affinity fail.')

            # recovery
            ret = testbase.request_to_start_smr(server)
            self.assertEqual(ret, 0, '[SLAVE PGS FAILOVER] failed to start smr')
            ret = testbase.request_to_start_redis(server)
            self.assertEqual(ret, 0, '[SLAVE PGS FAILOVER] failed to start redis')

            ret = testbase.wait_until_finished_to_set_up_role(server, max_try)
            self.assertEquals(ret, 0, '[SLAVE PGS FAILOVER] failed to role change. smr_id:%d' % (server['id']))
            """
                +-------+----------------+------+-------+
                | GW_ID |       IP       | PORT | STATE |
                +-------+----------------+------+-------+
                |     0 |    127.0.0.100 | 8200 |  N(N) |
                |     1 |    127.0.0.100 | 9200 |  N(N) |
                |     2 |    127.0.0.100 |10200 |  N(N) |
                |     3 |      127.0.0.1 | 8210 |  N(N) |
                |     4 |      127.0.0.1 | 9210 |  N(N) |
                |     5 |      127.0.0.1 |10210 |  N(N) |
                +-------+----------------+------+-------+

                +-------+---------------------+--------+----------------+------+-----------+
                | PG_ID |         SLOT        | PGS_ID |       IP       | PORT |    ROLE   |
                +-------+---------------------+--------+----------------+------+-----------+
                |     0 |              0:1999 |      0 |    127.0.0.100 | 8100 | M         |
                |       |                     |      1 |    127.0.0.100 | 9100 | S         |
                |       |                     |      2 |    127.0.0.100 |10100 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     1 |           4096:6095 |      3 |      127.0.0.1 | 8110 | M         |
                |       |                     |      4 |      127.0.0.1 | 9110 | S         |
                |       |                     |      5 |      127.0.0.1 |10110 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     2 | 2000:4095 6096:8191 |    100 |    127.0.0.100 | 8120 | M server1 |
                |       |                     |    101 |      127.0.0.1 | 9120 | S server2 |
                +-------+---------------------+--------+----------------+------+-----------+
            """

            # check affinity
            expected_affinity = json.loads('[{"affinity":"A4096N2000A2096","gw_id":0},{"affinity":"A4096N2000A2096","gw_id":1},{"affinity":"A4096N2000A2096","gw_id":2},{"affinity":"N2000R2096A2000R2096","gw_id":3},{"affinity":"N2000R2096A2000R2096","gw_id":4},{"affinity":"N2000R2096A2000R2096","gw_id":5}]')
            expected_affinity = sorted(expected_affinity, key=lambda x: int(x['gw_id']))
            ok = util.await(10)(
                lambda real_affinity : real_affinity == expected_affinity, 
                lambda : sorted(json.loads(util.get_gateway_affinity(cluster['cluster_name'])), key=lambda x: int(x['gw_id'])))
            self.assertTrue(ok, '[SLAVE PGS FAILOVER] check gateway affinity fail.')

            ################################
            # Delete PG and check affinity #
            ################################
            ret = util.migration(cluster, 2, 1, 6096, 8191, 50000)
            self.assertTrue(ret, '[DELETE PG] migration fail 2 -> 1')

            ret = util.migration(cluster, 2, 0, 2000, 4095, 50000)
            self.assertTrue(ret, '[ADD PG] migration fail 2 -> 0')

            ret = util.pg_del(cluster, servers, leader_cm, stop_gw=False)
            self.assertTrue(ret, '[DELETE PG] delete pg fail.')

            expected_affinity = json.loads('[{"affinity":"A4096N4096","gw_id":0},{"affinity":"A4096N4096","gw_id":1},{"affinity":"A4096N4096","gw_id":2},{"affinity":"N4096A4096","gw_id":3},{"affinity":"N4096A4096","gw_id":4},{"affinity":"N4096A4096","gw_id":5}]')
            expected_affinity = sorted(expected_affinity, key=lambda x: int(x['gw_id']))
            ok = util.await(10)(
                lambda real_affinity : real_affinity == expected_affinity, 
                lambda : sorted(json.loads(util.get_gateway_affinity(cluster['cluster_name'])), key=lambda x: int(x['gw_id'])))
            self.assertTrue(ok, '[DELETE PG] check gateway affinity fail.')

            ############################
            # Check affinity hit ratio #
            ############################
            ret, hit_ratio = util.get_cluster_affinity_hit_ratio(gw_servers)
            self.assertTrue(ret, '[AFFINITY_HIT_RATIO] failed to get hit ratio')
            util.log('affinity-hit-ratio:%0.3f' % hit_ratio)
            self.assertGreater(hit_ratio, 0.90, '[AFFINITY_HIT_RATIO] too low affinity-hit-ratio:%0.3f' % hit_ratio)

            ##################
            # Write Affinity #
            ##################
            """
                +-------+----------------+------+-------+
                | GW_ID |       IP       | PORT | STATE |
                +-------+----------------+------+-------+
                |     0 |    127.0.0.100 | 8200 |  N(N) |
                |     1 |    127.0.0.100 | 9200 |  N(N) |
                |     2 |    127.0.0.100 |10200 |  N(N) |
                |     3 |      127.0.0.1 | 8210 |  N(N) |
                |     4 |      127.0.0.1 | 9210 |  N(N) |
                |     5 |      127.0.0.1 |10210 |  N(N) |
                +-------+----------------+------+-------+

                +-------+---------------------+--------+----------------+------+-----------+
                | PG_ID |         SLOT        | PGS_ID |       IP       | PORT |    ROLE   |
                +-------+---------------------+--------+----------------+------+-----------+
                |     0 |              0:4095 |      0 |    127.0.0.100 | 8100 | M         |
                |       |                     |      1 |    127.0.0.100 | 9100 | S         |
                |       |                     |      2 |    127.0.0.100 |10100 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
                |     1 |           4096:8191 |      3 |      127.0.0.1 | 8110 | M         |
                |       |                     |      4 |      127.0.0.1 | 9110 | S         |
                |       |                     |      5 |      127.0.0.1 |10110 | S         |
                +-------+---------------------+--------+----------------+------+-----------+
            """

            affinity = '[{\\"affinity\\":\\"W4096N4096\\",\\"gw_id\\":0},{\\"affinity\\":\\"W4096N4096\\",\\"gw_id\\":1},{\\"affinity\\":\\"W4096N4096\\",\\"gw_id\\":2},{\\"affinity\\":\\"N4096W4096\\",\\"gw_id\\":3},{\\"affinity\\":\\"N4096W4096\\",\\"gw_id\\":4},{\\"affinity\\":\\"N4096W4096\\",\\"gw_id\\":5}]'
            ret = util.zk_cmd('set /RC/NOTIFICATION/CLUSTER/%s/AFFINITY "%s"' % (cluster['cluster_name'], affinity))

            # Check OPS of Gateways
            con_cnt = 0
            for i in range(10):
                ok = True
                for s in self.cluster['servers']:
                    tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                    util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                    if tps < 500:
                        ok = False

                if ok:
                    con_cnt += 1
                    if con_cnt > 2:
                        break
                else:
                    con_cnt = 0

                time.sleep(1)

            if con_cnt >= 3:
                util.log('SUCCESS, arcci follows write affinity')
            else:
                self.fail('FAIL, arcci does not follow write affinity')

        finally:
            util.nic_del('eth1:arc')

            self.destroy_load_gens(load_gens)

            # shutdown cluster
            #ret = default_cluster.finalize(cluster)
            #self.assertEquals(ret, 0, 'failed to TestConfMaster.finalize')

            while len(cluster['servers']) > 6:
                cluster['servers'].pop(6)

    def fd_leak_test_wrapper(test_name):
        def wrapper(function):
            @functools.wraps(function)
            def call(self, **kwargs):
                try:
                    cluster = config.clusters[0]
                    leader_cm = cluster['servers'][0]

                    ret = default_cluster.initialize_starting_up_smr_before_redis(cluster)
                    self.assertEquals(ret, 0, 'failed to TestConfMaster.initialize')

                    # Check cluster state
                    ok = False
                    for i in xrange(10):
                        ok = util.check_cluster(cluster['cluster_name'], leader_cm['ip'], leader_cm['cm_port'], state=None, check_quorum=True)
                        if ok:
                            break
                        else:
                            time.sleep(1)
                    self.assertTrue(ok, 'Invalid cluster state')

                    dst_ports = []
                    for s in cluster['servers']:
                        dst_ports.append(s['smr_mgmt_port'])
                        dst_ports.append(s['redis_port'])
                        dst_ports.append(s['gateway_port'])
                        dst_ports.append(s['gateway_mgmt_port'])

                    for i in xrange(5):
                        # Get initial number of file descriptors of MGMT-CC.
                        init_fds = {s['id']: util.cm_get_socket_cnt(s, dst_ports) for s in cluster['servers']}
                        util.log('[FD COUNT]')
                        for id, fd in init_fds.items():
                            util.log("Initial file descriptor of CM%d : %d" % (id, fd))
                        time.sleep(1)

                    # Call test method
                    function(self, cluster=cluster, leader_cm=leader_cm, init_fds=init_fds)

                finally:
                    # shutdown cluster
                    ret = default_cluster.finalize(cluster)
                    self.assertEquals(ret, 0, 'failed to TestConfMaster.finalize')
                    pass
            call._original = function
            return call
        return wrapper

    # Upgrade GW and check leaks of connections of MGMT-CC.
    @fd_leak_test_wrapper('test_fd_leak_after_gw_upgrade')
    def test_fd_leak_after_gw_upgrade(self, **kwargs):
        util.print_frame()
        cluster = kwargs['cluster']
        leader_cm = kwargs['leader_cm']
        init_fds = kwargs['init_fds']

        dst_ports = []
        for s in cluster['servers']:
            dst_ports.append(s['smr_mgmt_port'])
            dst_ports.append(s['redis_port'])
            dst_ports.append(s['gateway_port'])
            dst_ports.append(s['gateway_mgmt_port'])

        for i in xrange(2):
            util.log('')
            util.log('')
            util.log(' ### LOOP %d ###' % i)
            for s in cluster['servers']:
                # Upgrade a gateway
                ret = util.upgrade_gw(s, leader_cm)
                self.assertTrue(ret, 'Failed to upgrade gateway %d (%s:%d)' % (s['id'], s['ip'], s['gateway_port']))
                time.sleep(3)

                ok = True
                # Observe the number of file descriptors
                for i in xrange(20):
                    util.log(' ### Check File descriptor ###')
                    ok = True
                    cur_fds = {s['id']: util.cm_get_socket_cnt(s, dst_ports) for s in cluster['servers']}
                    for id, cur_fd in cur_fds.items():
                        expected_fd = init_fds[id]
                        util.log('File descriptor of CM%d : %d %s %d' %
                                (id, expected_fd, '==' if expected_fd == cur_fd else '!=', cur_fd))
                        if id == leader_cm['id']:
                            expected_fd = 15
                        else:
                            expected_fd = 9

                        if expected_fd < cur_fd:
                            ok = False

                    if ok:
                        break
                    else:
                        time.sleep(1)

                self.assertTrue(ok, 'File descriptor mismatch.')

    # Upgrade PGS and check leaks of connections of MGMT-CC.
    @fd_leak_test_wrapper('test_fd_leak_after_pgs_upgrade')
    def test_fd_leak_after_pgs_upgrade(self, **kwargs):
        util.print_frame()
        cluster = kwargs['cluster']
        leader_cm = kwargs['leader_cm']
        init_fds = kwargs['init_fds']

        dst_ports = []
        for s in cluster['servers']:
            dst_ports.append(s['smr_mgmt_port'])
            dst_ports.append(s['redis_port'])
            dst_ports.append(s['gateway_port'])
            dst_ports.append(s['gateway_mgmt_port'])

        for i in xrange(2):
            util.log('')
            util.log('')
            util.log(' ### LOOP %d ###' % i)
            for s in cluster['servers']:
                role = util.smr_role(s['ip'], s['smr_mgmt_port'])
                # Upgade a pgs
                ret = util.upgrade_pgs(s, leader_cm, cluster)
                self.assertTrue(ret, 'Failed to upgrade smr %d (%s:%d)' % (s['id'], s['ip'], s['smr_base_port']))

                ok = True
                for i in xrange(20):
                    ok = True
                    # Observe the number of file descriptors
                    cur_fds = {s['id']: util.cm_get_socket_cnt(s, dst_ports) for s in cluster['servers']}
                    for id, cur_fd in cur_fds.items():
                        init_fd = init_fds[id]
                        if id == leader_cm['id']:
                            # 3(command smr) + 3(hb smr) + 2~3(command redis) + 3(hb redis) + 3(hb gw) + 3(command gw)
                            expected_fd = 18
                        else:
                            # 3(hb smr) + 3(hb redis) + 3(command gw)
                            expected_fd = 9
                        util.log('File descriptor of CM%d : %d -> %d' % (id, init_fd, cur_fd))

                        if expected_fd < cur_fd:
                            ok = False

                    if ok:
                        break
                    else:
                        time.sleep(1)

                self.assertTrue(ok, 'File descriptor mismatch.')

    # PGS Failover and check leaks of connections of MGMT-CC.
    @fd_leak_test_wrapper('test_fd_leak_after_pgs_failover')
    def test_fd_leak_after_pgs_failover(self, **kwargs):
        util.print_frame()
        cluster = kwargs['cluster']
        leader_cm = kwargs['leader_cm']

        dst_ports = []
        for s in cluster['servers']:
            dst_ports.append(s['smr_mgmt_port'])
            dst_ports.append(s['redis_port'])
            dst_ports.append(s['gateway_port'])
            dst_ports.append(s['gateway_mgmt_port'])

        # Get initial number of FDs
        init_fds = {s['id']: util.cm_get_socket_cnt(s, dst_ports) for s in cluster['servers']}
        util.log('[FD COUNT]')
        for id, fd in init_fds.items():
            util.log("Initial file descriptor of CM%d : %d" % (id, fd))

        for i in xrange(5):
            util.log('')
            util.log('')
            util.log(' ### LOOP %d ###' % i)
            for s in cluster['servers']:
                # PGS Failover
                ret = util.failover(s, leader_cm)
                self.assertTrue(ret, 'Failed to failover smr %d (%s:%d)' % (s['id'], s['ip'], s['smr_base_port']))
                time.sleep(3)

                ok = True
                for i in xrange(20):
                    ok = True
                    # Check connection leak of MGMT-CC
                    cur_fds = {s['id']: util.cm_get_socket_cnt(s, dst_ports) for s in cluster['servers']}
                    for id, cur_fd in cur_fds.items():
                        init_fd = init_fds[id]
                        util.log('File descriptor of CM%d : %d -> %d' % (id, init_fd, cur_fd))

                        if id == leader_cm['id']:
                            # 3(command smr) + 3(hb smr) + 3(command redis) + 3(hb redis) + 3(hb gw) + 3(command gw)
                            # smr commands : 'role master', 'role slave', 'role lconn', 'setquorum' and etc.
                            # redis commands : replication-ping and etc.
                            # gw commands : pgs_add, pgs_del, delay, redirect and etc.
                            expected_fd = 18
                        else:
                            # 3(hb smr) + 3(hb redis) + 3(hb gw)
                            expected_fd = 9

                        if expected_fd < cur_fd:
                            ok = False

                    if ok:
                        break
                    else:
                        time.sleep(1)

                self.assertTrue(ok, 'File descriptor mismatch.')

    # Uninstall cluster and check leaks of connections of MGMT-CC
    @fd_leak_test_wrapper('test_fd_leak_after_uninstall_cluster')
    def test_fd_leak_after_uninstall_cluster(self, **kwargs):
        util.print_frame()
        cluster = kwargs['cluster']
        leader_cm = kwargs['leader_cm']
        init_fds = kwargs['init_fds']

        dst_ports = []
        for s in cluster['servers']:
            dst_ports.append(s['smr_mgmt_port'])
            dst_ports.append(s['redis_port'])
            dst_ports.append(s['gateway_port'])
            dst_ports.append(s['gateway_mgmt_port'])

        # Remove PGS, GW
        ret = util.pg_del(cluster, cluster['servers'], leader_cm, stop_gw=True)
        self.assertTrue(ret, 'failed to delete all PGS and GW')

        ok = True
        for i in xrange(20):
            ok = True
            # Check leaks of connections of MGMT-CC
            cur_fds = {s['id']: util.cm_get_socket_cnt(s, dst_ports) for s in cluster['servers']}
            util.log('[FD COUNT]')
            for id, cur_fd in cur_fds.items():
                init_fd = init_fds[id]
                util.log('File descriptor of CM%d : %d -> %d' % (id, init_fd, cur_fd))

                if 0 != cur_fd:
                    ok = False

            if ok:
                break
            else:
                time.sleep(1)

        self.assertTrue(ok, 'File descriptor mismatch')

    """
    To check the number of connections of MGMT-CC:
        On linux, file descriptor of MGMT-CC is used for sockets, open files, and etc.
        After amdin adds 3 GWs, 3 PGSs, and MGMT-CC creates 9 heartbeat connections. (GW x 3, REDIS x 3, SMR x 3)
        Leader MGMT-CC has additional connections that send commands to each server.
    """
    @fd_leak_test_wrapper('test_fd_leak')
    def test_fd_leak(self, **kwargs):
        util.print_frame()
        self.test_fd_leak_after_gw_upgrade._original(self, **kwargs)
        self.test_fd_leak_after_pgs_upgrade._original(self, **kwargs)
        self.test_fd_leak_after_pgs_failover._original(self, **kwargs)
        self.test_fd_leak_after_uninstall_cluster._original(self, **kwargs)

    def test_worklog_no_after_mgmt_failover(self):
        util.print_frame()
        try:
            ret = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
            self.assertEquals( ret, 0, 'failed to TestConfMaster.initialize' )

            for i in range( 0, len( self.cluster['servers'] ) - 1 ):
                util.log( 'loop %d' % i )
                server = self.cluster['servers'][i]

                # Check last worklog number.
                leader_cm = self.cluster['servers'][i]
                sno, eno = util.worklog_info( leader_cm )
                self.assertGreaterEqual( sno, 1,
                    "Start number of worklog(%d) must be larger than or equal to 1" %
                        sno )
                self.assertGreaterEqual( eno, sno,
                    "End number of worklog(%d) must be larger than or equal to start number of worklog(%d)" %
                        (eno, sno) )

                # Shutdown leader_cm in order to elect new leader.
                util.log( 'Shutdown confmaster %s:%d' % (server['ip'], server['cm_port']) )
                self.assertEquals( 0, testbase.request_to_shutdown_cm( server ),
                                   'failed to request_to_shutdown_cm, server:%d' % server['id'] )
                time.sleep( 20 )

                # Get last worklog number of new leader.
                leader_cm = self.cluster['servers'][i+1]
                self.assertTrue( util.is_leader_cm( leader_cm ),
                    "It expects %s:%d is the leader, but it is not the leader." %
                        (leader_cm['ip'], leader_cm['cm_port']) )

                # Check laster worklog number after failover.
                n_sno, n_eno = util.worklog_info( leader_cm )
                self.assertGreaterEqual( n_sno, 1,
                    "New start number of worklog(%d) must be larger than or equal to 1" %
                        n_sno )
                self.assertGreaterEqual( n_eno, n_sno,
                    "New end number of worklog(%d) must be larger than or equal to new start number of worklog(%d)" %
                        (n_eno, n_sno) )
                self.assertGreaterEqual( n_eno, eno,
                    "New End number of worklog(%d) must be larger than or equal to old end number of worklog(%d)" %
                        (n_eno, eno) )

        finally:
            # shutdown cluster
            ret = default_cluster.finalize(self.cluster)
            self.assertEquals(ret, 0, 'failed to TestConfMaster.finalize')

    def test_no_gw_affinity_znode(self):
        util.print_frame()
        load_gens = []

        try:
            cluster = config.clusters[5]
            leader_cm = cluster['servers'][0]
            gw_servers = map(lambda s: util.deepcopy_server(s), cluster['servers'])

            ret = default_cluster.initialize_starting_up_smr_before_redis(cluster)
            self.assertEquals(ret, 0, 'failed to TestConfMaster.initialize')

            # Delete Affinity ZNODE
            util.log('Delete gateway affinity znode')
            ret = util.zk_cmd('rmr /RC/NOTIFICATION/CLUSTER/%s/AFFINITY' % cluster['cluster_name'])
            self.assertEqual(ret['exitcode'], 'OK', 'failed to remove affinity znode. ret:%s' % ret)
            time.sleep(1)

            # Start C clients
            load_gens = self.create_load_gens(cluster, leader_cm, 'singlekey')

            for i in xrange(10):
                util.log('perform some I/O to the test cluster. %d second' % (i+1))
                time.sleep(1)

            # Check OPS
            condition = (lambda s: s['ops'] > 100)
            ok = util.check_ops(cluster['servers'], 'gw', condition)
            self.assertTrue(ok, 'No request to gateways. Check whether arcci sends request to gateway, when affinity znode does not exist.')

            # Check affinity hit ratio
            ret, hit_ratio = util.get_cluster_affinity_hit_ratio(gw_servers)
            self.assertTrue(ret, '[AFFINITY_HIT_RATIO] failed to get hit ratio')
            util.log('affinity-hit-ratio:%0.3f' % hit_ratio)
            self.assertLess(hit_ratio, 0.6, '[AFFINITY_HIT_RATIO] too high affinity-hit-ratio:%0.3f' % hit_ratio)

        finally:
            self.destroy_load_gens(load_gens)

            # shutdown cluster
            ret = default_cluster.finalize(cluster)
            self.assertEquals(ret, 0, 'failed to TestConfMaster.finalize')

    def restart_gateway_with_virtual_network_info(self, cluster, leader_cm):
        # Shutdown gateways
        for s in cluster['servers']:
            ret = testbase.request_to_shutdown_gateway(cluster['cluster_name'], s, leader_cm)
            self.assertEqual(ret, 0, 'failed to shutdown gateway. gw=%s:%d' % (s['ip'], s['gateway_port']))

        # Run gateways on 127.0.0.100
        """
        +----------------------------------------------------------------+
        |                         Physical Machine                       |
        +----------------------------------------------------------------+
        |                        [Local Network IP]                      |
        |                            127.0.0.1                           |
        |                            127.0.0.100                         |
        +--------------------------------------------+-------------------+
        |                    [PGS]                   |        [GW]       |
        |     127.0.0.100  <-- local connection  <---|-+-- 127.0.0.100   |
        |                                            | |                 |
        |     127.0.0.101  <-- remote connection <---|-+                 |
        +--------------------------------------------+-------------------+
        """
        ret = util.nic_add('eth1:arc0', '127.0.0.100')
        self.assertTrue(ret, 'failed to add virtual network interface.')

        for i in xrange(0, 3):
            s = cluster['servers'][i]
            ret = testbase.request_to_start_gateway(cluster['cluster_name'], s, leader_cm)
            self.assertEqual(ret, 0, 'failed to start gateway. gw=%s:%d' % (s['ip'], s['gateway_port']))

        util.nic_del('eth1:arc0')

        # Run gateways on 127.0.0.101
        """
        +----------------------------------------------------------------+
        |                         Physical Machine                       |
        +----------------------------------------------------------------+
        |                        [Local Network IP]                      |
        |                            127.0.0.1                           |
        |                            127.0.0.101                         |
        +--------------------------------------------+-------------------+
        |                    [PGS]                   |        [GW]       |
        |     127.0.0.100  <-- remote connection <---|-+-- 127.0.0.101   |
        |                                            | |                 |
        |     127.0.0.101  <-- local connection  <---|-+                 |
        +--------------------------------------------+-------------------+
        """
        ret = util.nic_add('eth1:arc1', '127.0.0.101')
        self.assertTrue(ret, 'failed to add virtual network interface.')

        for i in xrange(3, 6):
            s = cluster['servers'][i]
            ret = testbase.request_to_start_gateway(cluster['cluster_name'], s, leader_cm)
            self.assertEqual(ret, 0, 'failed to start gateway. gw=%s:%d' % (s['ip'], s['gateway_port']))

        util.nic_del('eth1:arc1')

    def test_gw_affinity_hit_ratio(self):
        util.print_frame()
        load_gens = []

        try:
            cluster = config.clusters[9]
            leader_cm = cluster['servers'][0]
            gw_servers = map(lambda s: util.deepcopy_server(s), cluster['servers'])

            ret = default_cluster.initialize_starting_up_smr_before_redis(cluster)
            self.assertEquals(ret, 0, 'failed to TestConfMaster.initialize')

            # Set up local connection info in gateways
            self.restart_gateway_with_virtual_network_info(cluster, leader_cm)

            optypes = ['singlekey', 'range-singlekey', 'range-multikey', 'pipeline-singlekey', 'pipeline-multikey']
            for optype in optypes:
                util.log('### %s ###' % optype)
                # Start C clients
                load_gens = self.create_load_gens(cluster, leader_cm, optype)

                for i in xrange(10):
                    util.log('perform some I/O to the test cluster. %d second' % (i+1))
                    time.sleep(1)

                # Check OPS
                condition = (lambda s: s['ops'] > 100)
                ok = util.check_ops(cluster['servers'], 'gw', condition)
                self.assertTrue(ok, 'No request to gateways. Check whether arcci sends request to gateway.')

                # Check affinity hit ratio
                ret, hit_ratio = util.get_cluster_affinity_hit_ratio(gw_servers)
                self.assertTrue(ret, '[AFFINITY_HIT_RATIO] failed to get hit ratio')
                util.log('affinity-hit-ratio:%0.3f' % hit_ratio)
                if optype in ['singlekey', 'pipeline-singlekey']:
                    self.assertGreater(hit_ratio, 0.9, '[AFFINITY_HIT_RATIO] too low affinity-hit-ratio:%0.3f' % hit_ratio)
                else:
                    self.assertLess(hit_ratio, 0.6, '[AFFINITY_HIT_RATIO] too high affinity-hit-ratio:%0.3f' % hit_ratio)

                # Stop load generators
                self.destroy_load_gens(load_gens)

                # Restart Gateways in order to clear stat-info of gateways
                self.restart_gateway_with_virtual_network_info(cluster, leader_cm)

        finally:
            self.destroy_load_gens(load_gens)

            # shutdown cluster
            ret = default_cluster.finalize(cluster)
            self.assertEquals(ret, 0, 'failed to TestConfMaster.finalize')

    def test_no_stale_info_in_new_leader(self):
        util.print_frame()
        
        try:
            ret = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
            if ret is not 0:
                default_cluster.finalize( self.cluster )
            self.assertEquals( ret, 0, 'failed to TestClusteredConfigurator.initialize' )

            # Run new ConfMaster
            s = self.cluster['servers'][0]
            new_cm = {}
            new_cm['id'] = 5000
            new_cm['ip'] = s['ip']
            new_cm['zk_port'] = s['zk_port']

            ret = testbase.setup_cm(5000)
            self.assertEquals(0, ret, 'failed to copy heartbeat checker, server:%d' % new_cm['id'])

            ret = testbase.request_to_start_cm(5000, 5000)
            self.assertEquals(0, ret,
                               'failed to request_to_start_cm, server:%d' % new_cm['id'])
        
            # Remove all PGS and GW in a PM
            for s in self.cluster['servers']:
                reply = ''
                try:
                    cmd = 'pgs_leave %s %d forced' % (s['cluster_name'], s['id'])
                    reply = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
                    self.assertEquals(json.loads(reply)['state'], 'success', 'Execution fail, cmd: "%s"' % cmd)

                    cmd = 'pgs_del %s %d' % (s['cluster_name'], s['id'])
                    reply = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
                    self.assertEquals(json.loads(reply)['state'], 'success', 'Execution fail, cmd: "%s"' % cmd)

                    cmd = 'gw_del %s %d' % (s['cluster_name'], s['id'])
                    reply = util.cm_command(self.leader_cm['ip'], self.leader_cm['cm_port'], cmd)
                    self.assertEquals(json.loads(reply)['state'], 'success', 'Execution fail, cmd: "%s"' % cmd)
                except ValueError as e:
                    util.log('cmd: "%s", reply: "%s", exception: %s' % (cmd, reply, str(e)))
                    raise e

            # Shuwdown Leader-Confmaster
            for s in self.cluster['servers']:
                self.assertEquals( 0, testbase.request_to_shutdown_cm(s),
                                   'failed to request_to_shutdown_cm, server:%d' % s['id'] )

            for i in xrange(30):
                time.sleep(1)
                reply = util.cm_command(new_cm['ip'], 5000, 'cluster_ls')
                if json.loads(reply)['state'] == 'success':
                    break

            # Add PGS
            cmd = 'pgs_add %s 100 %d %s %s %d %d' % (s['cluster_name'], s['pg_id'], s['pm_name'], s['ip'], s['smr_base_port'], s['redis_port']) 
            reply = util.cm_command(new_cm['ip'], 5000, cmd)
            self.assertEquals(json.loads(reply)['state'], 'success', 'Execution fail, cmd: "%s", reply: "%s"' % (cmd, reply))

            # Add GW
            cmd = 'gw_add %s 100 %s %s %d' % (s['cluster_name'], s['pm_name'], s['ip'], s['gateway_port']) 
            reply = util.cm_command(new_cm['ip'], 5000, cmd)
            self.assertEquals(json.loads(reply)['state'], 'success', 'Execution fail, cmd: "%s", reply: "%s"' % (cmd, reply))
          
        finally:
            ret = default_cluster.finalize( self.cluster ) 
            self.assertEquals( ret, 0, 'failed to TestClusteredConfigurator.finalize' )

    def test_role_change_log_rank(self):
        util.print_frame()
        load_gen_list = {}
        try:
            ret = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
            self.assertEquals( ret, 0, 'failed to TestConfMaster.initialize' )

            # start load generator
            for i in range( len(self.cluster['servers']) ):
                server = self.cluster['servers'][i]
                load_gen = load_generator.LoadGenerator(server['id'], server['ip'], server['gateway_port'], timeout=10)
                load_gen.start()
                load_gen_list[i] = load_gen

            # decrease PG.Quorum
            ret = util.pg_dq(self.leader_cm, self.cluster['cluster_name'], self.cluster['servers'][0]['pg_id'])
            self.assertTrue(ret, 'Failed to pg_dq')

            for i in range(10):
                # get master, slave1, slave2
                m, s1, s2 = util.get_mss( self.cluster )
                self.assertNotEqual( m, None, 'master is None.' )
                self.assertNotEqual( s1, None, 'slave1 is None.' )
                self.assertNotEqual( s2, None, 'slave2 is None.' )

                util.log( 'Loop:%d, role_change, master:%d' % (i, m['id']) )
                self.role_change(m)

                for i in range(len(load_gen_list)):
                    self.assertTrue(load_gen_list[i].consistency, 'data consistency error')

        finally:
            # shutdown load generators
            for i in range(len(load_gen_list)):
                load_gen_list[i].quit()
                load_gen_list[i].join()
                load_gen_list.pop(i, None)

            # shutdown cluster
            ret = default_cluster.finalize( self.cluster )
            self.assertEquals( ret, 0, 'failed to TestConfMaster.finalize' )

    def cluster_on_off_connection_count(self):
        # initial heartbeat connections
        con_cnt = {}
        """
            con_cnt = {
                server_id : {
                    "redis" : N,
                    "smr" : N,
                    "gw" : N
                }, ...
            }
        """
        init_cons = dict((s['id'], util.connection_count(s)) for s in self.cluster['servers'])

        # cluster off
        ret = util.await(30, True)(
                lambda ret: ret == True,
                lambda : util.cluster_off(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name']))
        self.assertTrue(ret, 'Failed to cluster_off')

        # check workflows
        ret = util.await(5, False)(
                lambda cinfo: cinfo['wf'] == 0,
                lambda : util.cluster_info(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name']))
        self.assertTrue(ret, 'Failed to trigger workflows')

        # check whether heartbeat connections are closed
        for s in self.cluster['servers']:
            ret = util.await(100)(
                    util.con_compare(init_cons[s['id']], lt), 
                    util.connection_count_closure(s))
            self.assertTrue(ret, 'Heartbeat connections are not disconnected, after cluster_off.')

        # cluster on
        ret = util.cluster_on(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name'])
        self.assertTrue(ret, 'Failed to cluster_on')

        # check whether heartbeat connections are established
        for s in self.cluster['servers']:
            ret = util.await(100)(
                    util.con_compare(init_cons[s['id']], ge), 
                    util.connection_count_closure(s))
            self.assertTrue(ret, 'Heartbeat connections are not established, after cluster_on.')

    def cluster_on_off_connection_count_with_left_pgs(self, role_left):
        # initial heartbeat connections
        init_cons = dict((s['id'], util.connection_count(s)) for s in self.cluster['servers'])

        # pgs_leave
        left_pgs_list = []
        for s in self.cluster['servers']:
            pinfo = util.get_pgs_info(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name'], s['id'])
            self.assertNotEquals(pinfo, None, 'pgs_info fail')
            if pinfo['smr_Role'] == role_left:
                left_pgs_list.append(s)

        for s in left_pgs_list:
            ret = util.pgs_leave(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name'], s['id'])
            self.assertTrue(ret, 'pgs_leave fail.')

        # cluster off
        ret = util.await(30, True)(
                lambda ret: ret == True,
                lambda : util.cluster_off(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name']))
        self.assertTrue(ret, 'Failed to cluster_off')

        # check whether heartbeat connections are closed
        for s in self.cluster['servers']:
            ret = util.await(100)(
                    util.con_compare(init_cons[s['id']], lt), 
                    util.connection_count_closure(s))
            self.assertTrue(ret, 'Heartbeat connections are not disconnected, after cluster_off.')

        # cluster on
        ret = util.cluster_on(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name'])
        self.assertTrue(ret, 'Failed to cluster_on')

        for s in self.cluster['servers']:
            # check whether heartbeat connections are still closed
            if s in left_pgs_list:
                ret = util.await(100)(
                        util.con_compare(init_cons[s['id']], lt, ['gw']), 
                        util.connection_count_closure(s))
                self.assertTrue(ret, 'Heartbeat connections are not disconnected, after pgs_leave and cluster_on.')
            else:
                ret = util.await(100)(
                        util.con_compare(init_cons[s['id']], ge), 
                        util.connection_count_closure(s))
                self.assertTrue(ret, 'Heartbeat connections are not established, after cluster_on.')

        # pgs_join
        for s in left_pgs_list:
            ret = util.pgs_join(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name'], s['id'])
            self.assertTrue(ret, 'pgs_leave fail.')

        # wait for normal state of cluster
        ret = util.await(30)(
            lambda x: x,
            util.check_cluster_closure(self.cluster['cluster_name'], self.leader_cm['ip'], self.leader_cm['cm_port'], None, True))

        # check whether heartbeat connections are established
        for s in self.cluster['servers']:
            ret = util.await(100)(
                    util.con_compare(init_cons[s['id']], ge), 
                    util.connection_count_closure(s))
            self.assertTrue(ret, 'Heartbeat connections are not established, after cluster_on.')

    def cluster_on_off_failover(self):
        for s in self.cluster['servers']:
            ret = util.failover(s, self.leader_cm)
            self.assertTrue(ret, 'Failover error, incorrect state of PGS')

            ret = util.await(50)(
                lambda x: x,
                util.check_cluster_closure(self.cluster['cluster_name'], self.leader_cm['ip'], self.leader_cm['cm_port'], None, True))
            self.assertTrue(ret, 'cluster_on_off_failover, incorrect state of Cluster')

    def cluster_on_off_interleaved_fail(self):
        for s in self.cluster['servers']:
            # cluster off
            ret = util.await(30, True)(
                    lambda ret: ret == True,
                    lambda : util.cluster_off(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name']))
            self.assertTrue(ret, 'Failed to cluster_off')

            # shutdown pgs
            ret = util.shutdown_pgs(s, self.leader_cm, False)
            self.assertTrue(ret, 'Failed to shutdown pgs')

            # check workflows
            ret = util.await(5, False)(
                    lambda cinfo: cinfo['wf'] == 0,
                    lambda : util.cluster_info(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name']))
            self.assertTrue(ret, 'Failed to trigger workflows')

            # state of pgs must not be changed
            ret = util.await(10, False)(
                    lambda pgs: pgs['state'] == 'N' and pgs['color'] == 'GREEN',
                    util.get_pgs_info_closure(
                        self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name'], s['id']))
            self.assertTrue(ret, 'state of pgs must not be changed')

            # cluster on
            ret = util.cluster_on(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name'])
            self.assertTrue(ret, 'Failed to cluster_on')

            # state of pgs must be changed
            ret = util.await(50, True)(
                    lambda pgs: pgs['state'] == 'F' and pgs['color'] == 'RED',
                    util.get_pgs_info_closure(
                        self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name'], s['id']))
            self.assertTrue(ret, 'state of pgs must be changed')

            # recover pgs
            ret = util.recover_pgs(s, self.leader_cm)
            self.assertTrue(ret, 'Failed to recover pgs')

            # check_cluster state
            ret = util.await(50)(
                lambda x: x,
                util.check_cluster_closure(self.cluster['cluster_name'], self.leader_cm['ip'], self.leader_cm['cm_port'], None, True))
            self.assertTrue(ret, 'cluster_on_off_interleaved_fail, incorrect state of Cluster')

    def test_cluster_on_off(self):
        util.print_frame()
        load_gen_list = {}
        try:
            ret = default_cluster.initialize_starting_up_smr_before_redis(self.cluster)
            self.assertEquals(ret, 0, 'failed to TestConfMaster.initialize')
            
            # start load generator
            for i in range(len(self.cluster['servers'])):
                server = self.cluster['servers'][i]
                load_gen = load_generator.LoadGenerator(server['id'], server['ip'], server['gateway_port'], timeout=10)
                load_gen.start()
                load_gen_list[i] = load_gen

            print '\n\n### cluster_on_off_connection_count ###\n'
            self.cluster_on_off_connection_count()
            print '\n\n### cluster_on_off_connection_count_with_left_pgs(master) ###\n'
            self.cluster_on_off_connection_count_with_left_pgs('M')
            print '\n\n### cluster_on_off_connection_count_with_left_pgs(slave) ###\n'
            self.cluster_on_off_connection_count_with_left_pgs('S')
            print '\n\n### cluster_on_off_failover ###\n'
            self.cluster_on_off_failover()
            print '\n\n### cluster_on_off_interleaved_fail ###\n'
            self.cluster_on_off_interleaved_fail()

        finally:
            print '\n\n### release resources ###\n'

            # shutdown load generators
            for i in range(len(load_gen_list)):
                load_gen_list[i].quit()
                load_gen_list[i].join()
                load_gen_list.pop(i, None)

            # cluster_on in order to enable confmaster commands on test cluster
            cinfo = util.cluster_info(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name'])
            if cinfo != None and cinfo['cluster_info']['Mode'] == 2:
                util.cluster_on(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name'])

            # shutdown cluster
            ret = default_cluster.finalize( self.cluster )
            self.assertEquals( ret, 0, 'failed to TestConfMaster.finalize' )

    def test_migrate_mgmt_cluster(self):
        util.print_frame()

        dest_cm_ip = '127.0.0.1'
        dest_cm_port = 10010
        dest_zk_port = 10000
        zk = zookeeper.ZooKeeper(dest_zk_port)

        try:
            # install cluster
            ret = default_cluster.initialize_starting_up_smr_before_redis(self.cluster)
            self.assertEquals(ret, 0, 'failed to TestConfMaster.initialize')

            # install destination zookeeper 
            self.assertTrue(zk.start(), 'Failed to start ZooKeeper')

            # install destination confmaster
            util.copy_cm(dest_cm_port, '127.0.0.1:%d' % dest_zk_port)
            self.assertEquals(0, util.start_confmaster(dest_cm_port, dest_cm_port, ''),
                'Failed to start confmaster')

            # migrate mgmt cluster
            ret = util.migrate_mgmt_cluster(self.cluster['cluster_name'], 
                    '127.0.0.1', 1122, '127.0.0.1:2181', dest_cm_ip, dest_cm_port, '127.0.0.1:%d' % dest_zk_port)
            self.assertTrue(ret, 'Failed to migrate mgmt cluster')

            ret = util.await(8, False)(
                lambda x: x,
                util.check_cluster_closure(self.cluster['cluster_name'], dest_cm_ip, dest_cm_port, None, True))
            self.assertTrue(ret, 'after mgmt cluster migration, incorrect state of Cluster')

            # failover
            for s in self.cluster['servers']:
                dest_cm = {'ip': dest_cm_ip, 'cm_port': dest_cm_port}
                ret = util.failover(s, dest_cm)
                self.assertTrue(ret, 'Failover error, incorrect state of PGS')

                ret = util.await(50)(
                    lambda x: x,
                    util.check_cluster_closure(self.cluster['cluster_name'], dest_cm_ip, dest_cm_port, None, True))
                self.assertTrue(ret, 'after mgmt cluster migration, incorrect state of Cluster')

        finally:
            # cluster_on in order to enable confmaster commands on test cluster
            cinfo = util.cluster_info(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name'])
            if cinfo != None and cinfo['cluster_info']['Mode'] == 2:
                util.cluster_on(self.leader_cm['ip'], self.leader_cm['cm_port'], self.cluster['cluster_name'])

            # shutdown cluster
            ret = default_cluster.finalize( self.cluster )
            self.assertEquals( ret, 0, 'failed to TestConfMaster.finalize' )

            # uninstall destination confmaster
            util.shutdown_cm(dest_cm_port)

            # uninstall destination zookeeper
            zk.stop()
            zk.cleanup()

