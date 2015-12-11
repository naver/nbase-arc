#!/usr/bin/env python
import subprocess
import util
import unittest
import testbase
import default_cluster
import os
import smr_mgmt
import redis_mgmt
import time
import config
import random
import json
import constant
import ast
import copy
import sys
import threading
import crc16
import traceback
from load_generator import *
from arcci.arcci import *
from ctypes import *

MSG_GATEWAY_ADD_ZK = 'Got zookeeper node'
MSG_GATEWAY_ADD_GW_PREFIX_FMT = 'Connection [%s:%d('
MSG_GATEWAY_ADD_GW_POSTFIX = '->2'

HOST_NAME = "localhost"
HOST_IP = "127.0.0.1"
MGMT_IP = "127.0.0.1"
MGMT_PORT = 1122
ZK_ADDR = "localhost:2181"
CLUSTER_NAME = config.clusters[2]['cluster_name']
ARCCI_LOG_FILE_PREFIX = 'bin/log/arcci_log'

class MultipleARCCIThread(threading.Thread):
    def __init__(self, id, api):
        threading.Thread.__init__(self)
        self.id = id
        self.error = False
        self.total = 0
        self.api = api

    def get_error(self):
        return self.error

    def get_total(self):
        return self.total

    def run(self):
        i = 0
        while i < 1000:
            i += 1

            key = 'testkey_%d' % self.id

            rqst = self.api.create_request()
            if rqst == None:
                self.error = True
                return False

            cmd = 'set %s %s' % (key, key)
            self.api.append_command(rqst, cmd)

            cmd = 'get %s' % key
            self.api.append_command(rqst, cmd)

            ret = self.api.do_request(rqst, 2000)
            if ret != 0:
                self.error = True
                return False

            # Get reply of set
            be_errno, reply = self.api.get_reply(rqst)
            if be_errno < 0 or reply == None:
                self.error = True
                return False

            if reply[0] != ARC_REPLY_STATUS:
                self.error = True
                return False

            # Get reply of get
            be_errno, reply = self.api.get_reply(rqst)
            if be_errno < 0 or reply == None:
                self.error = True
                return False

            if reply[0] != ARC_REPLY_STRING:
                self.error = True
                return False

            if reply[1] != key:
                self.error = True
                return False

            self.total += 1

class TestARCCI(unittest.TestCase):
    cluster = config.clusters[2]
    load_gen_list = {}

    def setUp(self):
        # Initialize cluster
        util.set_process_logfile_prefix( 'TestARCCI%s' % self._testMethodName )
        ret = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
        if ret is not 0:
            default_cluster.finalize( self.cluster )
        self.assertEquals( ret, 0, 'failed to test_arcci.initialize' )

        self.GW_LIST = []
        for s in self.cluster['servers']:
            self.GW_LIST.append({"ip":s['ip'],"port":s['gateway_port']})

        return 0

    def tearDown(self):
        # Shutdown load generators
        i = 0
        max = len(self.load_gen_list)
        while i < max:
            self.load_gen_list[i].quit()
            self.load_gen_list[i].join()
            self.load_gen_list.pop(i, None)
            i += 1

        # Finalize cluster
        default_cluster.finalize( self.cluster )
        return 0

    def test_gateway_load_info_zk(self):
        util.print_frame()

        api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)
        self._gateway_load_info(api)

    def test_gateway_load_info_gw(self):
        util.print_frame()

        addrs = make_gw_addrs(self.cluster)
        api = ARC_API(None, None, gwAddrs=addrs, logFilePrefix = self.arcci_log, so_path = self.so_path)
        self._gateway_load_info(api)
    
    def _gateway_load_info(self, api):
        time.sleep(3)

        log_reader = LogReader(api.conf.log_file_prefix)

        gw_list = copy.deepcopy(self.GW_LIST)
        while True:
            line = log_reader.readline()
            if line == None: 
                break

            if line.find(MSG_GATEWAY_ADD_ZK) != -1:
                gw = line.split('data:')[1]
                gw = ast.literal_eval(gw)

                if gw not in gw_list:
                    self.fail("FAIL, unexpected gateway information. gw:%s" % util.json_to_str(gw))
                else:
                    print gw
                    gw_list.remove(gw)

                continue
            else:
                for gw in gw_list:
                    find_str = MSG_GATEWAY_ADD_GW_PREFIX_FMT % (gw['ip'], gw['port'])

                    if line.find(find_str) != -1:
                        if line.find(MSG_GATEWAY_ADD_GW_POSTFIX) != -1:
                            print gw
                            gw_list.remove(gw)
        
        if len(gw_list) != 0:
            self.fail("FAIL, load gateway information. gw:%s" % util.json_to_str(gw_list))
        else:
            util.log('SUCCESS, load gateway information.')

        api.destroy()

    def test_gateway_add_del(self):
        util.print_frame()

        api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)

        # Add gateway
        gw_port = 10000
        gw_id = 10
        cmd = 'gw_add %s %d %s %s %d' % (CLUSTER_NAME, gw_id, HOST_NAME, HOST_IP, gw_port)
        ret = util.cm_command(MGMT_IP, MGMT_PORT, cmd)
        if ret != None and len(ret) >= 2:
            ret = ret[:-2]
        util.log('cmd:"%s", ret:"%s"' % (cmd, ret))
        if not ret.startswith('{"state":"success","msg":"+OK"}'):
            self.fail('failed to add gateway')

        # Deploy gateway
        server = self.cluster['servers'][0]
        ret = util.deploy_gateway(gw_id)
        self.assertTrue(ret, 'failed to deploy_gateway')

        # Start gateway
        ret = util.start_gateway( gw_id, server['ip'], MGMT_PORT, server['cluster_name'], gw_port)
        self.assertEqual( ret, 0, 'failed : start gateawy%d' % gw_id )

        time.sleep(5)

        # Check if gateway is added
        added_gw = {"ip":HOST_IP,"port":gw_port}

        log_reader = LogReader(api.conf.log_file_prefix)
        found = False
        while True:
            line = log_reader.readline()
            if line == None: 
                break

            if line.find(MSG_GATEWAY_ADD_ZK) == -1:
                continue

            gw = line.split('data:')[1]
            gw = ast.literal_eval(gw)

            if gw['ip'] == added_gw['ip'] and gw['port'] == added_gw['port']:
                found = True
        
        if not found:
            self.fail('FAIL, load gateway information, gw:%s' % util.json_to_str(added_gw))
        else:
            util.log('SUCCESS, load gateway information.')

        # Delete gateway
        cmd = 'gw_del %s %d' % (CLUSTER_NAME, gw_id)
        ret = util.cm_command(MGMT_IP, MGMT_PORT, cmd)
        if ret != None and len(ret) >= 2:
            ret = ret[:-2]
        util.log('cmd:"%s", ret:"%s"' % (cmd, ret))
        if not ret.startswith('{"state":"success","msg":"+OK"}'):
            self.fail('failed to delete gateway')

        # Check if gateway is deleted
        deleted_gw = {"ip":HOST_IP,"port":gw_port}
        
        found = check_gateway_deleted(deleted_gw, api)
        if not found:
            self.fail('FAIL, delete gateway information, gw:%s' % util.json_to_str(deleted_gw))
        else:
            util.log('SUCCESS, delete gateway information.')

        # Stop gateway
        ret = util.shutdown_gateway(gw_id, gw_port)
        self.assertEqual(ret, 0, 'failed : shutdown gateawy%d' % gw_id)

        api.destroy()

    def test_gateway_fault_failback_zk(self):
        util.print_frame()

        # Start load generation
        self.load_gen_list = {}
        for i in range(len(self.cluster['servers'])):
            arc_api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)
            server = self.cluster['servers'][i]
            load_gen = LoadGenerator_ARCCI_FaultTolerance(server['id'], arc_api)
            load_gen.start()
            self.load_gen_list[i] = load_gen

        self._gateway_fault_failback(arc_api)

    def test_gateway_fault_failback_gw(self):
        util.print_frame()

        addrs = make_gw_addrs(self.cluster)

        # Start load generation
        self.load_gen_list = {}
        for i in range(len(self.cluster['servers'])):
            arc_api = ARC_API(None, None, gwAddrs=addrs, logFilePrefix = self.arcci_log, so_path = self.so_path)
            server = self.cluster['servers'][i]
            load_gen = LoadGenerator_ARCCI_FaultTolerance(server['id'], arc_api)
            load_gen.start()
            self.load_gen_list[i] = load_gen

        self._gateway_fault_failback(arc_api)

    def _gateway_fault_failback(self, api):
        # Set up arguments
        server = self.cluster['servers'][0]
        gw_id = server['id']
        gw_port = server['gateway_port']

        # Stop gateway
        ret = util.shutdown_gateway(gw_id, gw_port, True)
        self.assertEqual(ret, 0, 'failed : shutdown gateawy%d' % gw_id)
        time.sleep(3)

        # Check error
        saved_err_cnt = {}
        for i in range(len(self.load_gen_list)):
            saved_err_cnt[i] = 0

        no_error_cnt = 0
        for i in range(10):
            util.log('check error count loop:%d' % i)
            no_err = True
            for j in range(len(self.load_gen_list)):
                err_cnt = self.load_gen_list[j].get_err_cnt()
                if err_cnt != saved_err_cnt[j]:
                    no_err = False
                util.log('saved_err_cnt:%d, err_cnt:%d' % (saved_err_cnt[j], err_cnt))
                saved_err_cnt[j] = err_cnt

            if no_err:
                no_error_cnt += 1

            if no_error_cnt >= 3:
                break

            time.sleep(1)
        self.assertTrue(no_error_cnt >= 3, 'failed to get replys from well working gateways')

        # Check if gateway is deleted
        deleted_gw = {"ip":HOST_IP,"port":gw_port}
        
        found = check_gateway_deleted(deleted_gw, api)
        if not found:
            self.fail('FAIL, delete gateway information, gw:%s' % util.json_to_str(deleted_gw))
        else:
            util.log('SUCCESS, delete gateway information.')

        # Start gateway
        ret = util.start_gateway( gw_id, server['ip'], MGMT_PORT, server['cluster_name'], gw_port)
        self.assertEqual( ret, 0, 'failed : start gateawy%d' % gw_id )
        time.sleep(3)

        # Check if gateway is added
        found = check_gateway_added(deleted_gw, api)
        if not found:
            self.fail('FAIL, load gateway information, gw:%s' % util.json_to_str(added_gw))
        else:
            util.log('SUCCESS, load gateway information.')

        # Check loadbalancing
        for i in range(3):
            ok = True
            for s in self.cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)

        if not ok:
            self.fail('FAIL, loadbalancing,')
        else:
            util.log('SUCCESS, loadbalancing.')

    def test_gateway_upgrade(self):
        util.print_frame()

        api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)

        # Start load generation
        self.load_gen_list = {}
        for i in range(len(self.cluster['servers'])):
            arc_api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)
            server = self.cluster['servers'][i]
            load_gen = LoadGenerator_ARCCI(server['id'], arc_api, timeout_second=10)
            load_gen.start()
            self.load_gen_list[i] = load_gen

        # Set up arguments
        server = self.cluster['servers'][0]
        gw_id = server['id']
        gw_port = server['gateway_port']

        # Check load
        for i in range(5):
            ok = True
            for s in self.cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)
        self.assertTrue(ok, 'failed to send requests')

        # Delete gateway
        self.assertTrue(
                util.gw_del(CLUSTER_NAME, gw_id, MGMT_IP, MGMT_PORT), 
                'failed to delete gateway')

        # Check load
        for i in range(5):
            ok = True
            tps = util.get_tps(server['ip'], server['gateway_port'], 'gw')
            util.log('%s:%d TPS:%d' % (server['ip'], server['gateway_port'], tps))
            if tps > 10:
                ok = False

            if ok:
                break

            time.sleep(1)
        self.assertTrue(ok, 'failed to send requests')

        # Stop gateway
        ret = util.shutdown_gateway(gw_id, gw_port, True)
        self.assertEqual(ret, 0, 'failed : shutdown gateawy%d' % gw_id)

        # Check if gateway is deleted
        deleted_gw = {"ip":HOST_IP,"port":gw_port}
        
        found = check_gateway_deleted(deleted_gw, api)
        if not found:
            self.fail('FAIL, delete gateway information, gw:%s' % util.json_to_str(deleted_gw))
        else:
            util.log('SUCCESS, delete gateway information.')

        # Start gateway
        ret = util.start_gateway( gw_id, server['ip'], MGMT_PORT, server['cluster_name'], gw_port)
        self.assertEqual( ret, 0, 'failed : start gateawy%d' % gw_id )
        time.sleep(3)

        # Add gateway
        self.assertTrue(
                util.gw_add(CLUSTER_NAME, gw_id, HOST_NAME, HOST_IP, gw_port, MGMT_IP, MGMT_PORT), 
                'failed to add gateway')

        # Check if gateway is added
        added_gw = {"ip":HOST_IP,"port":gw_port}

        log_reader = LogReader(api.conf.log_file_prefix)
        found = False
        while True:
            line = log_reader.readline()
            if line == None: 
                break

            if line.find(MSG_GATEWAY_ADD_ZK) == -1:
                continue

            gw = line.split('data:')[1]
            gw = ast.literal_eval(gw)

            if gw['ip'] == added_gw['ip'] and gw['port'] == added_gw['port']:
                found = True
        
        if not found:
            self.fail('FAIL, load gateway information, gw:%s' % util.json_to_str(added_gw))
        else:
            util.log('SUCCESS, load gateway information.')

        # Check loadbalancing
        for i in range(5):
            ok = True
            for s in self.cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)

        if not ok:
            self.fail('FAIL, loadbalancing,')
        else:
            util.log('SUCCESS, loadbalancing.')

        api.destroy()

    def test_zookeeper_failback(self):
        util.print_frame()

        try:
            api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)

            # Start load generation
            self.load_gen_list = {}
            for i in range(len(self.cluster['servers'])):
                arc_api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)
                server = self.cluster['servers'][i]
                load_gen = LoadGenerator_ARCCI(server['id'], arc_api, timeout_second=10)
                load_gen.start()
                self.load_gen_list[i] = load_gen

            # Check load
            for i in range(5):
                ok = True
                for s in self.cluster['servers']:
                    tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                    util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                    if tps < 50:
                        ok = False

                if ok:
                    break

                time.sleep(1)
            self.assertTrue(ok, 'failed to send requests')

            # Stop zookeeper
            stdout, returncode = util.stop_zookeeper(config.zookeeper_info[0]['bin_dir'])
            util.log("zookeeper stop - stdout:%s" % stdout)
            self.assertEqual(returncode, 0, 'failed to stop zookeeper')
            time.sleep(1)

            # Check loadbalancing
            for i in range(5):
                ok = True
                for s in self.cluster['servers']:
                    tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                    util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                    if tps < 50:
                        ok = False

                time.sleep(1)

            if not ok:
                self.fail('FAIL, loadbalancing,')
            else:
                util.log('SUCCESS, loadbalancing.')

        finally:
            # Start zookeeper
            stdout, returncode = util.start_zookeeper(config.zookeeper_info[0]['bin_dir'])
            util.log("zookeeper start - stdout:%s" % stdout)
            self.assertEqual(returncode, 0, 'failed to stop zookeeper')
            time.sleep(1)

            # Check loadbalancing
            for i in range(5):
                ok = True
                for s in self.cluster['servers']:
                    tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                    util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                    if tps < 50:
                        ok = False

                if ok:
                    break

                time.sleep(1)

            if not ok:
                self.fail('FAIL, loadbalancing,')
            else:
                util.log('SUCCESS, loadbalancing.')

            api.destroy()

    def test_zookeeper_ensemble_failback(self):
        util.print_frame()

        try:
            api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)

            # Start load generation
            self.load_gen_list = {}
            for i in range(len(self.cluster['servers'])):
                arc_api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)
                server = self.cluster['servers'][i]
                load_gen = LoadGenerator_ARCCI(server['id'], arc_api, timeout_second=10)
                load_gen.start()
                self.load_gen_list[i] = load_gen

            # Check load
            for i in range(5):
                ok = True
                for s in self.cluster['servers']:
                    tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                    util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                    if tps < 50:
                        ok = False

                if ok:
                    break

                time.sleep(1)
            self.assertTrue(ok, 'failed to send requests')

            # Stop zookeeper ensemble
            for zk in config.zookeeper_info:
                stdout, returncode = util.stop_zookeeper(zk['bin_dir'])
                util.log("zookeeper stop - stdout:%s" % stdout)
                self.assertEqual(returncode, 0, 'failed to stop zookeeper')
            time.sleep(1)

            # Check loadbalancing
            for i in range(5):
                ok = True
                for s in self.cluster['servers']:
                    tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                    util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                    if tps < 50:
                        ok = False

                time.sleep(1)

            if not ok:
                self.fail('FAIL, loadbalancing,')
            else:
                util.log('SUCCESS, loadbalancing.')

        finally:
            # Start zookeeper ensemble
            for zk in config.zookeeper_info:
                stdout, returncode = util.start_zookeeper(zk['bin_dir'])
                util.log("zookeeper start - stdout:%s" % stdout)
                self.assertEqual(returncode, 0, 'failed to stop zookeeper')
            time.sleep(1)

            # Check loadbalancing
            for i in range(5):
                ok = True
                for s in self.cluster['servers']:
                    tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                    util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                    if tps < 50:
                        ok = False

                if ok:
                    break

                time.sleep(1)

            if not ok:
                self.fail('FAIL, loadbalancing,')
            else:
                util.log('SUCCESS, loadbalancing.')

            api.destroy()

    def test_zookeeper_delete_root_of_gw_znodes(self):
        util.print_frame()

        # Start load generation
        self.load_gen_list = {}
        arc_api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)
        server = self.cluster['servers'][0]
        load_gen = LoadGenerator_ARCCI(server['id'], arc_api, timeout_second=10, verbose=True)
        load_gen.start()
        self.load_gen_list[0] = load_gen

        # Set up arguments
        server = self.cluster['servers'][0]
        gw_id = server['id']
        gw_port = server['gateway_port']

        # Check load
        for i in range(5):
            ok = True
            for s in self.cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)
        self.assertTrue(ok, 'failed to send requests')

        # Delete root of GW znodes
        print 'try remove root of GW znodes'
        ret = util.zk_cmd('rmr /RC/NOTIFICATION/CLUSTER/%s/GW' % server['cluster_name'])
        ret = ret['err']
        self.assertEqual(ret, '', 'failed to remove root of GW znodes, ret:%s' % ret)

        # Check loadbalancing
        for i in range(10):
            ok = True
            for s in self.cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps > 10:
                    ok = False

            if ok:
                break

            time.sleep(1)

        if not ok:
            self.fail('FAIL, loadbalancing,')
        else:
            util.log('SUCCESS, loadbalancing.')

        # Recover root of GW znodes
        print 'try recover GW znodes'
        ret = util.zk_cmd('create /RC/NOTIFICATION/CLUSTER/%s/GW test' % server['cluster_name'])
        ret = ret['err']
        self.assertNotEqual(ret.find('Created /RC/NOTIFICATION/CLUSTER/testCluster0/GW'), -1, 
                'failed to create root of GW znodes, ret:%s' % ret)
        for s in self.cluster['servers']:
            path = '/RC/NOTIFICATION/CLUSTER/%s/GW/%d' % (s['cluster_name'], s['id'])
            cmd = 'create %s \'{"ip":"%s","port":%d}\'' % (path, s['ip'], s['gateway_port'])
            print cmd
            ret = util.zk_cmd(cmd)
            ret = ret['err']
            self.assertNotEqual(ret.find('Created %s' % path), -1, 'failed to recover GW znode, ret:%s' % ret)

        # Check loadbalancing
        for i in range(10):
            ok = True
            for s in self.cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)

        if not ok:
            self.fail('FAIL, loadbalancing,')
        else:
            util.log('SUCCESS, loadbalancing.')


    def test_gateway_network_isolation(self):
        util.print_frame()

        cluster = self.cluster

        # Clear rules
        while True:
            out = util.sudo('iptables -t nat -D OUTPUT -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
            util.log(out)
            if out.succeeded == False:
                break

        while True:
            out = util.sudo('iptables -t nat -D PREROUTING -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
            util.log(out)
            if out.succeeded == False:
                break

        while True:
            out = util.sudo('iptables -D OUTPUT -d 127.0.0.100 -j DROP')
            util.log(out)
            if out.succeeded == False:
                break

        # Print rules
        out = util.sudo('iptables -L')
        util.log('====================================================================')
        util.log(out.succeeded)
        util.log('out : %s' % out)
        util.log('out.return_code : %d' % out.return_code)
        util.log('out.stderr : %s' % out.stderr)

        # Start loadgenerators
        self.load_gen_list = {}
        for i in range(len(cluster['servers'])):
            arc_api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)
            server = cluster['servers'][i]
            load_gen = LoadGenerator_ARCCI_FaultTolerance(server['id'], arc_api)
            load_gen.start()
            self.load_gen_list[i] = load_gen

        # Add forwarding role (127.0.0.100 -> 127.0.0.1)
        out = util.sudo('iptables -t nat -A OUTPUT -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)

        out = util.sudo('iptables -t nat -A PREROUTING -d 127.0.0.100 -p tcp -j DNAT --to-destination 127.0.0.1')
        self.assertTrue(out.succeeded, 'add a forwarding role to iptables fail. output:%s' % out)

        # Add virtualhost information to MGMT
        VIRTUAL_HOST_NAME = 'virtualhost'
        VIRTUAL_HOST_IP = '127.0.0.100'
        ret = util.pm_add(VIRTUAL_HOST_NAME, VIRTUAL_HOST_IP, MGMT_IP, MGMT_PORT)
        self.assertTrue(ret, 'pm_add fail.')

        # Modify gateway information of MGMT
        server = cluster['servers'][0]
        gw_id = server['id']
        gw_port = server['gateway_port']

        # Delete gateway
        ret = util.gw_del(CLUSTER_NAME, gw_id, MGMT_IP, MGMT_PORT)
        self.assertTrue(ret, 'gw_del fail')

        # Add gateway
        ret= util.gw_add(CLUSTER_NAME, gw_id, VIRTUAL_HOST_NAME, VIRTUAL_HOST_IP, gw_port, MGMT_IP, MGMT_PORT) 
        self.assertTrue(ret, 'gw_add fail')

        # Check load balancing
        for i in range(5):
            ok = True
            for s in cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)

        self.assertTrue(ok, 'load balancing fail')
        util.log('load balancing success')

        # Block
        out = util.sudo('iptables -A OUTPUT -d 127.0.0.100 -j DROP')
        self.assertTrue(out.succeeded, 'add a bloking role to iptables fail. output:%s' % out)

        # Check blocked gateway`s ops
        for i in range(5):
            ok = True
            tps = util.get_tps(server['ip'], server['gateway_port'], 'gw')
            util.log('%s:%d TPS:%d' % (server['ip'], server['gateway_port'], tps))
            if tps > 10:
                ok = False

            if ok:
                break

            time.sleep(1)

        self.assertTrue(ok, 'load balancing fail - blocked gateway')
        util.log('load balancing success - blocked gateway')

        # Check unblocked gateway`s ops
        for i in range(10):
            ok = True
            for s in cluster['servers']:
                if s == server:
                    continue

                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)

        self.assertTrue(ok, 'load balancing fail - nonblocked gateways')
        util.log('load balancing success - nonblocked gateways')

        # Unblock
        out = util.sudo('iptables -D OUTPUT -d 127.0.0.100 -j DROP')
        self.assertTrue(out.succeeded, 'delete a bloking role to iptables fail. output:%s' % out)

        # Check load balancing
        ok = False
        for i in xrange(5):
            condition = (lambda s: (s['ops'] <= 10 if s['id'] == gw_id else s['ops'] >= 50))
            if util.check_ops(cluster['servers'], 'gw', condition):
                ok = True
                break
            time.sleep(1)

        self.assertTrue(ok, 'load balancing fail - all gateways after unblocking network')
        util.log('load balancing success - all gateways after unblocking network')

        server = cluster['servers'][0]

        # Wait until opinion for the gateway deleted.
        for i in xrange(5):
            util.log('Wait until opinions for the gateway have been deleted... %d' % i)
            time.sleep(1)

        # Delete gateway
        ret = util.gw_del(CLUSTER_NAME, gw_id, MGMT_IP, MGMT_PORT)
        self.assertTrue(ret, 'gw_del fail')

        # Add gateway
        ret= util.gw_add(CLUSTER_NAME, gw_id, server['pm_name'], server['ip'], gw_port, MGMT_IP, MGMT_PORT) 
        self.assertTrue(ret, 'gw_add fail')

        # Check load balancing
        for i in range(10):
            ok = True
            for s in cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)

        self.assertTrue(ok, 'load balancing fail - all gateways after unblocking network')
        util.log('load balancing success - all gateways after unblocking network')

    def test_a_pg_delay(self):
        util.print_frame()

        # Start load generation
        self.load_gen_list = {}
        for i in range(len(self.cluster['servers'])):
            arc_api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)
            server = self.cluster['servers'][i]
            load_gen = LoadGenerator_ARCCI_FaultTolerance(server['id'], arc_api)
            load_gen.start()
            self.load_gen_list[i] = load_gen

        # Set up arguments
        server = self.cluster['servers'][0]
        gw_id = server['id']
        gw_port = server['gateway_port']

        # Check load
        for i in range(5):
            ok = True
            for s in self.cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)
        self.assertTrue(ok, 'failed to send requests')

        # HANG SMR
        smr = smr_mgmt.SMR(server['id'])
        ret = smr.connect(server['ip'], server['smr_mgmt_port'])
        self.assertEqual(ret, 0, 'failed to connect to master. %s:%d' % (server['ip'], server['smr_mgmt_port']) )
        smr.write( 'fi delay sleep 1 5000\r\n' )
        reply = smr.read_until( '\r\n', 1 )
        if reply != None and reply.find('-ERR not supported') != -1:
            self.assertEqual( 0, 1, 'make sure that smr has compiled with gcov option.' )
        smr.disconnect()

        # Check load
        i = 0
        while i < 10:
            i += 1
            ok = True
            for s in self.cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)

        if not ok:
            self.fail('FAIL, loadbalancing, while HANG')
        else:
            util.log('SUCCESS, loadbalancing, while HANG')

        for id, load_gen in self.load_gen_list.items():
            util.log('err_cnt:%d' % load_gen.get_err_cnt())

    def test_pipelining_and_join(self):
        util.print_frame()

        api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)

        i = 0;
        while i < 1000:
            i += 1

            rqst = api.create_request()
            self.assertNotEqual(rqst, None, 'failed to create_request')

            # Set
            j = 0
            while j < 30:
                j += 1
                api.append_command(rqst, "set %d_%d %d_%d" % (i, j, i, j))

            ret = api.do_request(rqst, 3000)
            self.assertEqual(ret, 0, 'failed to do_request, ret:%d' % ret)

            j = 0
            while j:
                j += 1
                be_errno, reply = api.get_reply(rqst)
                self.assertEqual(be_errno, 0, 'failed to get_reply, be_errno:%d' % be_errno)
                self.assertNotEqual(reply, None, 'failed to get_reply, reply is None')
                self.assertEqual(reply[0], ARC_REPLY_STATUS, 'failed to get_reply, reply[0]:%d' % reply[0])

            api.free_request(rqst)

            # Get
            rqst = api.create_request()
            self.assertNotEqual(rqst, None, 'failed to create_request')

            j = 0
            while j < 30:
                j += 1
                api.append_command(rqst, "get %d_%d" % (i, j))

            ret = api.do_request(rqst, 3000)
            self.assertEqual(ret, 0, 'failed to do_request, ret:%d' % ret)

            j = 0
            while j:
                j += 1

                be_errno, reply = api.get_reply(rqst)
                self.assertEqual(be_errno, 0, 'failed to get_reply, be_errno:%d' % be_errno)
                self.assertNotEqual(reply, None, 'failed to get_reply, reply is None')
                self.assertEqual(reply[0], ARC_REPLY_STRING, 'failed to get_reply, reply[0]:%d' % reply[0])
                self.assertEqual(reply[1], '%d_%d' % (i, j), 'failed to get_reply, value:%s' % reply[1])

            api.free_request(rqst)
            
    def test_timeout_in_partial_PG(self):
        util.print_frame()

        api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)

        rqst = api.create_request()
        if rqst == None:
            return False

        # Request - PG 0
        api.append_command(rqst, "set haha0 haha0")
        api.append_command(rqst, "set haha2 haha2")
        dummy_value = ''
        for i in range(10): # 1 KB
            dummy_value += '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789'
        large_value_for_timeout  = ''
        for i in range(1024): # 1 MB
            large_value_for_timeout += dummy_value
        large_value_for_timeout += large_value_for_timeout # 2 MB
        large_value_for_timeout += large_value_for_timeout # 4 MB
        large_value_for_timeout += large_value_for_timeout # 8 MB
        large_value_for_timeout += large_value_for_timeout # 16 MB
        large_value_for_timeout += large_value_for_timeout # 32 MG
        large_value_for_timeout += large_value_for_timeout # 64 MB
        large_value_for_timeout += large_value_for_timeout # 128 MB
        large_value_for_timeout += large_value_for_timeout # 256 MB
        util.log('Large value : %f MB' % (float(len(large_value_for_timeout))/1024/1024))
        total_cmd = 3
        api.append_command(rqst, "set haha %s", large_value_for_timeout)

        # Request - PG 1
        sent_for_pg1 = []
        for i in range(10):
            key = 'hahahoho%d' % i
            slot = crc16.crc16_buff(key) % 8192
            total_cmd += 1
            api.append_command(rqst, "set %s %s" % (key, key))

            if slot > 4095:
                sent_for_pg1.append(key)

        for i in range(6):
            util.log('waiting... %d' % i)
            time.sleep(1)

        total_reply = 0
        err_cnt = 0
        try:
            ret = api.do_request(rqst, 1000)
            if ret != 0:
                err_cnt += 1
                util.log('Partial error occurs')

            util.log('Reply : ')
            while True:
                be_errno, reply = api.get_reply(rqst)
                if be_errno < 0 or reply == None:
                    util.log('be_errno : %d' % be_errno)
                    break

                total_reply += 1
                util.log(reply)
                if reply is not None:
                    if reply[0] != ARC_REPLY_STATUS:
                        err_cnt += 1
                    elif reply[0] != ARC_REPLY_ERROR:
                        err_cnt += 1
                    elif reply[0] != ARC_REPLY_NIL:
                        err_cnt += 1

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)

        finally:
            util.log('total cmd:%d' % total_cmd)
            util.log('total reply:%d' % total_reply)
            api.destroy()

        self.assertEquals(be_errno, ARC_ERR_TIMEOUT, 'could not get timeout error. be_errno:%d' % be_errno)

    def test_multiple_arcci(self):
        util.print_frame()

        arc_api_list = []
        i = 0
        while i < 3:
            i += 1
            util.log('create arc_api using zk')
            arc_api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)
            arc_api_list.append(arc_api)

        addrs = make_gw_addrs(self.cluster)
        i = 0
        while i < 3:
            i += 1
            util.log('create arc_api using gw')
            arc_api = ARC_API(None, None, gwAddrs=addrs, logFilePrefix = self.arcci_log, so_path = self.so_path)
            arc_api_list.append(arc_api)

        operators = []
        util.log('start operations')
        for i in range(len(arc_api_list)):
            operator = MultipleARCCIThread(i, arc_api_list[i])
            operator.start()
            operators.append(operator)

        for operator in operators:
            operator.join()

        for arc_api in arc_api_list:
            arc_api.destroy()

        util.log('check result')
        ok = True
        for operator in operators:
            util.log('total:%d, error:%d' % (operator.get_total(), operator.get_error()))
            if operator.get_error():
                ok = False

        self.assertTrue(ok, 'fail, error occurs while using multiple arc_apis')

    def test_no_error_scenario_and_memory_leak(self):
        util.print_frame()

        # Start with valgrind
        p = util.exec_proc_async('%s/.obj%d' % (constant.ARCCI_DIR, self.arch),
                "valgrind ./dummy-perf -z localhost:2181 -c %s -n 5 -s 60" % self.cluster['cluster_name'], 
                subprocess.PIPE, subprocess.PIPE, subprocess.PIPE);

        # Set up arguments
        server = self.cluster['servers'][0]
        gw_id = server['id']
        gw_port = server['gateway_port']

        # Check load
        for i in range(5):
            ok = True
            for s in self.cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)
        self.assertTrue(ok, 'failed to send requests')

        # Delete gateway
        self.assertTrue(
                util.gw_del(CLUSTER_NAME, gw_id, MGMT_IP, MGMT_PORT), 
                'failed to delete gateway')

        # Check load
        for i in range(5):
            ok = True
            tps = util.get_tps(server['ip'], server['gateway_port'], 'gw')
            util.log('%s:%d TPS:%d' % (server['ip'], server['gateway_port'], tps))
            if tps > 10:
                ok = False

            if ok:
                break

            time.sleep(1)
        self.assertTrue(ok, 'failed to send requests')

        # Stop gateway
        ret = util.shutdown_gateway(gw_id, gw_port, True)
        self.assertEqual(ret, 0, 'failed : shutdown gateawy%d' % gw_id)
        time.sleep(5)

        # Start gateway
        ret = util.start_gateway( gw_id, server['ip'], MGMT_PORT, server['cluster_name'], gw_port)
        self.assertEqual( ret, 0, 'failed : start gateawy%d' % gw_id )
        time.sleep(3)

        # Add gateway
        self.assertTrue(
                util.gw_add(CLUSTER_NAME, gw_id, HOST_NAME, HOST_IP, gw_port, MGMT_IP, MGMT_PORT), 
                'failed to add gateway')
        time.sleep(10)

        # Check loadbalancing
        for i in range(5):
            ok = True
            for s in self.cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)

        if not ok:
            self.fail('FAIL, loadbalancing,')
        else:
            util.log('SUCCESS, loadbalancing.')

        # Check no error
        no_memory_leak = False
        (stdout, stderr) = p.communicate()
        for line in stdout.split("\n"):
            print line

            if line.find('[ERR]') != -1:
                self.fail('find error, msg:%s' % line)

            if line.find('All heap blocks were freed -- no leaks are possible'):
                no_memory_leak = True

        self.assertTrue(no_memory_leak, 'memory leaks are possible')
        util.log('no leaks are possible')

    def test_argument_chekcing(self):
        util.print_frame()

        # ARC_API
        # Invalid zookeeper address
        api = ARC_API('invalidaddr:2181', CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)
        self.assertEqual(api.arc.value, None, 'fail, call create_zk_new with invalid zookeeper address')
        api.destroy()

        # Invalid  cluster name
        api = ARC_API(ZK_ADDR, 'invalid_cluster_name', logFilePrefix = self.arcci_log, so_path = self.so_path)
        self.assertEqual(api.arc.value, None, 'fail, call create_zk_new with invalid cluster name')
        api.destroy()

        # Null arguments
        api = ARC_API(None, None, logFilePrefix = self.arcci_log, so_path = self.so_path)
        self.assertEqual(api.arc.value, None, 'fail, call create_zk_new with invalid zookeeper address and invalid cluster name')
        api.destroy()

        api = ARC_API(None, None, gwAddrs=None, logFilePrefix = self.arcci_log, so_path = self.so_path)
        self.assertEqual(api.arc.value, None, 'fail, call crate_gw_new with invalid gateways')
        api.destroy()

        # [BEGIN] Mixed : invalid gateway address and valid gateway address
        s0 = self.cluster['servers'][0]
        s1 = self.cluster['servers'][1]
        servers = [s0, s1]
        s_no_load = self.cluster['servers'][2]

        gw_addrs = "%s:%d,%s:%d,123.123.123.123:8200" % (s0['ip'], s0['gateway_port'], s1['ip'], s1['gateway_port'])

        api = ARC_API(None, None, gwAddrs=gw_addrs, logFilePrefix = self.arcci_log, so_path = self.so_path)
        self.assertNotEqual(api.arc.value, None, 'fail, call create_zk_new with mixed gateway addresses')

        # Start load generation
        self.load_gen_list = {}
        load_gen = LoadGenerator_ARCCI_FaultTolerance(0, api)
        load_gen.start()
        self.load_gen_list[0] = load_gen

        # Check loadbalancing
        for i in range(5):
            ok = True
            for s in servers:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
                if tps < 50:
                    ok = False

            if ok:
                break

            time.sleep(1)

        if not ok:
            self.fail('FAIL, loadbalancing,')
        else:
            util.log('SUCCESS, loadbalancing.')

        tps = util.get_tps(s_no_load['ip'], s_no_load['gateway_port'], 'gw')
        util.log('%s:%d TPS:%d' % (s_no_load['ip'], s_no_load['gateway_port'], tps))
        self.assertTrue(tps < 10, 'FAIL, loadbalancing,')

        # close
        max = len(self.load_gen_list)
        i = 0
        while i < max:
            self.load_gen_list[i].quit()
            self.load_gen_list[i].join()
            self.load_gen_list.pop(i, None)
            i += 1
        # [END] Mixed : invalid gateway address and valid gateway address

        # valid
        api = ARC_API(ZK_ADDR, CLUSTER_NAME, logFilePrefix = self.arcci_log, so_path = self.so_path)
        self.assertNotEqual(api.arc.value, None, 'fail, call create_zk_new with valid arguments')

        rqst = api.create_request()
        self.assertNotEqual(rqst, None, 'fail, call create_request')

        # append_command 
        ret = api.append_command(rqst, None, None)
        self.assertEqual(ret, -1, 'fail, call append_command with Null format and Null args.') 

        ret = api.append_command(rqst, None, 'hahahoho')
        self.assertEqual(ret, -1, 'fail, call append_command with Null format')

        # valid set operation
        ret = api.append_command(rqst, 'set haha %s', 'hoho')
        self.assertEqual(ret, 0, 'fail, call append_command with valid arguments')

        ret = api.append_command(rqst, 'set %s %d', 'hoho', 1)
        self.assertEqual(ret, 0, 'fail call append command with valid agruments.')

        ret = api.append_command(rqst, 'set %d %s', 1, 'hoho')
        self.assertEqual(ret, 0, 'fail call append command with valid agruments.')

        ret = api.append_command(rqst, 'set %d %d', 2, 2)
        self.assertEqual(ret, 0, 'fail call append command with valid agruments.')

        ret = api.append_command(rqst, 'mset %d %d %s %s %f %f', 2, 2, c_char_p('mset0'), c_char_p('mset0'), c_double(1.23), c_double(1.234))
        self.assertEqual(ret, 0, 'fail call append command with valid agruments.')

        ret = api.do_request(rqst, 3000)
        self.assertEqual(ret, 0, 'arguments checking fail.')

        i = 0
        while i < 5:
            i += 1
            be_errno, reply = api.get_reply(rqst)
            self.assertEqual(be_errno, 0, 'arguments checking fail.')
            self.assertEqual(reply[0], ARC_REPLY_STATUS)

        api.free_request(rqst)

        # valid get operation
        rqst = api.create_request()
        ret = api.append_command(rqst, 'mget %d %s %f', 2, 'mset0', c_double(1.23))
        self.assertEqual(ret, 0, 'fail call append command with valid agruments.')

        ret = api.do_request(rqst, 3000)
        self.assertEqual(ret, 0, 'arguments checking fail.')

        be_errno, reply = api.get_reply(rqst)
        self.assertEqual(be_errno, 0, 'arguments checking fail.')
        self.assertEqual(reply[0], ARC_REPLY_ARRAY)
        self.assertEqual(reply[1], [(4, '2'), (4, 'mset0'), (4, '1.234000')])

        api.free_request(rqst)

        api.destroy()

    def test_error_log(self):
        util.print_frame()

        cluster = self.cluster

        # Start test-fiall
        p = util.exec_proc_async('%s/.obj%d' % (constant.ARCCI_DIR, self.arch),
                "./test-fiall -z localhost:2181 -c %s -s 10" % cluster['cluster_name'], 
                subprocess.PIPE, subprocess.PIPE, subprocess.PIPE);

        # Set up arguments
        server = cluster['servers'][0]
        gw_id = server['id']
        gw_port = server['gateway_port']

        # Check load
        for i in range(20):
            ok = True
            for s in cluster['servers']:
                tps = util.get_tps(s['ip'], s['gateway_port'], 'gw')
                util.log('%s:%d TPS:%d' % (s['ip'], s['gateway_port'], tps))
            time.sleep(1)

        # Check no error
        util.log(' ### BEGIN - ARCCI LOGS ### ')
        (stdout, stderr) = p.communicate()
        for line in stdout.split("\n"):
            util.log(line)
        util.log(' ### END - ARCCI LOGS ### ')

