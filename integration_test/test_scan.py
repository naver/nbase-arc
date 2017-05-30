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
import unittest
import config
import default_cluster
import string
import random

class TestScan(unittest.TestCase):
    cluster = config.clusters[2]

    def setUp(self):
        util.set_process_logfile_prefix('TestScan_%s' % self._testMethodName)
        if default_cluster.initialize_starting_up_smr_before_redis(self.cluster) is not 0:
            util.log('failed to TestScan.initialize')
            return -1
        return 0

    def tearDown(self):
        if default_cluster.finalize(self.cluster) is not 0:
            util.log('failed to TestScan.finalize')
        return 0

    def string_gen(self, size=6, chars=string.ascii_letters):
        return ''.join(random.choice(chars) for x in range(size))

    def check_scan(self, servers, insert_fn, scan_fn):
        # Insert values for test
        expect = insert_fn(servers[0])

        # Test SCAN
        cursor_order_prev = None
        cursor_order_now = None
        for i in range(3):
            result, cursor_order_now = scan_fn(servers)

            # Check cursor order
            if cursor_order_prev != None:
                self.assertTrue(all(i == j for i, j in zip(cursor_order_prev, cursor_order_now)))
            cursor_order_prev = cursor_order_now
            cursor_order_now = None

            # Check scan result
            unmatched_item  = set(expect.items()) ^ set(result.items())
            if len(unmatched_item) != 0:
                util.log("SCAN unmatched items = " + repr(unmatched_item))
                self.assertTrue(len(unmatched_item) == 0)

    def insert_key(self, svr, count = 10000):
        expect = {}
        for i in range(count):
            key = self.string_gen()
            svr.write('SET %s scan_test_value\r\n' % key)
            svr.read_until('\r\n', 3)
            expect[key] = 'scan_test_value'
        return expect

    def scan_key(self, servers):
        result = {}
        cursor_order = []
        cursor = 0
        while True:
            svr = random.choice(servers)
            svr.write('SCAN %d\r\n' % cursor)
            svr.write('PING\r\n')
            ret = svr.read_until('+PONG\r\n', 3)
            sp = ret.split('\r\n')

            for i in range(5, len(sp)-1, 2):
                key = sp[i]
                value = 'scan_test_value'
                result[key] = value

            cursor = int(sp[2])
            cursor_order.append(cursor)
            if cursor == 0:
                break
        return result, cursor_order

    def cscan_key(self, servers, pg_idx):
        result = {}
        cursor_order = []
        cursor = 0
        while True:
            svr = random.choice(servers)
            svr.write('CSCAN %d %d\r\n' % (pg_idx, cursor))
            svr.write('PING\r\n')
            ret = svr.read_until('+PONG\r\n', 3)
            sp = ret.split('\r\n')

            for i in range(5, len(sp)-1, 2):
                key = sp[i]
                value = 'scan_test_value'
                result[key] = value

            cursor = int(sp[2])
            cursor_order.append(cursor)
            if cursor == 0:
                break
        return result, cursor_order

    def insert_hash(self, svr, count = 10000):
        svr.write('del hscantest\r\n')
        svr.read_until('\r\n', 3)

        expect = {}
        for i in range(count):
            key = self.string_gen()
            value = "Value"
            svr.write('HSET hscantest %s %s\r\n' % (key, value))
            svr.read_until('\r\n', 3)
            expect[key] = value
        return expect

    def scan_hash(self, servers):
        result = {}
        cursor_order = []
        cursor = 0
        while True:
            svr = random.choice(servers)
            svr.write('HSCAN hscantest %d\r\n' % cursor)
            svr.write('PING\r\n')
            ret = svr.read_until('+PONG\r\n', 3)
            sp = ret.split('\r\n')

            for i in range(5, len(sp)-1, 4):
                key = sp[i]
                value = "Value"
                result[key] = value

            cursor = int(sp[2])
            cursor_order.append(cursor)
            if cursor == 0:
                break
        return result, cursor_order

    def insert_zset(self, svr, count = 10000):
        svr.write('del zscantest\r\n')
        svr.read_until('\r\n', 3)

        expect = {}
        for i in range(count):
            key = self.string_gen()
            score = i
            svr.write('ZADD zscantest %d %s\r\n' % (score, key))
            svr.read_until('\r\n', 3)
            expect[key] = score
        return expect

    def scan_zset(self, servers):
        result = {}
        cursor_order = []
        cursor = 0
        while True:
            svr = random.choice(servers)
            svr.write('ZSCAN zscantest %d\r\n' % cursor)
            svr.write('PING\r\n')
            ret = svr.read_until('+PONG\r\n', 3)
            sp = ret.split('\r\n')

            for i in range(5, len(sp)-1, 4):
                key = sp[i]
                score = int(sp[i+2])
                result[key] = score

            cursor = int(sp[2])
            cursor_order.append(cursor)
            if cursor == 0:
                break
        return result, cursor_order

    def insert_set(self, svr, count = 10000):
        svr.write('del sscantest\r\n')
        svr.read_until('\r\n', 3)

        expect = {}
        for i in range(count):
            key = self.string_gen()
            svr.write('SADD sscantest %s\r\n' % key)
            svr.read_until('\r\n', 3)
            expect[key] = 'setValue'
        return expect

    def scan_set(self, servers):
        result = {}
        cursor_order = []
        cursor = 0
        while True:
            svr = random.choice(servers)
            svr.write('SSCAN sscantest %d\r\n' % cursor)
            svr.write('PING\r\n')
            ret = svr.read_until('+PONG\r\n', 3)
            sp = ret.split('\r\n')

            for i in range(5, len(sp)-1, 2):
                key = sp[i]
                result[key] = 'setValue'

            cursor = int(sp[2])
            cursor_order.append(cursor)
            if cursor == 0:
                break
        return result, cursor_order

    def test_scan_gateway(self):
        util.print_frame()

        # Test scan through gateway
        gateway_list = []
        for server in self.cluster['servers']:
            gateway_list.append(telnetlib.Telnet(server['ip'], server['gateway_port']))

        util.log("run SCAN test through gateway")
        self.check_scan(gateway_list, self.insert_key, self.scan_key)
        util.log("run HSCAN test through gateway")
        self.check_scan(gateway_list, self.insert_hash, self.scan_hash)
        util.log("run ZSCAN test through gateway")
        self.check_scan(gateway_list, self.insert_zset, self.scan_zset)
        util.log("run SSCAN test through gateway")
        self.check_scan(gateway_list, self.insert_set, self.scan_set)

    def test_scan_redis(self):
        util.print_frame()

        # Test scan through redis
        redis_list = []
        for i in range(3):
            server = self.cluster['servers'][i]
            redis_list.append(telnetlib.Telnet(server['ip'], server['redis_port']))

        util.log("run HSCAN test through redis")
        self.check_scan(redis_list, self.insert_hash, self.scan_hash)
        util.log("run ZSCAN test through redis")
        self.check_scan(redis_list, self.insert_zset, self.scan_zset)
        util.log("run SSCAN test through redis")
        self.check_scan(redis_list, self.insert_set, self.scan_set)

    def test_cscan(self):
        util.print_frame()

        gateway_list = []
        for server in self.cluster['servers']:
            gateway_list.append(telnetlib.Telnet(server['ip'], server['gateway_port']))

        util.log("run CSCAN test")

        svr = random.choice(gateway_list)
        expect = self.insert_key(svr)
        svr.write('CSCANLEN\r\n')
        cscanlen = int(svr.read_until('\r\n', 3)[1:])

        total = {}
        for i in range(cscanlen):
            result, cursor_order = self.cscan_key(gateway_list, i)
            total = dict(total.items() + result.items())

        # Check scan result
        unmatched_item  = set(expect.items()) ^ set(total.items())
        if len(unmatched_item) != 0:
            util.log("CSCAN unmatched items = " + repr(unmatched_item))
            self.assertTrue(len(unmatched_item) == 0)

    def test_cdigest(self):
        util.print_frame()

        gateway_list = []
        for server in self.cluster['servers']:
            gateway_list.append(telnetlib.Telnet(server['ip'], server['gateway_port']))

        util.log("run CSCAN test")

        svr = random.choice(gateway_list)
        expect = self.insert_key(svr)

        svr.write('CDIGEST\r\n')
        svr.read_until('\r\n', 3)
        digest1 = svr.read_until('\r\n', 3)

        ret = util.migration(self.cluster, 0, 1, 0, 4095, 40000)
        self.assertEqual(True, ret, 'Migration Fail')

        svr.write('CDIGEST\r\n')
        svr.read_until('\r\n', 3)
        digest2 = svr.read_until('\r\n', 3)

        ret = util.migration(self.cluster, 1, 0, 0, 4095, 40000)
        self.assertEqual(True, ret, 'Migration Fail')

        svr.write('CDIGEST\r\n')
        svr.read_until('\r\n', 3)
        digest3 = svr.read_until('\r\n', 3)

        self.assertEqual(digest1, digest3, "Incompatible Cluster Digest")
        self.assertNotEqual(digest1, digest2, "Incompatible Cluster Digest")
