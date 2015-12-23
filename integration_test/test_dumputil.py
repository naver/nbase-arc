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

import base64
import string
import random
import unittest
import default_cluster
import telnetlib
import config
import util
import time
import json
import subprocess
import crc16

BASE32    = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567'
BASE32HEX = '0123456789ABCDEFGHIJKLMNOPQRSTUV'
TRANS = string.maketrans(BASE32HEX, BASE32)

class TestDumpUtil(unittest.TestCase):
    cluster = config.clusters[0]

    def setUp(self):
        util.set_process_logfile_prefix('TestDumpUtil_%s' % self._testMethodName)
        if default_cluster.initialize_starting_up_smr_before_redis(self.cluster) is not 0:
            util.log('failed to TestDumpUtil.initialize')
            return -1
        return 0

    def tearDown(self):
        if default_cluster.finalize(self.cluster) is not 0:
            util.log('failed to TestDumpUtil.finalize')
        return 0

    def b32hexdecode(self, s):
        s = s.encode('ascii')
        s = s.upper()
        base32 = string.translate(s, TRANS)
        return base64.b32decode(base32)

    #def string_gen(self, size=6, chars=string.printable):
    def string_gen(self, size=6, chars=string.ascii_letters):
        return ''.join(random.choice(chars) for x in range(size))

    def bgsave(self, redis):
        redis.write('time\r\n')
        redis.read_until('\r\n', 1)
        redis.read_until('\r\n', 1)
        ret = redis.read_until('\r\n', 1)
        before_save_time = int(ret.strip())
        redis.read_until('\r\n', 1)
        redis.read_until('\r\n', 1)

        time.sleep(1.1)

        redis.write('bgsave\r\n')
        ret = redis.read_until('\r\n', 1)
        self.assertEqual(ret, '+Background saving started\r\n')

        # Wait finishing bgsave
        while True:
            redis.write('lastsave\r\n')
            ret = redis.read_until('\r\n', 1)
            lastsave_time = int(ret[1:].strip())
            if lastsave_time > before_save_time: break
            time.sleep(0.1)

    def testbase32hex_conversion(self):
        util.print_frame()

        count = 100
        dict = {}

        server0 = self.cluster['servers'][0]
        redis0 = telnetlib.Telnet(server0['ip'], server0['redis_port'])

        while count > 0:
            count -= 1;
            key = self.string_gen(random.randint(1,1000))
            val = self.string_gen(random.randint(1,1000))
            dict[key] = val
            redis0.write('*3\r\n$3\r\nset\r\n')
            redis0.write('$%d\r\n%s\r\n' % (len(key), key))
            redis0.write('$%d\r\n%s\r\n' % (len(val), val))
            ret = redis0.read_until('\r\n', 1)
            self.assertEqual(ret, '+OK\r\n')

        self.bgsave(redis0)

        cmd = "./dump-util --dump-iterator dump.rdb ./dump2json_base32hex.so out.json"
        p = util.exec_proc_async(util.dump_util_dir(0), cmd, True, None, subprocess.PIPE, None)

        ret = p.wait()
        self.assertTrue(ret == 0);

        f = file("%s/out.json" % util.dump_util_dir(0), "r")
        skip_line = 2
        for line in f.readlines():
            # skip first 2 lines (smr_seqnum, smr_mstime)
            if skip_line > 0:
                skip_line -= 1
                continue

            line = line.strip()
            key = self.b32hexdecode(json.loads(line)['key'])
            val = self.b32hexdecode(json.loads(line)['value'])

            self.assertTrue(key in dict.keys(), 'key(%s) is not in json output' % key)
            self.assertEqual(dict[key], val,
                             "val(%s) is not match with %s" % (dict[key], val))

        f.close()


    def test_data_type(self):
        util.print_frame()

        dict = {}
        server0 = self.cluster['servers'][0]
        redis0 = telnetlib.Telnet(server0['ip'], server0['redis_port'])

        # String
        dict['string'] = {}
        key = self.string_gen(random.randint(1,5)) + '_type_string'
        val = self.string_gen(random.randint(1,5))
        dict['string']['key'] = key
        dict['string']['val'] = val

        redis0.write('*3\r\n$3\r\nset\r\n')
        redis0.write('$%d\r\n%s\r\n' % (len(key), key))
        redis0.write('$%d\r\n%s\r\n' % (len(val), val))
        ret = redis0.read_until('\r\n', 1)
        self.assertEqual(ret, '+OK\r\n')

        # List
        dict['list'] = {}
        key = self.string_gen(random.randint(1,5)) + '_type_list'
        val1 = self.string_gen(random.randint(1,5))
        val2 = self.string_gen(random.randint(1,5))
        dict['list']['key'] = key
        dict['list']['val1'] = val1
        dict['list']['val2'] = val1 # Duplicate value
        dict['list']['val3'] = val2

        redis0.write('*5\r\n$5\r\nrpush\r\n')
        redis0.write('$%d\r\n%s\r\n' % (len(key), key))
        redis0.write('$%d\r\n%s\r\n' % (len(val1), val1))
        redis0.write('$%d\r\n%s\r\n' % (len(val1), val1))
        redis0.write('$%d\r\n%s\r\n' % (len(val2), val2))
        ret = redis0.read_until('\r\n', 1)
        self.assertEqual(ret, ':3\r\n')

        # Set
        dict['set'] = {}
        key = self.string_gen(random.randint(1,5)) + '_type_set'
        val1 = self.string_gen(random.randint(1,5)) + '_v1'
        val2 = self.string_gen(random.randint(1,5)) + '_v2'
        dict['set']['key'] = key
        dict['set']['val1'] = val1
        dict['set']['val2'] = val2

        redis0.write('*4\r\n$4\r\nsadd\r\n')
        redis0.write('$%d\r\n%s\r\n' % (len(key), key))
        redis0.write('$%d\r\n%s\r\n' % (len(val1), val1))
        redis0.write('$%d\r\n%s\r\n' % (len(val2), val2))
        ret = redis0.read_until('\r\n', 1)
        self.assertEqual(ret, ':2\r\n')

        # Sorted Set
        dict['zset'] = {}
        key = self.string_gen(random.randint(1,5)) + '_type_zset'
        val1 = self.string_gen(random.randint(1,5)) + '_v1'
        val2 = self.string_gen(random.randint(1,5)) + '_v2'
        dict['zset']['key'] = key
        dict['zset']['val1'] = val1
        dict['zset']['score1'] = 20
        dict['zset']['val2'] = val2
        dict['zset']['score2'] = 10

        redis0.write('*6\r\n$4\r\nzadd\r\n')
        redis0.write('$%d\r\n%s\r\n' % (len(key), key))
        redis0.write('$2\r\n20\r\n$%d\r\n%s\r\n' % (len(val1), val1))
        redis0.write('$2\r\n10\r\n$%d\r\n%s\r\n' % (len(val2), val2))
        ret = redis0.read_until('\r\n', 1)
        self.assertEqual(ret, ':2\r\n')

        # Hash
        dict['hash'] = {}
        key = self.string_gen(random.randint(1,5)) + '_type_hash'
        key1 = self.string_gen(random.randint(1,5)) + '_k1'
        val1 = self.string_gen(random.randint(1,5))
        key2 = self.string_gen(random.randint(1,5)) + '_k2'
        val2 = self.string_gen(random.randint(1,5))
        dict['hash']['key'] = key
        dict['hash'][key1] = val1
        dict['hash'][key2] = val2

        redis0.write('*6\r\n$5\r\nhmset\r\n')
        redis0.write('$%d\r\n%s\r\n' % (len(key), key))
        redis0.write('$%d\r\n%s\r\n' % (len(key1), key1))
        redis0.write('$%d\r\n%s\r\n' % (len(val1), val1))
        redis0.write('$%d\r\n%s\r\n' % (len(key2), key2))
        redis0.write('$%d\r\n%s\r\n' % (len(val2), val2))
        ret = redis0.read_until('\r\n', 1)
        self.assertEqual(ret, '+OK\r\n')

        self.bgsave(redis0)

        cmd = "./dump-util --dump-iterator dump.rdb ./dump2json_base32hex.so out.json"
        p = util.exec_proc_async(util.dump_util_dir(0), cmd, True, None, subprocess.PIPE, None)

        ret = p.wait()
        self.assertTrue(ret == 0)

        f = file("%s/out.json" % util.dump_util_dir(0), "r")
        skip_line = 2
        for line in f.readlines():
            # skip first 2 lines (smr_seqnum, smr_mstime)
            if skip_line > 0:
                skip_line -= 1
                continue

            data = json.loads(line.strip())
            key = self.b32hexdecode(data['key'])

            if data['type'] == 'string':
                self.assertEqual(dict['string']['key'], key,
                                 "key(%s) is not match with %s" % (dict['string']['key'], key))

                val = self.b32hexdecode(data['value'])

                self.assertEqual(dict['string']['val'], val,
                                 "val(%s) is not match with %s" % (dict['string']['val'], val))

            elif data['type'] == 'list':
                self.assertEqual(dict['list']['key'], key,
                                 "key(%s) is not match with %s" % (dict['list']['key'], key))

                val1 = self.b32hexdecode(data['value'][0])
                val2 = self.b32hexdecode(data['value'][1])
                val3 = self.b32hexdecode(data['value'][2])

                self.assertEqual(dict['list']['val1'], val1,
                                 "val(%s) is not match with %s" % (dict['list']['val1'], val1))
                self.assertEqual(dict['list']['val2'], val2,
                                 "val(%s) is not match with %s" % (dict['list']['val2'], val2))
                self.assertEqual(dict['list']['val3'], val3,
                                 "val(%s) is not match with %s" % (dict['list']['val3'], val3))

            elif data['type'] == 'set':
                self.assertEqual(dict['set']['key'], key,
                                 "key(%s) is not match with %s" % (dict['set']['key'], key))

                val1 = self.b32hexdecode(data['value'][0])
                val2 = self.b32hexdecode(data['value'][1])
                if not (val1 == dict['set']['val1'] and val2 == dict['set']['val2']
                    or val1 == dict['set']['val2'] and val2 == dict['set']['val1']):

                    util.log("values(%s, %s) is not match with (%s, %s)" % (dict['set']['val1'],
                                                                            dict['set']['val2'],
                                                                            val1,
                                                                            val2))
                    self.assertTrue(False)

            elif data['type'] == 'zset':
                self.assertEqual(dict['zset']['key'], key,
                                 "key(%s) is not match with %s" % (dict['zset']['key'], key))

                # Set variable as sort order
                val2 = self.b32hexdecode(data['value'][0]['data'])
                score2 = int(data['value'][0]['score'])
                val1 = self.b32hexdecode(data['value'][1]['data'])
                score1 = int(data['value'][1]['score'])

                self.assertEqual(dict['zset']['val1'], val1,
                                 "val(%s) is not match with %s" % (dict['zset']['val1'], val1))
                self.assertEqual(dict['zset']['score1'], score1,
                                 "score(%d) is not match with %d" % (dict['zset']['score1'], score1))
                self.assertEqual(dict['zset']['val2'], val2,
                                 "val(%s) is not match with %s" % (dict['zset']['val2'], val2))
                self.assertEqual(dict['zset']['score2'], score2,
                                 "score(%d) is not match with %d" % (dict['zset']['score2'], score2))

            elif data['type'] == 'hash':
                self.assertEqual(dict['hash']['key'], key,
                                 "key(%s) is not match with %s" % (dict['zset']['key'], key))

                key1 = self.b32hexdecode(data['value'][0]['hkey'])
                val1 = self.b32hexdecode(data['value'][0]['hval'])
                key2 = self.b32hexdecode(data['value'][1]['hkey'])
                val2 = self.b32hexdecode(data['value'][1]['hval'])

                self.assertTrue(key1 in dict['hash'].keys(), 'hkey(%s) is not in json output' % key1)
                self.assertTrue(key2 in dict['hash'].keys(), 'hkey(%s) is not in json output' % key2)
                self.assertEqual(dict['hash'][key1], val1,
                                 "val(%s) is not match with %s" % (dict['hash'][key1], val1))
                self.assertEqual(dict['hash'][key2], val2,
                                 "val(%s) is not match with %s" % (dict['hash'][key2], val2))

            else:
                self.assertTrue(False, "Unknown type")

        f.close()

    def get_redis_curtime(self, redis):
        redis.write('time\r\n')
        redis.read_until('\r\n')
        redis.read_until('\r\n')
        cur_time = int(redis.read_until('\r\n')[:-2]) + 1
        redis.read_until('\r\n')
        redis.read_until('\r\n')
        return cur_time

    def timedump_and_make_json_output(self, target_time):
        cmd = "./dump-util --dump %d ../smr0/log0 . out.rdb" % target_time
        p = util.exec_proc_async(util.dump_util_dir(0), cmd, True, None, subprocess.PIPE, None)
        ret = p.wait()
        self.assertTrue(ret == 0, p.stdout.readlines())

        cmd = "./dump-util --dump-iterator out.rdb ./dump2json_base32hex.so out.json"
        p = util.exec_proc_async(util.dump_util_dir(0), cmd, True, None, subprocess.PIPE, None)
        ret = p.wait()
        self.assertTrue(ret == 0, p.stdout.readlines())

    def is_key_exists_in_json_file(self, key, json_file):
        found = False

        f = open(json_file)
        for line in f.readlines():
            data = json.loads(line.strip())
            if self.b32hexdecode(data['key']) == key:
                found = True
        f.close()
        return found

    def print_file(self, json_file):
        f = open(json_file)
        for line in f.readlines():
            print line.strip()
        f.close()

    def test_timedump_with_expire(self):
        util.print_frame()

        server0 = self.cluster['servers'][0]
        redis0 = telnetlib.Telnet(server0['ip'], server0['redis_port'])
        json_file = "%s/out.json" % util.dump_util_dir(0)

        redis0.write('setex key1 5 value\r\n')
        ret = redis0.read_until('\r\n')

        self.bgsave(redis0)

        curtime_1 = self.get_redis_curtime(redis0)
        time.sleep(7)

        redis0.write('setex key2 5 value\r\n')
        ret = redis0.read_until('\r\n')

        curtime_2 = self.get_redis_curtime(redis0)
        time.sleep(7)

        redis0.write('setex key3 5 value\r\n')
        ret = redis0.read_until('\r\n')
        curtime_3 = self.get_redis_curtime(redis0)

        print 'currtime_1:%d currtime_2:%d currtime_3:%d ' % (curtime_1, curtime_2, curtime_3)
        self.timedump_and_make_json_output(curtime_1)
        self.print_file(json_file)
        self.assertTrue(self.is_key_exists_in_json_file('key1', json_file))
        self.assertFalse(self.is_key_exists_in_json_file('key2', json_file))
        self.assertFalse(self.is_key_exists_in_json_file('key3', json_file))

        self.timedump_and_make_json_output(curtime_2)
        self.print_file(json_file)
        self.assertFalse(self.is_key_exists_in_json_file('key1', json_file))
        self.assertTrue(self.is_key_exists_in_json_file('key2', json_file))
        self.assertFalse(self.is_key_exists_in_json_file('key3', json_file))

        self.timedump_and_make_json_output(curtime_3)
        self.print_file(json_file)
        self.assertFalse(self.is_key_exists_in_json_file('key1', json_file))
        self.assertFalse(self.is_key_exists_in_json_file('key2', json_file))
        self.assertTrue(self.is_key_exists_in_json_file('key3', json_file))

    def migstart(self, redis, range_from, range_to):
        cmd = 'migconf migstart %s-%s\r\n' % (range_from, range_to)
        redis.write(cmd)
        ret = redis.read_until('\r\n', 1)
        self.assertEqual(ret, '+OK\r\n')

    def clearstart(self, redis, range_from, range_to):
        cmd = 'migconf clearstart %s-%s\r\n' % (range_from, range_to)
        redis.write(cmd)
        ret = redis.read_until('\r\n', 1)
        self.assertEqual(ret, '+OK\r\n')

    def test_dump_iterator_with_mig_conf_migstart(self):
        util.print_frame()

        num_test = 100
        dict = {}
        server0 = self.cluster['servers'][0]
        redis0 = telnetlib.Telnet(server0['ip'], server0['redis_port'])

        for i in xrange(num_test):
            key = self.string_gen(random.randint(1, 64))
            val = self.string_gen(random.randint(1, 64))
            dict[key] = val

            redis0.write('*3\r\n$3\r\nset\r\n')
            redis0.write('$%d\r\n%s\r\n' % (len(key), key))
            redis0.write('$%d\r\n%s\r\n' % (len(val), val))
            ret = redis0.read_until('\r\n', 1)
            self.assertEqual(ret, '+OK\r\n')

        self.migstart(redis0, 0, 4095)

        self.bgsave(redis0)

        cmd = "./dump-util --dump-iterator dump.rdb ./dump2json_base32hex.so out.json"
        p = util.exec_proc_async(util.dump_util_dir(0), cmd, True, None, subprocess.PIPE, None)
        ret = p.wait()
        self.assertTrue(ret == 0)

        count = 0
        f = file("%s/out.json" % util.dump_util_dir(0), "r")
        for line in f.readlines():
            count += 1
            data = json.loads(line.strip())
            key = self.b32hexdecode(data['key'])
            val = self.b32hexdecode(data['value'])
            self.assertEqual(dict[key], val)
            if ((crc16.crc16_buff(key, 0) % 8192) < 4096):
                print key
                print val
                print crc16.crc16_buff(key, 0) % 8192
                self.assertTrue(False, "dump-util doesn't recognize keys on migration")

        print "Total Count of json output = %d" % count
        f.close()

    def test_dump_iterator_with_mig_conf_clearstart(self):
        util.print_frame()

        num_test = 100
        dict = {}
        server0 = self.cluster['servers'][0]
        redis0 = telnetlib.Telnet(server0['ip'], server0['redis_port'])

        for i in xrange(num_test):
            key = self.string_gen(random.randint(1, 64))
            val = self.string_gen(random.randint(1, 64))
            dict[key] = val

            redis0.write('*3\r\n$3\r\nset\r\n')
            redis0.write('$%d\r\n%s\r\n' % (len(key), key))
            redis0.write('$%d\r\n%s\r\n' % (len(val), val))
            ret = redis0.read_until('\r\n', 1)
            self.assertEqual(ret, '+OK\r\n')

        self.clearstart(redis0, 4096, 8191)

        self.bgsave(redis0)

        cmd = "./dump-util --dump-iterator dump.rdb ./dump2json_base32hex.so out.json"
        p = util.exec_proc_async(util.dump_util_dir(0), cmd, True, None, subprocess.PIPE, None)
        ret = p.wait()
        self.assertTrue(ret == 0)

        count = 0
        f = file("%s/out.json" % util.dump_util_dir(0), "r")
        for line in f.readlines():
            count += 1
            data = json.loads(line.strip())
            key = self.b32hexdecode(data['key'])
            val = self.b32hexdecode(data['value'])
            self.assertEqual(dict[key], val)
            if ((crc16.crc16_buff(key, 0) % 8192) >= 4096):
                print key
                print val
                print crc16.crc16_buff(key, 0) % 8192
                self.assertTrue(False, "dump-util doesn't recognize keys on migration")

        print "Total Count of json output = %d" % count
        f.close()
