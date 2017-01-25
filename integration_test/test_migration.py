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
import random
import json

class TestMigration(unittest.TestCase):
    cluster = config.clusters[1]
    max_load_generator = 128

    def isExist(self, redis, key):
        cmd = 'get %s\r\n' % key
        redis.write(cmd)
        res = redis.read_until('\r\n')
        if res == '$7\r\n':
            res = redis.read_until('\r\n')
            self.assertEquals( res, 'migtest\r\n' )
            return True

        self.assertEquals( res, '$-1\r\n' )
        return False

    def isS3Exist(self, redis, key):
        cmd = 's3lget ks %s svc key\r\n' % key
        redis.write(cmd)
        res = redis.read_until('\r\n')
        if res == '*1\r\n':
            res = redis.read_until('\r\n')
            res = redis.read_until('\r\n')
            self.assertEquals( res, 'value\r\n' )
            return True

        self.assertEquals( res, '*0\r\n' )
        return False

    def persistKey(self, redis, key):
        cmd = 'ping\r\n'
        redis.write(cmd)
        res = redis.read_until('\r\n')
        self.assertEquals( res, '+PONG\r\n' )

        util.log("persist key %s" % key)
        cmd = 'persist %s\r\n' % key
        redis.write(cmd)
        res = redis.read_until('\r\n')
        if (res != ':1\r\n' and res != ':0\r\n'):
            self.assertFalse()
        return res

    def persistS3Key(self, redis, key):
        cmd = 'ping\r\n'
        redis.write(cmd)
        res = redis.read_until('\r\n')
        self.assertEquals( res, '+PONG\r\n' )

        util.log("persist s3 key %s" % key)
        cmd = 's3lexpire ks %s 0 svc key value\r\n' % key
        redis.write(cmd)
        res = redis.read_until('\r\n')
        if (res != ':1\r\n' and res != ':0\r\n'):
            self.assertFalse()
        return res

    def setExpireKey(self, redis, key, ex_sec):
        util.log("set expire key %s, %dsec" % (key, ex_sec))
        cmd = 'setex %s %d migtest\r\n' % (key, ex_sec)
        redis.write(cmd)
        res = redis.read_until('\r\n')
        self.assertEquals( res, '+OK\r\n' )

    def setExpireS3Key(self, redis, key, ex_sec):
        util.log("set expire s3 key %s, %dsec" % (key, ex_sec))
        cmd = 's3ladd ks %s svc key value %d \r\n' % (key, ex_sec*1000)
        redis.write(cmd)
        res = redis.read_until('\r\n')
        self.assertEquals( res, ':1\r\n' )

    def getS3TTL(self, redis, key):
        util.log("get ttl s3 key %s" % key)
        cmd = 's3lttl ks %s svc key value\r\n' % key
        redis.write(cmd)
        res = redis.read_until('\r\n')
        if (res != ':0\r\n'):
            return False
        return True

    @classmethod
    def setUpClass( cls ):
        return 0

    @classmethod
    def tearDownClass( cls ):
        return 0

    def setUp(self):
        util.set_process_logfile_prefix( 'TestMigration_%s' % self._testMethodName )
        ret = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
        if ret is not 0:
            default_cluster.finalize( self.cluster )
        return 0

    def tearDown(self):
        default_cluster.finalize( self.cluster )
        return 0

    def test_migrate_all(self):
        util.print_frame()
        migration_count = 10
        # start load generator
        load_gen_thrd_list = {}
        util.log("start load_generator")
        for i in range(self.max_load_generator):
            ip, port = util.get_rand_gateway(self.cluster)
            load_gen_thrd_list[i] = load_generator.LoadGenerator(i, ip, port)
            load_gen_thrd_list[i].start()

        time.sleep(5) # generate load for 5 sec

        # start migration
        for i in range(migration_count):
            # pg0 -> pg1
            ret = util.migration(self.cluster, 0, 1, 4096, 8191, 40000)
            self.assertEqual(True, ret, 'Migration Fail')
            # pg0 <- pg1
            ret = util.migration(self.cluster, 1, 0, 4096, 8191, 40000)
            self.assertEqual(True, ret, 'Migration Fail')

            ok = True
            for j in range(len(load_gen_thrd_list)):
                if load_gen_thrd_list[j].isConsistent() == False:
                    ok = False
                    break
            if not ok:
                break;

        time.sleep(5) # generate load for 5 sec
        # check consistency of load_generator
        for i in range(len(load_gen_thrd_list)):
            load_gen_thrd_list[i].quit()
        for i in range(len(load_gen_thrd_list)):
            load_gen_thrd_list[i].join()
            self.assertTrue(load_gen_thrd_list[i].isConsistent(), 'Inconsistent after migration')

    def test_random_migrate(self):
        util.print_frame()

        # start load generator
        load_gen_thrd_list = {}
        util.log("start load_generator")
        for i in range(self.max_load_generator):
            ip, port = util.get_rand_gateway(self.cluster)
            load_gen_thrd_list[i] = load_generator.LoadGenerator(i, ip, port)
            load_gen_thrd_list[i].start()

        ret = util.migration(self.cluster, 0, 1, 4096, 8191, 40000)
        self.assertEqual(True, ret, 'Migration Fail')

        leader_cm = self.cluster['servers'][0]
        cluster_name = self.cluster['cluster_name']
        mapping = [-1] * 8192

        count = 50
        while count > 0:
            # get PN -> PG map
            cmd = 'cluster_info %s' % cluster_name
            result = util.cm_command(leader_cm['ip'], leader_cm['cm_port'], cmd)
            ret = json.loads(result)
            rle = ret['data']['cluster_info']['PN_PG_Map']
            print "PN_PG_MAP = %s" % rle
            sp = rle.split()
            index = 0
            for i in range(len(sp)/2):
                for j in range(int(sp[i*2+1])):
                    mapping[index] = int(sp[i*2])
                    index += 1

            slot = random.randint(0, 8191)
            src_pgid = mapping[slot]
            dst_pgid = (src_pgid+1) % 2
            slot_end = slot
            while random.randint(0,5) <= 4:
                if slot_end < 8191 and mapping[slot_end+1] == src_pgid:
                    slot_end += 1
                else:
                    break

            print "SLOT=%d, SRC_PGID=%d, DST_PGID=%d" % (slot, src_pgid, dst_pgid)
            ret = util.migration(self.cluster, src_pgid, dst_pgid, slot, slot_end, 40000)
            self.assertEqual(True, ret, 'Migration Fail')

            ok = True
            for j in range(len(load_gen_thrd_list)):
                if load_gen_thrd_list[j].isConsistent() == False:
                    ok = False
                    break
            if not ok:
                break;

            count -= 1;

        # check consistency of load_generator
        for i in range(len(load_gen_thrd_list)):
            load_gen_thrd_list[i].quit()
        for i in range(len(load_gen_thrd_list)):
            load_gen_thrd_list[i].join()
            self.assertTrue(load_gen_thrd_list[i].isConsistent(), 'Inconsistent after migration')

        # Go back to initial configuration
        cinfo = util.cluster_info(leader_cm['ip'], leader_cm['cm_port'], cluster_name)
        for slot in util.get_slots(cinfo['cluster_info']['PN_PG_Map'], 1):
            self.assertTrue(util.migration(self.cluster, 1, 0, slot['begin'], slot['end'], 40000),
                    'failed to rollback migration')

    def test_migration_with_expire_command(self):
        util.print_frame()

        util.log("start load_generator")
        load_gen_thrd_list = {}
        for i in range(1):
            ip, port = util.get_rand_gateway(self.cluster)
            load_gen_thrd_list[i] = load_generator.LoadGenerator(i, ip, port)
            load_gen_thrd_list[i].start()

        time.sleep(5) # generate load for 5 sec
        tps = 20000
        src_pg_id = 0
        dst_pg_id = 1
        leader_cm = self.cluster['servers'][0]
        src_master = util.get_server_by_role_and_pg(self.cluster['servers'], 'master', src_pg_id)
        dst_master = util.get_server_by_role_and_pg(self.cluster['servers'], 'master', dst_pg_id)

        smr = smr_mgmt.SMR(src_master['id'])
        ret = smr.connect(src_master['ip'], src_master['smr_mgmt_port'])
        if ret != 0:
            util.log('failed to connect to smr(source master)')
            return False

        src_redis = redis_mgmt.Redis(src_master['id'])
        ret = src_redis.connect(src_master['ip'], src_master['redis_port'] )
        self.assertEquals( ret, 0, 'failed to connect to redis' )

        dst_redis = redis_mgmt.Redis(dst_master['id'])
        ret = dst_redis.connect(dst_master['ip'], dst_master['redis_port'] )
        self.assertEquals( ret, 0, 'failed to connect to redis' )

        ts = time.time()
        self.setExpireKey(src_redis, 'beforeCheckpoint~beforeCheckpoint:expired', 10)
        self.setExpireKey(src_redis, 'beforeCheckpoint~beforeCheckpoint:persist', 20)
        self.setExpireS3Key(src_redis, 'S3:beforeCheckpoint~beforeCheckpoint:expired', 10)
        self.setExpireS3Key(src_redis, 'S3:beforeCheckpoint~beforeCheckpoint:persist', 20)

        self.setExpireS3Key(src_redis, 'S3:PermanentKey', 0)

        util.log(">>> sleep until 15 sec pass")
        self.assertFalse(time.time() - ts >= 15)
        time.sleep(15 - (time.time() - ts))

        res = self.persistKey(src_redis, 'beforeCheckpoint~beforeCheckpoint:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistKey(src_redis, 'beforeCheckpoint~beforeCheckpoint:expired')
        self.assertEquals(res, ":0\r\n")
        res = self.persistS3Key(src_redis, 'S3:beforeCheckpoint~beforeCheckpoint:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistS3Key(src_redis, 'S3:beforeCheckpoint~beforeCheckpoint:expired')
        self.assertEquals(res, ":0\r\n")

        util.log(">>> migrate test with expire command start(%s), ts:%d" % (time.asctime(), ts))

        ts = time.time()
        self.setExpireKey(src_redis, 'beforeCheckpoint~afterCheckpoint:expired', 10)
        self.setExpireKey(src_redis, 'beforeCheckpoint~afterCheckpoint:persist', 20)
        self.setExpireS3Key(src_redis, 'S3:beforeCheckpoint~afterCheckpoint:expired', 10)
        self.setExpireS3Key(src_redis, 'S3:beforeCheckpoint~afterCheckpoint:persist', 20)

        # notify dst_redis of migration start
        util.log(">>> notify dst_redis of migration start (%s)" % time.asctime())

        cmd = 'migconf migstart %d-%d\r\n' % (0, 8191)
        dst_redis.write(cmd)
        res = dst_redis.read_until('\r\n')
        self.assertEquals( res, '+OK\r\n' )

        # remote partial checkpoint
        util.log(">>> start remote checkpoint and load (%s)" % time.asctime())
        cmd = "./cluster-util --getandplay %s %d %s %d %d-%d %d" % (
                    src_master['ip'], src_master['redis_port'],
                    dst_master['ip'], dst_master['redis_port'],
                    0, 8191, tps)
        p = util.exec_proc_async(util.cluster_util_dir(src_master['id']), cmd, True, None, subprocess.PIPE, None)

        ret = p.wait()
        for line in p.stdout:
            if line.find("Checkpoint Sequence Number:") != -1:
                util.log("seqnumber : " + line[line.rfind(":")+1:])
                seq = int(line[line.rfind(":")+1:])
            util.log(">>>" + str(line.rstrip()))

        self.assertEqual(0, ret)
        util.log(">>> end remote checkpoint and load (%s)" % time.asctime())

        util.log(">>> sleep until 15 sec pass")
        self.assertFalse(time.time() - ts >= 15)
        time.sleep(15 - (time.time() - ts))

        res = self.persistKey(src_redis, 'beforeCheckpoint~afterCheckpoint:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistKey(src_redis, 'beforeCheckpoint~afterCheckpoint:expired')
        self.assertEquals(res, ":0\r\n")
        res = self.persistS3Key(src_redis, 'S3:beforeCheckpoint~afterCheckpoint:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistS3Key(src_redis, 'S3:beforeCheckpoint~afterCheckpoint:expired')
        self.assertEquals(res, ":0\r\n")

        # bgsave for testing later about recovery during migration
        util.log(">>> bgsave for testing later about recovery during migration (%s)" % time.asctime())
        cmd = 'bgsave\r\n'
        dst_redis.write(cmd)
        res = dst_redis.read_until('\r\n')
        self.assertEquals( res, '+Background saving started\r\n' )

        ts = time.time()
        self.setExpireKey(src_redis, 'afterCheckpoint~afterCheckpoint:expired', 10)
        self.setExpireKey(src_redis, 'afterCheckpoint~afterCheckpoint:persist', 20)
        self.setExpireS3Key(src_redis, 'S3:afterCheckpoint~afterCheckpoint:expired', 10)
        self.setExpireS3Key(src_redis, 'S3:afterCheckpoint~afterCheckpoint:persist', 20)

        util.log(">>> sleep until 15 sec pass")
        self.assertFalse(time.time() - ts >= 15)
        time.sleep(15 - (time.time() - ts))

        res = self.persistKey(src_redis, 'afterCheckpoint~afterCheckpoint:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistKey(src_redis, 'afterCheckpoint~afterCheckpoint:expired')
        self.assertEquals(res, ":0\r\n")
        res = self.persistS3Key(src_redis, 'S3:afterCheckpoint~afterCheckpoint:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistS3Key(src_redis, 'S3:afterCheckpoint~afterCheckpoint:expired')
        self.assertEquals(res, ":0\r\n")

        ts = time.time()
        self.setExpireKey(src_redis, 'afterCheckpoint~duringCatchup:expired', 10)
        self.setExpireKey(src_redis, 'afterCheckpoint~duringCatchup:persist', 100)
        self.setExpireS3Key(src_redis, 'S3:afterCheckpoint~duringCatchup:expired', 10)
        self.setExpireS3Key(src_redis, 'S3:afterCheckpoint~duringCatchup:persist', 100)

        # remote catchup (smr log migration)
        util.log(">>> start remote catchup (%s)" % time.asctime())

        dst_host = dst_master['ip']
        dst_smr_port = dst_master['smr_base_port']
        rle = '1 8192'
        num_part = 8192

        smr.write('migrate start %s %d %d %d %d %s\r\n' % (dst_host, dst_smr_port,
                                                     seq, tps, num_part, rle))
        response = smr.read_until('\r\n')
        if response[:3] != '+OK':
            util.log('failed to execute migrate start command, response:%s' % response)
            return False

        while True:
            smr.write('migrate info\r\n')
            response = smr.read_until('\r\n')
            seqs = response.split()
            logseq = int(seqs[1].split(':')[1])
            mig = int(seqs[2].split(':')[1])
            util.log('migrate info: %s' % response)
            if (logseq-mig < 500000):
                util.log('Remote catchup almost done. try mig2pc')
                break
            time.sleep(1)

        util.log(">>> sleep until 90 sec pass")
        self.assertFalse(time.time() - ts >= 90)
        time.sleep(90 - (time.time() - ts))

        res = self.persistKey(src_redis, 'afterCheckpoint~duringCatchup:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistKey(src_redis, 'afterCheckpoint~duringCatchup:expired')
        self.assertEquals(res, ":0\r\n")
        res = self.persistS3Key(src_redis, 'S3:afterCheckpoint~duringCatchup:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistS3Key(src_redis, 'S3:afterCheckpoint~duringCatchup:expired')
        self.assertEquals(res, ":0\r\n")

        ts = time.time()
        self.setExpireKey(src_redis, 'duringCatchup~duringCatchup:expired', 10)
        self.setExpireKey(src_redis, 'duringCatchup~duringCatchup:persist', 20)
        self.setExpireS3Key(src_redis, 'S3:duringCatchup~duringCatchup:expired', 10)
        self.setExpireS3Key(src_redis, 'S3:duringCatchup~duringCatchup:persist', 20)

        util.log(">>> sleep until 15 sec pass")
        self.assertFalse(time.time() - ts >= 15)
        time.sleep(15 - (time.time() - ts))

        res = self.persistKey(src_redis, 'duringCatchup~duringCatchup:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistKey(src_redis, 'duringCatchup~duringCatchup:expired')
        self.assertEquals(res, ":0\r\n")
        res = self.persistS3Key(src_redis, 'S3:duringCatchup~duringCatchup:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistS3Key(src_redis, 'S3:duringCatchup~duringCatchup:expired')
        self.assertEquals(res, ":0\r\n")

        ts = time.time()
        self.setExpireKey(src_redis, 'duringCatchup~afterMig2pc:expired', 10)
        self.setExpireKey(src_redis, 'duringCatchup~afterMig2pc:persist', 20)
        self.setExpireS3Key(src_redis, 'S3:duringCatchup~afterMig2pc:expired', 10)
        self.setExpireS3Key(src_redis, 'S3:duringCatchup~afterMig2pc:persist', 20)

        util.log(">>> remote catchup phase almost done (%s)" % time.asctime())

        # mig2pc
        util.log(">>> start mig2pc (%s)" % time.asctime())

        cmd = 'mig2pc %s %d %d %d %d' % (self.cluster['cluster_name'], src_pg_id, dst_pg_id,
                                         0, 8191)
        result = util.cm_command(leader_cm['ip'], leader_cm['cm_port'], cmd)
        util.log('mig2pc result : ' + result)
        if not result.startswith('{"state":"success","msg":"+OK"}\r\n'):
            util.log('failed to execute mig2pc command, result:%s' % result)
            return False

        util.log(">>> sleep until 15 sec pass")
        self.assertFalse(time.time() - ts >= 15)
        time.sleep(15 - (time.time() - ts))

        res = self.persistKey(dst_redis, 'duringCatchup~afterMig2pc:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistKey(dst_redis, 'duringCatchup~afterMig2pc:expired')
        self.assertEquals(res, ":0\r\n")
        res = self.persistS3Key(dst_redis, 'S3:duringCatchup~afterMig2pc:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistS3Key(dst_redis, 'S3:duringCatchup~afterMig2pc:expired')
        self.assertEquals(res, ":0\r\n")

        ts = time.time()
        self.setExpireKey(dst_redis, 'afterMig2pc~migrateEnd:expired', 10)
        self.setExpireKey(dst_redis, 'afterMig2pc~migrateEnd:persist', 20)
        self.setExpireS3Key(dst_redis, 'S3:afterMig2pc~migrateEnd:expired', 10)
        self.setExpireS3Key(dst_redis, 'S3:afterMig2pc~migrateEnd:persist', 20)

        # finish migration
        smr.write('migrate interrupt\r\n')
        response = smr.read_until('\r\n')
        util.log('migrate interrupt: %s' % response)
        smr.disconnect()

        # notify dst_redis of migration end
        util.log(">>> notify dst_redis of migration end (%s)" % time.asctime())

        cmd = 'migconf migend\r\n'
        dst_redis.write(cmd)
        res = dst_redis.read_until('\r\n')
        self.assertEquals( res, '+OK\r\n' )

        cmd = 'migconf clearstart %d-%d\r\n' % (0, 8191)
        src_redis.write(cmd)
        res = src_redis.read_until('\r\n')
        self.assertEquals( res, '+OK\r\n' )

        util.log(">>> sleep until 15 sec pass")
        self.assertFalse(time.time() - ts >= 15)
        time.sleep(15 - (time.time() - ts))

        res = self.persistKey(dst_redis, 'afterMig2pc~migrateEnd:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistKey(dst_redis, 'afterMig2pc~migrateEnd:expired')
        self.assertEquals(res, ":0\r\n")
        res = self.persistS3Key(dst_redis, 'S3:afterMig2pc~migrateEnd:persist')
        self.assertEquals(res, ":1\r\n")
        res = self.persistS3Key(dst_redis, 'S3:afterMig2pc~migrateEnd:expired')
        self.assertEquals(res, ":0\r\n")

        ts = time.time()
        util.log(">>> sleep until 15 sec pass")
        self.assertFalse(time.time() - ts >= 15)
        time.sleep(15 - (time.time() - ts))

        self.assertTrue(self.isExist(dst_redis, 'beforeCheckpoint~beforeCheckpoint:persist'))
        self.assertFalse(self.isExist(dst_redis, 'beforeCheckpoint~beforeCheckpoint:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:beforeCheckpoint~beforeCheckpoint:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:beforeCheckpoint~beforeCheckpoint:expired'))

        self.assertTrue(self.isExist(dst_redis, 'beforeCheckpoint~afterCheckpoint:persist'))
        self.assertFalse(self.isExist(dst_redis, 'beforeCheckpoint~afterCheckpoint:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:beforeCheckpoint~afterCheckpoint:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:beforeCheckpoint~afterCheckpoint:expired'))

        self.assertTrue(self.isExist(dst_redis, 'afterCheckpoint~afterCheckpoint:persist'))
        self.assertFalse(self.isExist(dst_redis, 'afterCheckpoint~afterCheckpoint:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:afterCheckpoint~afterCheckpoint:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:afterCheckpoint~afterCheckpoint:expired'))

        self.assertTrue(self.isExist(dst_redis, 'afterCheckpoint~duringCatchup:persist'))
        self.assertFalse(self.isExist(dst_redis, 'afterCheckpoint~duringCatchup:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:afterCheckpoint~duringCatchup:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:afterCheckpoint~duringCatchup:expired'))

        self.assertTrue(self.isExist(dst_redis, 'duringCatchup~duringCatchup:persist'))
        self.assertFalse(self.isExist(dst_redis, 'duringCatchup~duringCatchup:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:duringCatchup~duringCatchup:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:duringCatchup~duringCatchup:expired'))

        self.assertTrue(self.isExist(dst_redis, 'duringCatchup~afterMig2pc:persist'))
        self.assertFalse(self.isExist(dst_redis, 'duringCatchup~afterMig2pc:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:duringCatchup~afterMig2pc:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:duringCatchup~afterMig2pc:expired'))

        self.assertTrue(self.isExist(dst_redis, 'afterMig2pc~migrateEnd:persist'))
        self.assertFalse(self.isExist(dst_redis, 'afterMig2pc~migrateEnd:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:afterMig2pc~migrateEnd:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:afterMig2pc~migrateEnd:expired'))

        # remote partial checkpoint
        util.log(">>> start rangedel (%s)" % time.asctime())
        cmd = "./cluster-util --rangedel %s %d %d-%d %d" % (
                    src_master['ip'], src_master['redis_port'],
                    0, 8191, tps)
        p = util.exec_proc_async(util.cluster_util_dir(src_master['id']), cmd, True, None, subprocess.PIPE, None)
        ret = p.wait()

        for line in p.stdout:
            util.log(">>>" + str(line.rstrip()))

        cmd = 'migconf clearend\r\n'
        src_redis.write(cmd)
        res = src_redis.read_until('\r\n')
        self.assertEqual(res, '+OK\r\n')

        time.sleep(5) # generate load for 5 sec
        # check consistency of load_generator
        for i in range(len(load_gen_thrd_list)):
            load_gen_thrd_list[i].quit()
        for i in range(len(load_gen_thrd_list)):
            load_gen_thrd_list[i].join()
            self.assertTrue(load_gen_thrd_list[i].isConsistent(), 'Inconsistent after migration')

        # kill dst_redis and recover from bgsave
        util.log(">>> kill dst_redis and recover from bgsave (%s)" % time.asctime())

        dst_redis.disconnect()
        ret = testbase.request_to_shutdown_redis(dst_master)
        self.assertEquals( ret, 0, 'failed to shutdown redis' )
        ret = testbase.request_to_shutdown_smr(dst_master)
        self.assertEquals(ret, 0, 'failed to shutdown smr')
        time.sleep(5)

        testbase.request_to_start_smr(dst_master)
        self.assertEqual( ret, 0, 'failed to start smr, server:%d' % dst_master['id'] )

        ret = testbase.request_to_start_redis(dst_master)
        self.assertEqual( ret, 0, 'failed to start redis, server:%d' % dst_master['id']  )

        ret = testbase.wait_until_finished_to_set_up_role(dst_master)
        self.assertEquals( ret, 0, 'failed to role change. server:%d' % (dst_master['id']) )

        dst_redis = redis_mgmt.Redis(dst_master['id'])
        ret = dst_redis.connect(dst_master['ip'], dst_master['redis_port'] )
        self.assertEquals( ret, 0, 'failed to connect to redis' )

        self.assertTrue(self.isExist(dst_redis, 'beforeCheckpoint~beforeCheckpoint:persist'))
        self.assertFalse(self.isExist(dst_redis, 'beforeCheckpoint~beforeCheckpoint:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:beforeCheckpoint~beforeCheckpoint:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:beforeCheckpoint~beforeCheckpoint:expired'))

        self.assertTrue(self.isExist(dst_redis, 'beforeCheckpoint~afterCheckpoint:persist'))
        self.assertFalse(self.isExist(dst_redis, 'beforeCheckpoint~afterCheckpoint:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:beforeCheckpoint~afterCheckpoint:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:beforeCheckpoint~afterCheckpoint:expired'))

        self.assertTrue(self.isExist(dst_redis, 'afterCheckpoint~afterCheckpoint:persist'))
        self.assertFalse(self.isExist(dst_redis, 'afterCheckpoint~afterCheckpoint:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:afterCheckpoint~afterCheckpoint:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:afterCheckpoint~afterCheckpoint:expired'))

        self.assertTrue(self.isExist(dst_redis, 'afterCheckpoint~duringCatchup:persist'))
        self.assertFalse(self.isExist(dst_redis, 'afterCheckpoint~duringCatchup:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:afterCheckpoint~duringCatchup:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:afterCheckpoint~duringCatchup:expired'))

        self.assertTrue(self.isExist(dst_redis, 'duringCatchup~duringCatchup:persist'))
        self.assertFalse(self.isExist(dst_redis, 'duringCatchup~duringCatchup:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:duringCatchup~duringCatchup:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:duringCatchup~duringCatchup:expired'))

        self.assertTrue(self.isExist(dst_redis, 'duringCatchup~afterMig2pc:persist'))
        self.assertFalse(self.isExist(dst_redis, 'duringCatchup~afterMig2pc:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:duringCatchup~afterMig2pc:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:duringCatchup~afterMig2pc:expired'))

        self.assertTrue(self.isExist(dst_redis, 'afterMig2pc~migrateEnd:persist'))
        self.assertFalse(self.isExist(dst_redis, 'afterMig2pc~migrateEnd:expired'))
        self.assertTrue(self.isS3Exist(dst_redis, 'S3:afterMig2pc~migrateEnd:persist'))
        self.assertFalse(self.isS3Exist(dst_redis, 'S3:afterMig2pc~migrateEnd:expired'))

        self.getS3TTL(dst_redis, 'S3:PermanentKey')

        # kill dst_slave redis and recover without dump file
        util.log(">>> kill dst_redis and recover without dump file (%s)" % time.asctime())
        dst_slave = util.get_server_by_role_and_pg(self.cluster['servers'], 'slave', dst_pg_id)

        ret = testbase.request_to_shutdown_redis(dst_slave)
        self.assertEquals( ret, 0, 'failed to shutdown redis' )
        ret = testbase.request_to_shutdown_smr(dst_slave)
        self.assertEquals(ret, 0, 'failed to shutdown smr')
        time.sleep(5)

        testbase.request_to_start_smr(dst_slave)
        self.assertEqual( ret, 0, 'failed to start smr, server:%d' % dst_slave['id'] )

        ret = testbase.request_to_start_redis(dst_slave)
        self.assertEqual( ret, 0, 'failed to start redis, server:%d' % dst_slave['id']  )

        ret = testbase.wait_until_finished_to_set_up_role(dst_slave)
        self.assertEquals( ret, 0, 'failed to role change. server:%d' % (dst_slave['id']) )

        dst_redis_slave = redis_mgmt.Redis(dst_slave['id'])
        ret = dst_redis_slave.connect(dst_slave['ip'], dst_slave['redis_port'] )
        self.assertEquals( ret, 0, 'failed to connect to redis' )

        self.assertTrue(self.isExist(dst_redis_slave, 'beforeCheckpoint~beforeCheckpoint:persist'))
        self.assertFalse(self.isExist(dst_redis_slave, 'beforeCheckpoint~beforeCheckpoint:expired'))
        self.assertTrue(self.isS3Exist(dst_redis_slave, 'S3:beforeCheckpoint~beforeCheckpoint:persist'))
        self.assertFalse(self.isS3Exist(dst_redis_slave, 'S3:beforeCheckpoint~beforeCheckpoint:expired'))

        self.assertTrue(self.isExist(dst_redis_slave, 'beforeCheckpoint~afterCheckpoint:persist'))
        self.assertFalse(self.isExist(dst_redis_slave, 'beforeCheckpoint~afterCheckpoint:expired'))
        self.assertTrue(self.isS3Exist(dst_redis_slave, 'S3:beforeCheckpoint~afterCheckpoint:persist'))
        self.assertFalse(self.isS3Exist(dst_redis_slave, 'S3:beforeCheckpoint~afterCheckpoint:expired'))

        self.assertTrue(self.isExist(dst_redis_slave, 'afterCheckpoint~afterCheckpoint:persist'))
        self.assertFalse(self.isExist(dst_redis_slave, 'afterCheckpoint~afterCheckpoint:expired'))
        self.assertTrue(self.isS3Exist(dst_redis_slave, 'S3:afterCheckpoint~afterCheckpoint:persist'))
        self.assertFalse(self.isS3Exist(dst_redis_slave, 'S3:afterCheckpoint~afterCheckpoint:expired'))

        self.assertTrue(self.isExist(dst_redis_slave, 'afterCheckpoint~duringCatchup:persist'))
        self.assertFalse(self.isExist(dst_redis_slave, 'afterCheckpoint~duringCatchup:expired'))
        self.assertTrue(self.isS3Exist(dst_redis_slave, 'S3:afterCheckpoint~duringCatchup:persist'))
        self.assertFalse(self.isS3Exist(dst_redis_slave, 'S3:afterCheckpoint~duringCatchup:expired'))

        self.assertTrue(self.isExist(dst_redis_slave, 'duringCatchup~duringCatchup:persist'))
        self.assertFalse(self.isExist(dst_redis_slave, 'duringCatchup~duringCatchup:expired'))
        self.assertTrue(self.isS3Exist(dst_redis_slave, 'S3:duringCatchup~duringCatchup:persist'))
        self.assertFalse(self.isS3Exist(dst_redis_slave, 'S3:duringCatchup~duringCatchup:expired'))

        self.assertTrue(self.isExist(dst_redis_slave, 'duringCatchup~afterMig2pc:persist'))
        self.assertFalse(self.isExist(dst_redis_slave, 'duringCatchup~afterMig2pc:expired'))
        self.assertTrue(self.isS3Exist(dst_redis_slave, 'S3:duringCatchup~afterMig2pc:persist'))
        self.assertFalse(self.isS3Exist(dst_redis_slave, 'S3:duringCatchup~afterMig2pc:expired'))

        self.assertTrue(self.isExist(dst_redis_slave, 'afterMig2pc~migrateEnd:persist'))
        self.assertFalse(self.isExist(dst_redis_slave, 'afterMig2pc~migrateEnd:expired'))
        self.assertTrue(self.isS3Exist(dst_redis_slave, 'S3:afterMig2pc~migrateEnd:persist'))
        self.assertFalse(self.isS3Exist(dst_redis_slave, 'S3:afterMig2pc~migrateEnd:expired'))

        self.getS3TTL(dst_redis_slave, 'S3:PermanentKey')

        # Go back to initial configuration
        self.assertTrue(util.migration(self.cluster, dst_pg_id, src_pg_id, 0, 8191, 40000),
                'failed to rollback migration')

    def __test_pdump(self):
        util.print_frame()
        pass

    # check if pthread getspecific, setspecific work properly
    def __test_partial_load_with_s3command(self):
        util.print_frame()
        pass
