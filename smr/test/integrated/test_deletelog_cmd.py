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
import time
import Util, Conf, Cm, Pg, Pgs, Smr, Client

class TestDeletelogCmd (unittest.TestCase):

    def test_logdelete(self):
        cm = None
        pgs1 = None
        try:
            cm = Cm.CM("test_logdelete")
            cm.create_workspace()
            pg = Pg.PG(0)

            # pgs --> master
            pgs = Pgs.PGS(0, 'localhost', 1900, cm.dir)
            pg.join(pgs, start=True)
            pgs.smr.wait_role(Smr.SMR.MASTER)

            # make lots of logs
            clients = []
            num_clients = 20
            for i in range (0, num_clients):
                C = Client.Client()
                clients.append(C)
                C.slotid = i
                C.add(C.slotid, 'localhost', 1909)
                C.size(64*1024, C.slotid)
                C.tps(100, C.slotid)

            for C in clients:
                C.start(C.slotid)

            runtime_limit = 15
            runtime = 0
            while runtime < runtime_limit:
                time.sleep(1)
                runtime = runtime + 1

            for C in clients:
                C.stop(C.slotid)

            # checkpoint server
            pgs.be.ckpt()
            time.sleep(1.0)

            # delete log
            log_delete_seq = pgs.smr.deletelog(retain = 0)
            assert(log_delete_seq > 0), log_delete_seq
            print ("deltelog 0 returns", log_delete_seq)

            # log delete interval in idle state is 10 sec. (see bio.c)
            # after than, 1.0 0.9 0.8 ... 0.1, 0.1 ...
            logdel_timeout = 10 + log_delete_seq/(64*1024*1024)
            deltime = 0
            while deltime < logdel_timeout:
                seqs = pgs.smr.getseq_log()
                # Note background delete spares one log file (see del_proc in bio.c)
                if seqs['min'] + 64*1024*1024 == log_delete_seq:
                    break
                time.sleep(1)
                deltime = deltime + 1
            assert(deltime < logdel_timeout), logdel_timeout
        finally:
            if pgs is not None:
                pgs.kill_smr()
                pgs.kill_be()
            if cm is not None:
                cm.remove_workspace()

if __name__ == '__main__':
    unittest.main()
