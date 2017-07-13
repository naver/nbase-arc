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
import threading
import Util, Cm, Pg, Pgs, Smr, Conn

class DummyPeer(threading.Thread):
    def __init__(self, host, port, sec):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.sec = sec
    def run(self):
        conn = Conn.Conn(self.host, self.port)
        conn.lazy_connect()
        time.sleep(self.sec)
        conn.disconnect()

class TestDeadline (unittest.TestCase):

    def test_deadline(self):
        cm = None
        pgs = None
        dummy_peer = None
        try:
            cm = Cm.CM("test_pgs")
            cm.create_workspace()
            pg = Pg.PG(0)

            pgs = Pgs.PGS(0, 'localhost', 1900, cm.dir)
            pg.join(pgs, start=True)
            pgs.smr.wait_role(Smr.SMR.MASTER)

            dummy_peer = DummyPeer('localhost', 1902, 3)
            dummy_peer.start()
            time.sleep(0.5)

            st = int(round(time.time() * 1000))
            seqs = pgs.smr.getseq_log()
            et = int(round(time.time() * 1000))
            assert et - st < 1000

        finally:
            # Util.tstop('Check output!')
            if pgs is not None:
                pgs.kill_smr()
                pgs.kill_be()
            if dummy_peer is not None:
                dummy_peer.join()
            if cm is not None:
                cm.remove_workspace()

if __name__ == '__main__':
    unittest.main()
