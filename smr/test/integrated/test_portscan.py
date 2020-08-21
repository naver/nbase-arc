#
# Copyright 2020 Naver Corp.
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
import Cm, Pg, Pgs, Smr, Conn, Util

class TestPortScan (unittest.TestCase):

    def _check_conn_blocked(self):
        st = time.time()
        for i in range(40):
            ok = True
            try:
                conn = Conn.Conn('localhost', 1900+(i%4))
                resp = conn.do_request('ping')
            except:
                ok = False
            if ok:
                raise Exception("Not blocked")
        et = time.time()
        if et - st > 1.0:
            raise Exception("10 connection try exceeds 1.0 sec.")

    def test_logdelete(self):
        cm = None
        pgs1 = None
        try:
            cm = Cm.CM("test_portscan")
            cm.create_workspace()
            pg = Pg.PG(0)

            # pgs --> master
            pgs = Pgs.PGS(0, 'localhost', 1900, cm.dir)
            pg.join(pgs, start=True)
            pgs.smr.wait_role(Smr.SMR.MASTER)

            # -----------------------------------------
            # Test bad handshake blocks IP temporarily
            # -----------------------------------------
            for off in range(0,3):
                # bad handshake
                try:
                    conn = Conn.Conn('localhost', 1900+off)
                    resp = conn.do_request('ping')
                except:
                    pass
                self._check_conn_blocked()

            # wait for block released
            time.sleep(2.0) # actually 1.5 sec

            # -------------------------------------------------------
            # Can't connect mgmt port SMR_MAX_MGMT_CLIENTS_PER_IP(50)
            # -------------------------------------------------------
            conns = []
            for i in range(50-1): # -1
                conn = Conn.Conn('localhost', 1903)
                resp = conn.do_request('ping')
                assert(len(resp) == 1 and resp[0].startswith('+OK')), resp
                conns.append(conn)
            self._check_conn_blocked()

        finally:
            if pgs is not None:
                pgs.kill_smr()
                pgs.kill_be()
            if cm is not None:
                cm.remove_workspace()

if __name__ == '__main__':
    unittest.main()
