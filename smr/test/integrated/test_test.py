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
import io
import socket
import os
import time
import traceback, sys
import Conf, Cm, Pg, Pgs, Smr, Be, Util, Conn

class TestTest (unittest.TestCase):
    def test_all(self):
        self._test_cm()
        self._test_pgs()
        self._test_smr()
        self._test_be()
        self._test_pg()
        self._test_client()

    def _test_cm(self):
        cm = None
        try:
            cm = Cm.CM("test_cm")
            assert cm is not None

            cm.create_workspace()
            dir = cm.dir
            assert os.path.exists(dir)

            cm.remove_workspace()
            assert cm.dir is None
            assert os.path.exists(dir) == False
            cm = None
        finally:
            if cm != None:
                cm.remove_workspace()

    def _test_pgs(self):
        cm = None
        pgs = None
        try:
            cm = Cm.CM("test_pgs")
            assert cm is not None

            cm.create_workspace()
            assert cm.dir != None

            pgs = Pgs.PGS(0, 'localhost', 1900, cm.dir)
            assert pgs is not None

            pgs.start_smr()
            assert pgs.smr != None

            role = pgs.smr.get_role()
            assert role == Smr.SMR.NONE

            pgs.start_be()
            assert pgs.be != None

            pgs.smr.wait_role(Smr.SMR.LCONN)
        except:
            #Util.tstop('Exception Occurred')
            raise
        finally:
            if pgs is not None:
                pgs.kill_be()
                pgs.kill_smr()
            if cm is not None:
                cm.remove_workspace()

    def _test_smr_singleton(self):
        c1 = None
        c2 = None
        try:
            c1 = Conn.Conn('localhost', 1903)
            c2 = Conn.Conn('localhost', 1903)
            # set
            r1 = c1.do_request('singleton a')
            assert r1[0] == '+OK'
            # override with same value
            r1 = c1.do_request('singleton a')
            assert r1[0] == '+OK'
            # override with other value
            r1 = c1.do_request('singleton b')
            assert r1[0] == '+OK'
            # bad command
            r2 = c2.do_request('singleton a b c')
            assert r2[0].startswith('-ERR')
            # set with already set singleton value
            r2 = c2.do_request('singleton b')
            assert r2[0] == '+OK'
            try:
                c1.do_request('ping')
                # -- must not be reached
                assert False
            except:
                c1.disconnect() # clear
                c1 = Conn.Conn('localhost', 1903)
        finally:
            if c1 != None:
                c1.disconnect()
            if c2 != None:
                c2.disconnect()

    def _test_smr(self):
        cm = None
        pgs = None
        try:
            cm = Cm.CM("test_pgs")
            assert cm is not None

            cm.create_workspace()
            assert cm.dir != None

            pgs = Pgs.PGS(0, 'localhost', 1900, cm.dir)
            assert pgs is not None

            pgs.start_smr()
            assert pgs.smr != None

            # send confset with empty arguments
            self.assertRaisesRegexp(Exception, '-ERR bad number of token:0',
                    pgs.smr.confset, '', '')

            # other command specific tests
            self._test_smr_singleton()
        finally:
            if pgs is not None:
                pgs.kill_smr()
            if cm is not None:
                cm.remove_workspace()

    def _test_be(self):
        cm = None
        pgs = None
        try:
            cm = Cm.CM("test_be")
            cm.create_workspace()
            pg = Pg.PG(0)

            # pgs --> master
            pgs = Pgs.PGS(0, 'localhost', 1900, cm.dir)
            pg.join(pgs, start=True)
            pgs.smr.wait_role(Smr.SMR.MASTER)

            # Test basic op
            old = pgs.be.set(0, '100')
            assert old >= 0

            r = pgs.be.reset()
            assert r == 0

            new = pgs.be.set(0, '100')
            assert old == new

            r = pgs.be.ping()
            assert r == 0

            # do checkpoint
            r = pgs.be.ckpt()
            assert r == 0

            # restart
            pg.leave(pgs.id, kill=True)
            pg.join(pgs, start=True)
            pgs.smr.wait_role(Smr.SMR.MASTER)

            # check crc of key = 0
            new = pgs.be.get(0)
            assert old == new
        except:
            #Util.tstop('Exception Occurred')
            raise
        finally:
            if pgs is not None:
                pgs.kill_smr()
                pgs.kill_be()
            if cm is not None:
                cm.remove_workspace()

    def _test_pg(self):
        cm = None
        pgs1 = None
        pgs2 = None
        try:
            cm = Cm.CM("test_pgs")
            cm.create_workspace()
            pg = Pg.PG(0)

            # pgs1 --> master
            pgs1 = Pgs.PGS(0, 'localhost', 1900, cm.dir)
            pg.join(pgs1, start=True)
            pgs1.smr.wait_role(Smr.SMR.MASTER)

            # pgs2 --> slave
            pgs2 = Pgs.PGS(1, 'localhost', 1910, cm.dir)
            pg.join(pgs2, start=True)
            pgs2.smr.wait_role(Smr.SMR.SLAVE)

            # kill pgs2, check quorum,
            pg.leave(pgs2.id, kill = True)
            assert pgs1.smr.getquorum() == 0

            # join pgs2
            pg.join(pgs2, start=True)
            pgs2.smr.wait_role(Smr.SMR.SLAVE)

            # kill pgs1 (check pgs1 is master)
            pg.leave(pgs1.id, kill = True)
            pgs2.smr.wait_role(Smr.SMR.MASTER)
            assert pgs2.smr.getquorum() == 0

            # join pgs1
            pg.join(pgs1, start=True)
            pgs1.smr.wait_role(Smr.SMR.SLAVE)

        finally:
            if pgs1 is not None:
                pgs1.kill_smr()
                pgs1.kill_be()
            if pgs2 is not None:
                pgs2.kill_smr()
                pgs2.kill_be()
            if cm is not None:
                cm.remove_workspace()

    def _test_client(self):
        # TODO implement smr-client test code
        pass

if __name__ == '__main__':
    unittest.main()
