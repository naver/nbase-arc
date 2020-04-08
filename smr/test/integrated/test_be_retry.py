# # Copyright 2015 Naver Corp.
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
import Util, Cm, Pgs, Smr

def _init_pgs (nid, host, port, dir):
    pgs = Pgs.PGS(nid, host, port, dir)
    pgs.start_smr()
    pgs.smr.wait_role(Smr.SMR.NONE)
    pgs.start_be()
    pgs.smr.wait_role(Smr.SMR.LCONN)
    return pgs

class TestBeRetry (unittest.TestCase):
    def test_be_retry(self):
        cm = None
        M = None
        S = None
        try:
            cm = Cm.CM("test_be_retry")
            cm.create_workspace()

            # M (master) S(slave)
            M = _init_pgs(0, 'localhost', 1900, cm.dir)
            M.smr.role_master(M.id, 1, 0)
            S = _init_pgs(1, 'localhost', 1910, cm.dir)
            S.smr.role_slave(S.id, 'localhost', M.base_port, 0)
            count = 1
            while count > 0:
                S.smr.role_lconn()
                seqs = S.smr.getseq_log()
                # Util.tstop('before fault injection!')
                M.smr.fi_error_client_accept()
                S.smr.role_slave(S.id, 'localhost', M.base_port, seqs['max'])
                time.sleep(0.5)
                info = M.smr.info()
                #print (info)
                assert (('client' in info) and not ('client_1' in info['client']))
                time.sleep(1.0)
                info = M.smr.info()
                #print (info)
                assert (('client' in info) and ('client_1' in info['client']))
                count = count - 1
        finally:
            if M is not None:
                M.kill()
            if S is not None:
                S.kill()
            if cm is not None:
                cm.remove_workspace()

    def test_split_connection(self):
        cm = None
        M = None
        S = None
        try:
            cm = Cm.CM("test_split_connection")
            cm.create_workspace()

            # M (master) S(slave)
            M = _init_pgs(0, 'localhost', 1900, cm.dir)
            M.smr.role_master(M.id, 1, 0)
            S = _init_pgs(1, 'localhost', 1910, cm.dir)
            S.smr.role_slave(S.id, 'localhost', M.base_port, 0)
            # M: confset "slave_idle_timeout_msec" to 100
            M.smr.fi_once('__no_free_client_conn')
            M.smr.confset('slave_idle_timeout_msec', 100)
            S.smr.wait_role(Smr.SMR.LCONN)
            M.smr.confset('slave_idle_timeout_msec', 18000)
            # S: role slave again
            ps = S.smr.getseq_log()
            S.smr.role_slave(S.id, 'localhost', M.base_port, ps['max'])
            S.smr.wait_role(Smr.SMR.SLAVE)
            S.be.init_conn()
            # check be at S works
            # Util.tstop('before check it!')
            r = S.be.set(0, '100')
            assert r >= 0
        finally:
            if M is not None:
                M.kill()
            if S is not None:
                S.kill()
            if cm is not None:
                cm.remove_workspace()

if __name__ == '__main__':
    unittest.main()
