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
import Util, Cm, Pgs, Smr, Client

def _init_pgs (nid, host, port, dir):
    pgs = Pgs.PGS(nid, host, port, dir)
    pgs.start_smr()
    pgs.smr.wait_role(Smr.SMR.NONE)
    pgs.start_be()
    pgs.smr.wait_role(Smr.SMR.LCONN)
    return pgs

def _attach_client(pgs, size, tps):
    client = Client.Client()
    client.slotid = pgs.id
    client.add(client.slotid, 'localhost', pgs.base_port + 9)
    client.size(size, client.slotid)
    client.tps(tps, client.slotid)
    pgs.client = client

def _check_progress(pgs_ary, noprogress=False):
    snap = []
    for pgs in pgs_ary:
        stat = pgs.client.stat(pgs.client.slotid)
        snap.append(stat)

    sleepCount = 0
    while True:
        time.sleep(0.1)
        currsnap = []
        for pgs in pgs_ary:
            stat = pgs.client.stat(pgs.client.slotid)
            currsnap.append(stat)
        nprogress = 0
        for i in range (0, len(pgs_ary)):
            if int(snap[i]['resp']) < int(currsnap[i]['resp']):
                nprogress = nprogress + 1
        if noprogress and nprogress == 0:
            break
        elif not noprogress and nprogress == len(pgs_ary):
            break
        sleepCount = sleepCount + 1
        assert sleepCount <= 10

class TestQuorum (unittest.TestCase):
    def test_quorum_membership_commands(self):
        cm = None
        M = None
        try:
            cm = Cm.CM("test_quorum")
            cm.create_workspace()
            # M(1), S, S
            M = _init_pgs(0, 'localhost', 1900, cm.dir)
            M.smr.role_master(M.id, 1, 0, [1,2])
            qm = M.smr.getquorumv()
            assert len(qm) == 3 # quorum and nids
        finally:
            #Util.tstop('Check output!')
            if M is not None:
                M.kill()
            if cm is not None:
                cm.remove_workspace()

    def test_quorum(self):
        cm = None
        M = None
        C = None
        P = None
        try:
            cm = Cm.CM("test_quorum")
            cm.create_workspace()
            # M(1), S, S
            M = _init_pgs(0, 'localhost', 1900, cm.dir)
            M.smr.role_master(M.id, 1, 0)

            C = _init_pgs(1, 'localhost', 1910, cm.dir)
            C.smr.role_slave(C.id, 'localhost', M.base_port, 0)

            P = _init_pgs(2, 'localhost', 1920, cm.dir)
            P.smr.role_slave(P.id, 'localhost', M.base_port, 0)

            _attach_client(M, 1024, 1000)
            _attach_client(C, 1024, 1000)
            _attach_client(P, 1024, 1000)

            M.client.start(M.client.slotid)
            C.client.start(C.client.slotid)
            P.client.start(P.client.slotid)

            # Loop
            loopCount = 20
            for i in range (0, loopCount):
                #  M(1), S, S
                M.smr.setquorum(1)
                _check_progress([M,C,P])
                #  M(1), S, L
                P.smr.role_lconn()
                _check_progress([M,C])
                #  M(C), S, S
                ps = P.smr.getseq_log()
                P.smr.role_slave(P.id, 'localhost', M.base_port, ps['max'])
                _check_progress([M,C,P])
                #  M(C), S, L
                M.smr.setquorum(1, [C.id])
                P.smr.role_lconn()
                _check_progress([M,C])
                #  M(C), S, S
                ps = P.smr.getseq_log()
                P.smr.role_slave(P.id, 'localhost', M.base_port, ps['max'])
                snap = _check_progress([M,C,P])
                #  M(C), L, S
                C.smr.role_lconn()
                _check_progress([M,P], noprogress=True)
                #  M(C), S, S
                cs = C.smr.getseq_log()
                C.smr.role_slave(C.id, 'localhost', M.base_port, cs['max'])
                _check_progress([M,C,P])
                loopCount = loopCount + 1
            # Stop clients
            M.client.stop(M.client.slotid)
            C.client.stop(C.client.slotid)
            P.client.stop(P.client.slotid)
        finally:
            #Util.tstop('Check output!')
            if M is not None:
                M.kill()
            if C is not None:
                C.kill()
            if P is not None:
                P.kill()
            if cm is not None:
                cm.remove_workspace()

if __name__ == '__main__':
    unittest.main()
