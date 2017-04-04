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

class TestBeReconf (unittest.TestCase):
    #  ---- Test scenario ----
    #
    # M(1) S S
    # launch clients
    # loop
    #   M(1) S L
    #   wait some time and check progress
    #   M(1) S S
    #   L L L
    #   S M(1) S
    #   alternate pgs1 and pgs2
    #   check..
    # stop clients
    #
    def test_be_reconfiguration(self):
        cm = None
        M = None
        C = None
        P = None
        try:
            cm = Cm.CM("test_pgs")
            cm.create_workspace()

            # M(1), S, S
            M = _init_pgs(0, 'localhost', 1900, cm.dir)
            M.smr.role_master(M.id, 1, 0)

            C = _init_pgs(1, 'localhost', 1910, cm.dir)
            C.smr.role_slave(C.id, 'localhost', M.base_port, 0)

            P = _init_pgs(2, 'localhost', 1920, cm.dir)
            P.smr.role_slave(P.id, 'localhost', M.base_port, 0)
            
            # dirty: there is some time interval between A ~ B
            # A. replicator has role 
            # B. backend is notified to be ready and setup listen port for clients
            time.sleep(1.0)

            _attach_client(M, 1024, 1000)
            _attach_client(C, 1024, 1000)
            _attach_client(P, 1024, 1000)

            M.client.start(M.client.slotid)
            C.client.start(C.client.slotid)
            P.client.start(P.client.slotid)

            # loop
            loopCount = 100
            for i in range (0, loopCount):
                # M(1) S L
                P.smr.role_lconn()

                # wait some time and check progress
                prev_mseqs = M.smr.getseq_log()
                prev_cseqs = C.smr.getseq_log()
                time.sleep(0.5)
                curr_mseqs = M.smr.getseq_log()
                curr_cseqs = C.smr.getseq_log()
                assert prev_mseqs['max'] < curr_mseqs['max']
                assert prev_cseqs['max'] < curr_cseqs['max']

                # M(1) S S
                ps = P.smr.getseq_log()
                P.smr.role_slave(P.id, 'localhost', M.base_port, ps['max'])

                # L L L
                M.smr.role_lconn()
                C.smr.role_lconn()
                P.smr.role_lconn()

                # alternate pgs1 and pgs2
                tmp = M
                M = C
                C = tmp

                # M(1) S S
                ms = M.smr.getseq_log()
                cs = C.smr.getseq_log()
                ps = P.smr.getseq_log()

                M.smr.role_master(M.id, 1, ms['max'])
                C.smr.role_slave(C.id, 'localhost', M.base_port, min(ms['max'], cs['max']))
                P.smr.role_slave(P.id, 'localhost', M.base_port, min(ms['max'], ps['max']))

                # check progress and backend sanity
                mstat = M.client.stat(M.client.slotid)
                cstat = C.client.stat(C.client.slotid)
                pstat = P.client.stat(P.client.slotid)
                print "M ====>"  + str(mstat)
                print "C ====>"  + str(cstat)
                print "P ====>"  + str(pstat)
                mresp = int(mstat['resp'])
                cresp = int(cstat['resp'])
                presp = int(pstat['resp'])

                sleepCount = 0
                while True:
                    time.sleep(0.1)
                    mstat = M.client.stat(M.client.slotid)
                    cstat = C.client.stat(C.client.slotid)
                    pstat = P.client.stat(P.client.slotid)
                    assert int(mstat['reconn']) == 0 and int(cstat['reconn']) == 0 and int(pstat['reconn']) == 0
                    mr = int(mstat['resp'])
                    cr = int(cstat['resp'])
                    pr = int(pstat['resp'])
                    if mr > mresp and cr > cresp and pr > presp:
                        break
                    sleepCount = sleepCount + 1
                    assert sleepCount <= 10

            # stop clients
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
