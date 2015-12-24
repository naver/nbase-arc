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

class TestLogRecover (unittest.TestCase):
  BASE_PORT = 1900
  BE_PORT = 1909

  def make_some_logs(self, runtime=10):
      # make some logs
      clients = []
      num_clients = 20
      for i in range (0, num_clients):
        C = Client.Client()
	clients.append(C)
	C.slotid = i
	C.add(C.slotid, 'localhost', self.BE_PORT)
	C.size(64*1024, C.slotid) # 1M
	C.tps(100, C.slotid)

      for C in clients:
	C.start(C.slotid)

      runtime_limit = 10
      runtime = 0
      while runtime < runtime_limit: 
	time.sleep(1)
	runtime = runtime + 1

      for C in clients:
	C.stop(C.slotid)

  def test_log_recover_idempotent(self):
    cm = None
    pgs = None
    try:
      cm = Cm.CM("test_log_recover_idempotent")
      cm.create_workspace()
      pg = Pg.PG(0)

      # pgs --> master
      pgs = Pgs.PGS(0, 'localhost', self.BASE_PORT, cm.dir)
      pg.join(pgs, start=True)
      pgs.smr.wait_role(Smr.SMR.MASTER)

      # make some logs
      self.make_some_logs(runtime=10)
      time.sleep(1)

      # remember original sequences
      org_seqs = pgs.smr.getseq_log()

      # kill pgs
      pg.leave(pgs.id, kill=True)

      # log_recovery is dempotent
      for i in range(0, 3):
	# due to sucking master election logic in Pg.py we do not launch be currently
	# use PgNew later
	pgs.start_smr()
	seqs = pgs.smr.getseq_log()
	pgs.kill_smr()
	assert seqs['commit'] == org_seqs['commit']
	assert seqs['min'] == org_seqs['min']
	assert seqs['max'] == org_seqs['max']
	
      
    finally:
      # Util.tstop('Check output!')
      if pgs is not None:
	pgs.kill_smr()
	pgs.kill_be()
      if cm is not None:
	cm.remove_workspace() 

  def test_log_hole(self):
    ''' make randome hole in log files and check '''
    pass

if __name__ == '__main__':
  unittest.main()
