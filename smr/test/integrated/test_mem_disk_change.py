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
import Util, Conf, Cm, Pg, Pgs, Smr, Client, Log

class TestMemDiskChange (unittest.TestCase):
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
	C.size(64*1024, C.slotid)
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

  def test_mem_disk_change (self):
    saved = Conf.USE_MEM_LOG
    Conf.USE_MEM_LOG = True
    cm = None
    pgs = None
    expected_seqs = {}
    try:
      cm = Cm.CM("tmpTestMemDiskChange")
      cm.create_workspace()
      pg = Pg.PG(0)
      pgs = Pgs.PGS(0, 'localhost', self.BASE_PORT, cm.dir)

      expected_seqs['min'] = 0
      expected_seqs['max'] = 0

      for i in range (0, 5):
	if i % 2 == 0:
	  Log.createlog(pgs.dir)
	else:
	  Log.syncdeletelog(pgs.dir)

	# start replicator 
	pgs.start_smr()
	pgs.smr.wait_role(Smr.SMR.NONE)

	# check seqs
	seqs = pgs.smr.getseq_log()
	print "expected =======>", expected_seqs
	print "seqs     =======>", seqs
	assert seqs['min'] == expected_seqs['min']
	assert seqs['max'] == expected_seqs['max']

	# pgs -> master
	pgs.start_be()
	pgs.smr.wait_role(Smr.SMR.LCONN)
	pg.join(pgs)
	pgs.smr.wait_role(Smr.SMR.MASTER)

	# kill be 
	self.make_some_logs(runtime=3)

	# wait for be to apply all logs
	time.sleep(1)

	# checkpoint
	pgs.be.ckpt()

	# remember original sequences
	expected_seqs = pgs.smr.getseq_log()

	# kill pgs
	pg.leave(pgs.id, kill=True)

    finally:
      #Util.tstop('Check output!')
      if pgs is not None:
	pgs.kill_smr()
	pgs.kill_be()
      if cm is not None:
	cm.remove_workspace() 
      Conf.USE_MEM_LOG = saved

  def test_log_hole(self):
    ''' make randome hole in log files and check '''
    pass

if __name__ == '__main__':
  unittest.main()
