import unittest
import time
import Util, Conf, Cm, Pg, Pgs, Smr, Client

class TestPingDuringLogSend (unittest.TestCase):

  def test_ping_during_log_send(self):
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


      # make lots of logs
      clients = []
      num_clients = 20
      for i in range (0, num_clients):
        C = Client.Client()
	clients.append(C)
	C.slotid = i
	C.add(C.slotid, 'localhost', 1909)
	C.size(64*1024, C.slotid) # 1M
	C.tps(100, C.slotid)

      for C in clients:
	C.start(C.slotid)

      runtime_limit = 60 
      runtime = 0
      while runtime < runtime_limit: 
	time.sleep(1)
	runtime = runtime + 1

      for C in clients:
	C.stop(C.slotid)

      # get log seqeunce of the master
      master_seqs = pgs1.smr.getseq_log()

      # pgs2 --> slave
      pgs2 = Pgs.PGS(1, 'localhost', 9000, cm.dir)
      pg.join(pgs2, start=True)
      pgs2.smr.wait_role(Smr.SMR.SLAVE)

      try_count = 0
      count = 0
      prev_seq = 0
      master_response_times = []
      slave_response_times = []

      while True:
	try_count = try_count + 1

	# master
	st = int(round(time.time() * 1000))
	seqs = pgs1.smr.getseq_log()
	et = int(round(time.time() * 1000))
	master_response_times.append(et-st)

	# slave
	st = int(round(time.time() * 1000))
	seqs = pgs2.smr.getseq_log()
	et = int(round(time.time() * 1000))
	slave_response_times.append(et-st)

	if prev_seq != seqs['max']:
	  count = count + 1
	  prev_seq = seqs['max']
	if master_seqs['max'] <= seqs['max']:
	  break
	time.sleep(0.1)
      print "==========> try_count:%d count:%d" % (try_count, count)
      print "MASTER ==========>", master_response_times
      print "SLAVE ==========>", slave_response_times
      for rt in master_response_times:
	assert rt < 100
      for rt in slave_response_times:
	assert rt < 100

    finally:
      # Util.tstop('Check output!')
      if pgs1 is not None:
	pgs1.kill_smr()
	pgs1.kill_be()
      if pgs2 is not None:
	pgs2.kill_smr()
	pgs2.kill_be()
      if cm is not None:
	cm.remove_workspace()

if __name__ == '__main__':
  unittest.main()
