import os
import traceback
import sys
import time
import json
from optparse import OptionParser
import Conf, Cm, Pg, Pgs, Smr, Be, Util, Client

def sprint(opt):
  if opt.valgrind_smr:
    Conf.VALGRIND_SMR = True
  if opt.use_mem_log:
    Conf.USE_MEM_LOG = True
  if opt.smr_opt_x:
    Conf.SMR_OPT_X = opt.smr_opt_x
  if opt.smr_bin_path:
    Conf.OVERRIDE_SMR_BIN_PATH=opt.smr_bin_path

  cm = None
  pgs = None
  pgs_ary = []
  client_ary = []
  try:
    cm = Cm.CM("test_be")
    cm.create_workspace()
    pg = Pg.PG(0)

    base_port = opt.base_port

    # master
    master = Pgs.PGS(0, 'localhost', base_port, cm.dir)
    pg.join(master, start=True)
    master.smr.wait_role(Smr.SMR.MASTER)
    pgs_ary.append(master);

    # slaves
    id = 1
    for i in range(opt.num_slaves):
      slave = Pgs.PGS(id, 'localhost', base_port + id * 10, cm.dir)
      pg.join(slave, start=True)
      slave.smr.wait_role(Smr.SMR.SLAVE)
      id = id + 1
      pgs_ary.append(slave)

    slotid = 0

    # set option and make clients
    for pgs in pgs_ary:
      try:
	if opt.pin:
	  if opt.pin == "be":
	    pgs.be.pin('arc_ftrace', 'be.ftrace')
	  elif opt.pin == "smr":
	    pgs.smr.pin('arc_ftrace', 'smr.ftrace')
      except:
	traceback.print_exc()
	e = sys.exc_info()[0]
	print e
	pass
      if not opt.no_client:
	client = Client.Client()
	client.slotid = slotid
	client.add(slotid, 'localhost', pgs.base_port + 9)
	client.size(opt.size, slotid)
	client.tps(opt.tps, slotid)
	client_ary.append(client)
	pgs.client = client;
	slotid = slotid + 1

    ##Util.tstop('DEBUG POINT')
    print '=====> Start!'
    for client in client_ary:
      client.start(client.slotid)

    # sleep and checkpoint
    runtime = 0
    ckpt_interval = 0
    while runtime < opt.runtime:
      time.sleep(1)
      runtime = runtime + 1
      ckpt_interval = ckpt_interval + 1
      if ckpt_interval >= opt.ckpt_interval:
	for pgs in pgs_ary:
	  print 'ckeckpoint backend %d' % pgs.base_port
	  pgs.be.ckpt()
	ckpt_interval = 0


    print '=====> Done!'
    while len(client_ary) > 0:
      client = client_ary.pop(0)
      client.stop(client.slotid)

    print '======> Clients Stopped'

    print '========================== RESULT==========================' 
    for pgs in pgs_ary:
      print '>>>>>>>>>>>>>>>>>>>>> PGS(%d)' % pgs.id
      print 'seq:', pgs.smr.getseq_log()
      info = pgs.smr.info('all')
      print 'info:\n', json.dumps(info, indent=4, sort_keys=True)
      if hasattr(pgs, 'client'):
	print pgs.client.stat(pgs.client.slotid)

    for pgs in pgs_ary:
      if hasattr(pgs, 'client'):
	print 'PGS(%d)' % pgs.id, pgs.client.stat(pgs.client.slotid)
    Util.tstop('Type to destroy intermediate results')
  except:
    traceback.print_exc()
    e = sys.exc_info()[0]
    print e
  finally:
    while len(client_ary) > 0:
      client = client_ary.pop(0)
      client.kill()

    while len(pgs_ary) > 0:
      pgs = pgs_ary.pop(0)
      pgs.kill_smr()
      pgs.kill_be()

    Util.tstop('CHECK OUTPUT')
    if cm is not None:
      cm.remove_workspace()

def parse_option():
  p = OptionParser()
  p.add_option("-r", "--runtime", type="float", dest="runtime", help = "run time (seconds)", default=10)
  p.add_option("-s", "--num-slaves", type="int", dest="num_slaves", help="# slaves", default = 1)
  p.add_option("-t", "--tps", type = "int", dest="tps", help="tps", default=10000)
  p.add_option("-b", "--base-port", type = "int", dest="base_port", help="base port", default=1900)
  p.add_option("-n", action="store_true", dest="no_client", help="No client launch", default= False)
  p.add_option("-p", type="string", dest="pin", help="pin be | smr", default= None)
  p.add_option("--size", type = "int", dest="size", help="data size", default=4000)
  p.add_option("--valgrind-smr", action="store_true", dest="valgrind_smr", help="valgrind smr", default=False)
  p.add_option("--checkpoint-interval-sec", type="int", dest="ckpt_interval", help="checkpoint", default=100)
  p.add_option("--smr-opt-x", type="string", dest="smr_opt_x", help="smr option x", default= None)
  p.add_option("--smr-bin-path", type="string", dest="smr_bin_path", help="smr bin to use", default= None)
  p.add_option("--use-mem-log", action="store_true", dest="use_mem_log", help="use_mem_log", default=False)
  (options, args) = p.parse_args()
  return options;

if __name__ == '__main__':
  opt = parse_option()
  sprint(opt)
