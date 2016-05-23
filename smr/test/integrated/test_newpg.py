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
import logging
import sys
import random
import filecmp
import Conf, Cm, PgNew, Pgs, Smr, Be, Util, Client

## -----------------
## replication state
## -----------------
class rState:
  def __init__(self, c, q):
    assert c >  q and q >= 0
    self.c = c # copy
    self.q = q # quorum
  def __str__(self):
    return "(%d, %d)" % (self.c, self.q)
  def __repr__(self):
    return "rState(%d, %d)" % (self.c, self.q)

## -------------------------------
## replication group configuration
## -------------------------------
class replicationGroup:
  def __init__(self, name, copy):
    self.name = name
    self.copy = copy
    self.cm = Cm.CM(self.name)
    self.cm.create_workspace()
    self.pg = None
    self.pgs_ary = []
    self.clients = []

  def init(self):
    self.pg = PgNew.PgNew(0)
    for i in range(0, self.copy):
      pgs = Pgs.PGS(i, 'localhost', 1900 + i * 10, self.cm.dir)
      self.pgs_ary.append(pgs)

    for i in range(0, self.copy):
      self.clients.append(None)

  def get_logfilename(self, name):
    return os.path.join(self.cm.dir, name)

  # 10, 17, 333, 1111, 2220
  def load_gen(self, i, tps=100000, size=2220):
    cli = Client.Client()
    cli.add(i, 'localhost', self.pgs_ary[i].base_port + 9)
    cli.tps(tps, i)
    cli.size(size, i)
    cli.start(i)
    self.clients[i] = cli

  def stop_gen(self, i):
    self.clients[i].kill()
    self.clients[i] = None

  def ckpt(self, i):
    self.pgs_ary[i].be.ckpt()

  def get_pgs(self, i):
    return self.pgs_ary[i]

  def up_pgs(self, i, run=False):
    pgs = self.pgs_ary[i]
    pgs.start_smr()
    pgs.smr.wait_role(Smr.SMR.NONE)
    pgs.start_be()
    pgs.smr.wait_role(Smr.SMR.LCONN)
    self.pg.add_member(pgs)

  def down_pgs(self, i):
    ''' kill replicator, backend and client'''
    pgs = self.pgs_ary[i]
    pgs.be.ckpt() # to save time
    pgs.kill()
    self.pg.remove_member(pgs)

  def lconn_pgs(self, i):
    pgs = self.get_pgs(i)
    role = pgs.smr.get_role()
    if role == Smr.SMR.MASTER or role == Smr.SMR.SLAVE:
      pgs.smr.role_lconn()
    elif role != Smr.SMR.LCONN:
      raise Exception('Bad pgs state')

    if pgs == self.pg.master:
      self.pg.master = None

  def stat (self, i):
    slotid = i
    resp = self.clients[i].stat(slotid)
    s = {}
    s['rqst'] = int(resp['rqst'])
    s['resp'] = int(resp['resp'])
    return s

  def reconfigure(self, copy, quorum, affinity=None, dis=None):
    self.pg.reconfigure(copy, quorum, affinity, dis)

  def finalize(self):
    for cli in self.clients:
      if cli:
	cli.kill()
    for pgs in self.pgs_ary:
      pgs.kill()
    if self.cm is not None:
      self.cm.remove_workspace()

## -----------------
## test case
## -----------------
class TestNewPg (unittest.TestCase):

  def check_progress(self, G, pgsid):
    prev_stat = G.stat(pgsid)

    sleep_count = 0
    while sleep_count < 10:
      time.sleep(0.5)
      stat = G.stat(pgsid)
      if stat['rqst'] > prev_stat['rqst']:
	return stat['rqst'] - prev_stat['rqst']
      time.sleep(0.9)
      sleep_count = sleep_count + 1

    raise Exception('No progress')

  def test_all(self):
    self._test_single_node_progress()
    self._test_role_change()
    self._test_role_change2()
    self._test_all_transition()

  def _test_single_node_progress(self):
    '''
    test progress where smr replicator moves LCONN state repeatedly
    '''
    logging.info("TEST test_single_node_progress")
    G = replicationGroup("_test_single_node_progress", 1)
    try:
      G.init()
      G.up_pgs(0)
      G.reconfigure(copy=1, quorum=0)
      G.load_gen(0)
      for i in range (0, 10):
	G.lconn_pgs(0)
	G.reconfigure(copy=1, quorum=0)
	diff = self.check_progress(G, 0)
	logging.debug('progress %d' % diff)
      G.down_pgs(0)
    except:
      #Util.tstop('Exception Occurred')
      logging.exception(sys.exc_info()[0])
      raise
    finally:
      G.finalize()

  def _test_role_change(self):
    '''
    test progress during repeated role change
    '''
    logging.info("TEST test_role_change")
    G = replicationGroup('_test_role_change', 2)
    try:
      G.init()
      G.up_pgs(0)
      G.up_pgs(1)
      G.reconfigure(copy=2, quorum=1)
      G.load_gen(0)
      G.load_gen(1)
      for i in range (0, 10):
	aff_id = i % 2
	kill_id = (aff_id + 1) % 2
	aff = G.get_pgs(aff_id)
	G.lconn_pgs(kill_id)
	G.reconfigure(copy=2, quorum=1, affinity=aff)
	progress0 = self.check_progress(G, 0)
	progress1 = self.check_progress(G, 1)
	logging.info('progress 0:%d 1:%d' % (progress0, progress1))
      G.down_pgs(0)
    except:
      #Util.tstop('Exception Occurred')
      logging.exception( sys.exc_info()[0])
      raise
    finally:
      G.finalize()

  def _test_role_change2(self):
    '''
    test progress during repeated role change (3copy)
    '''
    logging.info("TEST test_role_change2")
    G = replicationGroup('_test_role_change2', 3)
    try:
      G.init()
      G.up_pgs(0)
      G.up_pgs(1)
      G.up_pgs(2)
      G.reconfigure(copy=3, quorum=1)
      G.load_gen(0)
      G.load_gen(1)
      G.load_gen(2)
      for i in range (0, 100):
	was = G.pg.master
	G.lconn_pgs(i%3)
	G.lconn_pgs((i+1)%3)
	G.lconn_pgs((i+2)%3)
	G.reconfigure(copy=3, quorum=1, dis=was)
	progress0 = self.check_progress(G, 0)
	progress1 = self.check_progress(G, 1)
	progress2 = self.check_progress(G, 2)
	logging.info('progress 0:%d 1:%d 2:%d' % (progress0, progress1, progress2))
      G.down_pgs(0)
    except:
      #Util.tstop('Exception Occurred')
      logging.exception( sys.exc_info()[0])
      raise
    finally:
      G.finalize()

  def _test_all_transition(self):
    '''
    '''
    logging.info("TEST test_all_transition")
    COPY = 3 
    G = replicationGroup('_test_all_transion', COPY) 
    try:
      logging.info(">>>>> COPY:%d" % COPY)

      # starts with (1, 0)
      G.init()
      G.up_pgs(0)
      G.reconfigure(copy=1, quorum=0)
      curr = rState(1, 0)
      as_is_states = []
      as_is_states.append(True)
      for i in range (1, COPY):
	as_is_states.append(False)

      # generate load
      for i in range (0, COPY):
	G.load_gen(i)

      # test possible cases
      for C in range (1, COPY + 1):
	for Q in range (0, C):
	  base_state = rState(C, Q)
	  logging.info("\t base state: %s" % str(base_state))

	  curr, as_is_states = self.transit(G, curr, base_state,  as_is_states)
	  states = self.get_valid_transit_states(COPY, curr)
	  for next in states:
	    curr, as_is_states = self.transit(G, curr, next, as_is_states)
	    # rollback to base_state
	    if C < next.c:
	      curr, as_is_states = self.transit(G, curr, rState(next.c, next.c - C), as_is_states)
	    curr, as_is_states = self.transit(G, curr, base_state, as_is_states)

      # stop load generate
      for i in range (0, COPY):
	G.stop_gen(i)

      # check checkpoint file 
      for i in range (0, COPY):
	G.ckpt(i)

      for i in range (1, COPY):
	f1 = os.path.join(G.get_pgs(i-1).dir, "wc.ckpt")
	f2 = os.path.join(G.get_pgs(i).dir, "wc.ckpt")
	eq = filecmp.cmp(f1, f2)
	if not eq:
	  raise Exception("different checkpoint file %s %s" % (f1, f2))

    except:
      #Util.tstop('Exception Occurred')
      logging.exception( sys.exc_info()[0])
      raise
    finally:
      G.finalize()

  ## Utility functions
  def transit(self, G, curr, next, as_is_states, indent=""):
    logging.info ("%s transit from %s to %s" % (indent, str(curr), str(next)))
    to_be_states = self.get_to_be_states(as_is_states, curr, next)
    logging.info("%s as_is %s to_be %s" % (indent, str(as_is_states), str(to_be_states)))
    for i in range(0, len(as_is_states)):
      if as_is_states[i] == to_be_states[i]:
	continue
      elif to_be_states[i] == False:
	G.down_pgs(i)
      else:
	G.up_pgs(i)

    G.reconfigure(copy=next.c, quorum=next.q)
    as_is_states = to_be_states
    curr = next

    # check progress live pgs
    for i in range(0, len(as_is_states)):
      if as_is_states[i]:
	self.check_progress(G, i)
    return curr, as_is_states

  def get_to_be_states(self, asis, curr, next):
    # copy asis
    tobe = []
    for v in asis:
      tobe.append(v)

    # calc replacement target
    cdiff = next.c - curr.c
    rep_count = 0
    if cdiff > 0:
      target = False
      rep_count = cdiff
    elif cdiff < 0:
      target = True
      rep_count = 0 - cdiff
    else:
      return tobe

    # replace 
    rand_pos = random.randint(0, len(tobe) -1)
    for i in range (0, len(tobe)):
      idx = (rand_pos + i) % len(asis)
      if target == tobe[idx]:
	tobe[idx] = not target
	rep_count = rep_count - 1
	if rep_count == 0:
	  break
    return tobe

  def get_valid_states(self, copy):
    states = []
    for c1 in range (1, copy + 1):
      for q1 in range (0, copy):
	if (c1 > 0 and c1 <= copy) and (q1 >= 0 and q1 < copy) and (q1 < c1):
	  states.append(rState(c1, q1))
    return states

  def get_valid_transit_states(self, copy, state):
    states = []
    for c1 in range (1, copy + 1):
      for q1 in range (0, copy):
	if c1 >= state.c - state.q and ( (c1 > 0 and c1 <= copy) and (q1 >= 0 and q1 < copy) and (q1 < c1)) :
	  if not (state.c == c1 and state.q == q1):
	    states.append(rState(c1, q1))
    return states

if __name__ == '__main__':
  fn = '%s.log' % sys.argv[0]
  logging.basicConfig(filename=fn, level=logging.DEBUG, filemode='w')
  unittest.main()
