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

import time
import Smr
import logging

MASTER = Smr.SMR.MASTER
SLAVE = Smr.SMR.SLAVE
LCONN = Smr.SMR.LCONN

class PgNew:
  def __init__(self, id):
    self.id = id
    self.mgen = -1
    self.hist = []	     # index is generation, value is sequence number
    self.master = None       # master PGS
    self.members = {}        # key is pgsid, value is pgs

  def add_member (self, pgs):
    if pgs.id in self.members:
      raise Exception('Already a member %d' % pgs.id)
    else:
      self.members[pgs.id] = pgs

  def remove_member (self, pgs):
    if pgs.id in self.members:
      del self.members[pgs.id]
      if pgs == self.master:
	self.master = None
    else:
      raise Exception('Not such pgs')

  def check_master_join (self, pgs, log_commit, log_msg):
    if self.master != None:
      return False,  'PG has master already'
    else:
      return True, log_msg

  def check_slave_join (self, pgs, log_commit, log_msg):
    if self.master == None:
      return False,  'PG has no master'
    cand = []
    cand.append(log_msg)
    logging.info('pgs.mgen: %d' % pgs.mgen)
    for mgen in range(pgs.mgen + 1, self.mgen + 1):
      cand.append(self.hist[mgen])
    logging.info(cand)
    return True, min(cand)

  def check_pg_join (self, pgs, role):
    seqs = pgs.smr.getseq_log()
    log_commit = seqs['commit']
    log_msg = seqs['max']

    if role == MASTER:
      return self.check_master_join(pgs, log_commit, log_msg)
    elif role == SLAVE:
      return self.check_slave_join(pgs, log_commit, log_msg)
    else:
      raise Exception('Invalid role')

  def reconfigure (self, copy, quorum, affinity=None, dis=None):
    logging.info('>> reconfiguration copy:%d quorum:%d, affinity:%s, dis:%s, master:%s' % 
	(copy, quorum, str(affinity), str(dis), str(self.master))) 

    assert  copy > quorum

    # ---------------
    # master election
    # ---------------
    if self.master == None:
      master = None
      max_seq = -1

      # find candidates. A candidate must win at least copy - quourm - 1
      cand = []
      for pgs in self.members.values():
	ok, seq = self.check_pg_join(pgs, MASTER)
	cand.append([seq, pgs])
      cand.sort(key=lambda tup: tup[0])

      for c in range (copy - quorum, len(cand)):
	cand.pop(0)

      if len(cand) < copy - quorum:
	raise Exception('Need more pgs to properly select a master')

      # exclude dis if possible
      if len(cand) > 1 and dis != None:
	dis_idx = -1
	for idx in range(0, len(cand)):
	  if cand[idx][1] == dis:
	    dis_idx = idx
	    break
	if dis_idx != -1:
	  cand.pop(dis_idx)

      # elect affinity if possible
      if affinity != None:
	for tup in cand:
	  if tup[1] == affinity:
	    master = affinity
	    max_seq = tup[0]
	    break

      if master == None:
	master = cand[0][1]
	max_seq = cand[0][0]
      
      if master:
	logging.info('\tMASTER: pgs:%d seq:%d' % (master.id, max_seq))
	master.smr.role_master(master.id, quorum, max_seq)
	self.mgen = self.mgen + 1
	logging.info('master generation increased:%d' % self.mgen)
	self.hist.append(max_seq)
	logging.info(self.hist)
	master.mgen = self.mgen
	self.master = master
      else:
	raise Exception('Master election failed (no candidate)')

    # ----------
    # slave join
    # ----------
    for pgs in self.members.values():
      if pgs == self.master:
	continue
      role = pgs.smr.get_role()
      if role == SLAVE:
	assert pgs.mgen == self.mgen
	continue
      elif role != LCONN:
	raise Exception('Invalid state')
      ok, seq = self.check_pg_join (pgs, SLAVE)
      if not ok:
	raise Exception('Slave join failed %s' % str(seq))
      logging.info('\tSLAVE: pgs:%d seq:%d' % (pgs.id, seq))
      pgs.smr.role_slave(pgs.id, self.master.host, self.master.base_port, seq)
      pgs.mgen = self.mgen

    # adjuist quorum
    curr_quorum = self.master.smr.getquorum()
    if curr_quorum != quorum:
      self.master.smr.setquorum(quorum)

    logging.info('<<\n')
