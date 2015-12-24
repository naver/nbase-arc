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

class PG:
  def __init__(self, id):
    self.id = id
    self.pgs_map = {}        # key is pgsid, value is pgs
    self.mgen_seq = []       # index is generation, value is sequence number
    self.mgen_pgsid = []     # index is generation, value is pgs id
    self.pgsid_mgen_map = {} # index is pgsid, value is master generation
    self.master = None

  # -------- #
  # exported #
  # -------- #
  def get_quorum(self):
    return len(self.pgs_map)/2

  def adjust_quorum(self):
    q = self.get_quorum()
    self.master.smr.setquorum(q)

  def master_election(self):
    # master generation logic (this logic will be simplified)
    # 1. use log_commit
    # 2. if it was a master use min(log_commit, be_commit)
    assert self.master == None
    cseq = -1 
    cand = None
    was_master_pgsid = None
    if len(self.mgen_pgsid) > 0:
      was_master_pgsid = self.mgen_pgsid[-1]

    for pgs in self.pgs_map.values():
      seqs = pgs.smr.getseq_log()
      if pgs.id == was_master_pgsid:
	seq = seqs['be_sent']
      else:
	seq = seqs['commit']
      if seq > cseq:
	cseq = seq
	cand = pgs

    if cand == None:
      print '[WRN] No Pgs'
      return 
    cand.smr.role_master(pgs.id, self.get_quorum(), cseq)

    self.mgen_seq.append(cseq)
    self.mgen_pgsid.append(cand.id)
    self.pgsid_mgen_map[cand.id] = len(self.mgen_seq) - 1
    self.master = cand

    for pgs in self.pgs_map.values():
      if pgs is not cand:
	self.slave_join(pgs)

  def slave_join(self, pgs, mgen = -1):
    assert self.master != None

    if mgen == -1:
      if pgs.id in self.pgsid_mgen_map:
	mgen = self.pgsid_mgen_map[pgs.id]
      else:
	mgen = len(self.mgen_seq) - 1

    seqs = pgs.smr.getseq_log()
    cseq =  seqs['commit']
    if mgen + 1 < len(self.mgen_seq):
      if cseq > self.mgen_seq[mgen + 1]:
	raise Exception('Can not join: pgs commit seq:%d, next master seq:%d' % (cseq, self.mgen_seq[mgen + 1]))
    pgs.smr.role_slave(pgs.id, self.master.host, self.master.base_port, cseq)
    self.pgsid_mgen_map[pgs.id] = len(self.mgen_seq) - 1


  def leave(self, pgsid, kill = False):
    if pgsid not in self.pgs_map:
      raise Exception('There is no pgs. id:%d' % pgsid)

    pgs = self.pgs_map[pgsid]
    old_role = pgs.smr.get_role()
    del self.pgs_map[pgsid]
    if self.master is pgs:
      self.master = None
    
    if old_role > Smr.SMR.LCONN:
      if kill:
	pgs.kill()
      else:
	pgs.smr.role_lconn()
      if old_role == Smr.SMR.SLAVE:
	self.adjust_quorum()
      else:
	assert old_role == Smr.SMR.MASTER
	self.master_election()
    # do not reove from pgsid_mgen_map 
    

  def join(self, pgs, start = False, Force = False):
    if not Force and pgs.id in self.pgs_map:
      raise Exception('There is already a pgs. id:%d' % pgs.id)

    if start:
      pgs.start_smr()
      pgs.smr.wait_role(Smr.SMR.NONE)
      pgs.start_be()
      pgs.smr.wait_role(Smr.SMR.LCONN)

    self.pgs_map[pgs.id] = pgs
    if self.master == None:
      self.master_election()
    else:
      self.slave_join(pgs)
    pgs.be.init_conn()
