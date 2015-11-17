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
    self.commit = 0
    self.hist = []	     # index is generation, value is sequence number
    self.master = None       # master PGS
    self.members = {}        # key is pgsid, value is pgs

  def add_member (self, pgs):
    if pgs.id in self.members:
      logging.exception('Already a member %d' % pgs.id)
    else:
      self.members[pgs.id] = pgs

  def remove_member (self, pgs):
    if pgs.id in self.members:
      del self.members[pgs.id]
      if pgs == self.master:
	self.master = None
    else:
      logging.exception('Not such pgs')

  def check_master_join (self, pgs, log_commit, log_msg):
    if self.master != None:
      return False,  'PG has master already'
    elif log_commit < self.commit:
      return False, 'PG join constraint violation'
    else:
      return True, log_msg

  def check_slave_join (self, pgs, log_commit, log_msg):
    if self.master == None:
      return False,  'PG has no master'
    elif pgs.mgen == self.mgen:
      return True, log_msg
    else:
      assert pgs.mgen < self.mgen
      if log_commit > self.hist[pgs.mgen + 1]:
	return False,  'PG join constraint violation (brain split case)'
      else:
	return True, min(log_msg, self.hist[pgs.mgen+1])

  def check_pg_join (self, pgs, role):
    seqs = pgs.smr.getseq_log()
    log_commit = seqs['commit']
    log_msg = seqs['max']

    if role == MASTER:
      return self.check_master_join(pgs, log_commit, log_msg)
    elif role == SLAVE:
      return self.check_slave_join(pgs, log_commit, log_msg)
    else:
      logging.exception('Invalid role')

  def reconfigure (self, quorum, affinity=None, dis=None):
    logging.info('>> reconfiguration quorum:%d, affinity:%s, dis:%s, master: %s' % (quorum, str(affinity), str(dis), str(self.master))) 

    # master election
    if self.master == None:
      max_seq = -1
      master = None
      for pgs in self.members.values():
	ok, seq = self.check_pg_join(pgs, MASTER)
	if ok:
	  # check affinity
	  if master != None and master == affinity:
	    continue
	  elif seq >= max_seq:
	    if master != None and pgs == dis:
	      continue
	    max_seq = seq
	    master = pgs
      if master:
	logging.info('\tMASTER: pgs:%d seq:%d' % (master.id, max_seq))
	master.smr.role_master(master.id, quorum, max_seq)
	self.mgen = self.mgen + 1
	self.hist.append(max_seq)
	master.mgen = self.mgen
	self.master = master
      else:
	logging.exception('Master election failed (no candidate)')

    # slave join
    for pgs in self.members.values():
      if pgs == self.master:
	continue
      role = pgs.smr.get_role()
      if role == SLAVE:
	assert pgs.mgen == self.mgen
	continue
      elif role != LCONN:
	logging.exception('Invalid state')
      ok, seq = self.check_pg_join (pgs, SLAVE)
      if not ok:
	logging.exception('Slave join failed %s' % str(seq))
      logging.info('\tSLAVE: pgs:%d seq:%d' % (pgs.id, seq))
      pgs.smr.role_slave(pgs.id, self.master.host, self.master.base_port, seq)
      pgs.mgen = self.mgen

    # adjuist quorum
    curr_quorum = self.master.smr.getquorum()
    if curr_quorum != quorum:
      self.master.smr.setquorum(quorum)

    logging.info('<<\n')
