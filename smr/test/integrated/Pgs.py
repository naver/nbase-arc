import os
import shutil
import subprocess
import Smr, Be, Conf, Log

'''
Partition Group Server interface
'''
class PGS:
  def __init__(self, id, host, base_port, base_dir):
    self.id = id
    self.host = host 
    self.base_port = base_port
    self.dir = os.path.join(base_dir, str(id) + '-' + str(base_port))
    self.smr = None
    self.be = None
    self.mgen = -1
    os.makedirs(self.dir)
    Log.createlog(self.dir)

  def __str__(self):
    return '(PGS id:%d host:%s base_port:%d mgen:%d)' % (self.id, self.host, self.base_port, self.mgen)

  def kill(self):
    self.kill_be()
    self.kill_smr()

  def lazy_create_smr(self):
    if self.smr == None:
      self.smr = Smr.SMR(self)

  def start_smr(self, quiet=False):
    self.lazy_create_smr()
    so = None
    if quiet:
      so = open(os.devnull, 'wb')
    self.smr.start(Conf.get_smr_args(self), sout=so)

  def kill_smr(self):
    if self.smr != None:
      self.smr.kill()
      self.smr = None

  def lazy_create_be(self):
    if self.be == None:
      self.be = Be.BE(self)

  def start_be(self, quiet=False):
    self.lazy_create_be()
    so = None
    if quiet:
      so = open(os.devnull, 'wb')
    self.be.start(Conf.get_be_args(self), sout=so)

  def kill_be(self):
    if self.be != None:
      self.be.kill()
      self.be = None
