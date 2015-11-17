import os
import tempfile
import shutil
import Pg, Pgs, Conf, Log

class CM:
  def __init__(self, name):
    self.name = name
    self.dir = None

  def create_workspace(self):
    base_dir = Conf.BASE_DIR 
    if not os.path.exists (base_dir): 
      raise Exception('Base directory %s does not exist' % base_dir)
    self.dir = tempfile.mkdtemp(prefix=self.name + "-", dir=base_dir)
  
  def remove_workspace(self):
    if self.dir:
      Log.atExit()
      shutil.rmtree(self.dir)
    self.dir = None
