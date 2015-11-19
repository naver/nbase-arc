import os
import signal
import util


class Process:
  def __init__( self, uid, p ):
    self.uid = uid
    self.p = p

  def getPopen( self ):
    return self.p


class ProcessMgmt:
  def __init__( self ):
    self.dic = dict()
 
  def __del__( self ):
    self.kill_all()

  def insert( self, uid, p ):
    if self.dic.has_key( uid ):
      util.log('[INFO] Process(%s) already exists.' % uid)

      # TODO : return new uid to caller
      for i in range( 1000000 ):
        new_uid = '%s%d' % (uid, i)
        if self.dic.has_key( new_uid ) is False:
          o = Process( uid, p )
          self.dic[new_uid] = o
          return o

    else:
      o = Process( uid, p )
      self.dic[uid] = o 
      return o

    return None

  def delete( self, uid ):
    if not self.dic.has_key( uid ):
      util.log('Process(%s) does not exist.' % uid)
      return -1

    self.dic.pop( uid )
    return 0

  def get( self, uid ):
    if not self.dic.has_key( uid ):
      util.log('Process(%s) does not exist.' % uid)
      return None

    return self.dic.get( uid )

  def kill_all( self ):
    for k, v in self.dic.items():
      try:
        p = v.getPopen()
        util.kill_proc( p )
      except OSError:
        util.log('Invalid process. uid=%s' % (k))
    self.delete_all()

  def kill( self, uid ):
    p = self.get( uid ) 
    if p is None:
      return
    util.kill_proc( p.getPopen() )
    self.delete( uid )
    
  def delete_all( self ):
    self.dic.clear()

