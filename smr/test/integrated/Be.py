import time
import Proc
class BE(Proc.Proc):

  def __init__ (self, pgs):
    super(BE, self).__init__(9)
    self.pgs = pgs

  def start(self, args, sin=None, sout=None, serr=None):
    super(BE, self).start(args, sin, sout, serr)
    # Note can't connect to the Be before its restart recovery is finished
    # self.init_conn()

  # -------- #
  # Commands #
  # -------- #
  def set(self, key, data):
    resp = self._conn.do_request("SET %d %s" % (key, str(data)))
    return int(resp[0])

  def reset(self):
    resp = self._conn.do_request("RESET")
    return int(resp[0])

  def get(self, key):
    resp = self._conn.do_request("GET %d" % key)
    return int(resp[0])

  def ckpt(self):
    resp = self._conn.do_request("CKPT")
    return int(resp[0])

  def ping(self):
    resp = self._conn.do_request("PING")
    return int(resp[0])

