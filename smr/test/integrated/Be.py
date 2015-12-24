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

