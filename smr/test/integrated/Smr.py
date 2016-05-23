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

import Proc
import time

class SMR (Proc.Proc):
  NONE = 0
  LCONN = 1
  MASTER = 2
  SLAVE = 3

  def __init__(self, pgs):
    super(SMR, self).__init__(3)
    self.pgs = pgs

  def start(self, args, sin=None, sout=None, serr=None):
    super(SMR, self).start(args, sin, sout, serr)
    self.init_conn()

  # -------- #
  # Commands #
  # -------- #
  def get_role(self):
    resp = self._conn.do_request('ping')
    return int(resp[0].split()[1])

  def role_lconn(self):
    resp = self._conn.do_request('role lconn')
    seg = resp[0].split()
    if seg[0] != '+OK':
      raise Exception (resp[0])

  def role_master(self, nid, quorum, cseq):
    resp = self._conn.do_request('role master %d %d %d' % (nid, quorum, cseq))
    seg = resp[0].split()
    if seg[0] != '+OK':
      raise Exception (resp[0])

  def role_slave(self, nid, host, port, cseq):
    resp = self._conn.do_request('role slave %d %s %d %d' % (nid, host, port, cseq))
    seg = resp[0].split()
    if seg[0] != '+OK':
      raise Exception (resp[0])

  def setquorum(self, q):
    resp = self._conn.do_request('setquorum %d' % q)
    seg = resp[0].split()
    if seg[0] != '+OK':
      raise Exception (resp[0])

  def getquorum(self):
    # getquorum request DOES NOT starts with +OK nor -ERR
    resp = self._conn.do_request('getquorum')
    seg = resp[0].split()
    return int(seg[0])

  def wait_role(self, role, timeout = 2000):
    wait_time_sec = 0.0
    while wait_time_sec * 1000 < timeout:
      if role == self.get_role():
        return wait_time_sec * 1000
      time.sleep(0.1)
      wait_time_sec = wait_time_sec + 0.1
    raise Exception("Timeout:%f" % (wait_time_sec * 1000))

  def getseq_log(self):
    # e.g.) +OK log min:0 commit:0 max:0 [be_sent:0]
    resp = self._conn.do_request('getseq log')
    seg = resp[0].split()
    if seg[0] != '+OK':
      raise Exception(resp[0])
    seqs = {}
    for i in range(2, len(seg)):
      seqs[seg[i].split(':')[0]] = int(seg[i].split(':')[1])
    return seqs

  def confget(self,item):
    resp = self._conn.do_request('confget %s' % str(item))
    seg = resp[0].split()
    if seg[0] != '+OK':
      raise Exception(resp[0])
    return seg[1:]

  def delay(self,msec):
    resp = self._conn.do_request('fi delay sleep 1 %d' % msec)
    seg = resp[0].split()
    if seg[0] != '+OK':
      raise Exception(resp[0])
    return True

  def confset(self,item, value):
    resp = self._conn.do_request('confset %s %s' % (str(item), str(value)))
    seg = resp[0].split()
    if seg[0] != '+OK':
      raise Exception(resp[0])
    return seg[1:]

  def info(self, section='all'):
    ''' returns map(section --> map(key->value))'''
    resp = self._conn.do_request('info %s' % str(section))
    secmap = {}
    sec = "unknown"
    for line in resp:
      line = line.strip()
      if line == "":
	continue
      elif line[0] == '#':
	sec = line[1:].lower()
      else:
	pos = line.find(':')
	if pos < 0:
	  continue
	key = line[:pos]
	val = line[pos+1:]
	if sec not in secmap:
	  secmap[sec] = {}
	secmap[sec][key] = val
    return secmap
