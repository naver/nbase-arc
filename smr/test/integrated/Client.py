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
import subprocess
import sys
import Conf

class Client:
  def __init__(self):
    self._proc = subprocess.Popen(Conf.get_client_args(), stdin=subprocess.PIPE, stdout=subprocess.PIPE)

  def kill(self):
    if self._proc == None:
      return
    self._proc.terminate()
    count = 0
    try:
      while (self._proc.poll() == None and count < 10):
        time.sleep(0.1)
        count = count + 1
      if(self._proc.poll() == None):
        self._proc.kill()
        self._proc.wait()
    except:
      e = sys.exc_info()[0]
      print e
    finally:
      self._proc = None

  def _do_command(self, command):
    self._proc.stdin.write('%s\n' % command)
    self._proc.stdin.flush()
    resp = self._proc.stdout.readline()
    if resp[0] == '-1':
      raise resp
    return resp.split()[1:]

  # -------- #
  # Commands #
  # -------- #
  def add(self, id, host, port):
    resp = self._do_command('ADD %d %s %d' % (id, host, port))
    return True

  def tps(self, tps, id):
    resp = self._do_command('TPS %d %d' % (tps, id))
    return True

  def size(self, size, id):
    resp = self._do_command('SIZE %d %d' % (size, id))
    return True

  def start(self, id):
    resp = self._do_command('START %d' % id)
    return True

  def stop(self, id):
    resp = self._do_command('STOP %d' % id)
    return True

  def stat(self, id):
    resp = self._do_command('STAT %d' % id)
    return resp
