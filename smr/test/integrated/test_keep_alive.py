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

import unittest
import time
import Util, Conf, Cm, Pg, Pgs, Smr, Be

class TestKeepAlive (unittest.TestCase):

  def test_slave_keep_alive(self):
    cm = None
    pgs1 = None
    pgs2 = None
    pgs3 = None
    try:
      cm = Cm.CM("test_pgs")
      cm.create_workspace()
      pg = Pg.PG(0)

      # pgs1 --> master
      pgs1 = Pgs.PGS(0, 'localhost', 1900, cm.dir)
      pg.join(pgs1, start=True)
      pgs1.smr.wait_role(Smr.SMR.MASTER)

      # pgs2 --> slave
      pgs2 = Pgs.PGS(1, 'localhost', 1910, cm.dir)
      pg.join(pgs2, start=True)
      pgs2.smr.wait_role(Smr.SMR.SLAVE)

      # send command
      r = pgs2.be.set(0, '100')
      assert r >= 0

      # check master idle time grows close upto 1 second.
      for i in range(0, 9):
	res = pgs1.smr.info('slave')
        line = res['slave']['slave_' + str(pgs2.id)]
	assert line != None
	idle_msec = int(line[(line.find('idle_msec=') + len('idle_msec=')):])
	assert idle_msec < i * 100 + 500
	time.sleep(0.1)  # sleep 100 msec

      # make pgs3 and force join
      pgs3 = Pgs.PGS(1, 'localhost', 1920, cm.dir)
      pg.join(pgs3, start=True, Force=True)
      pgs3.smr.wait_role(Smr.SMR.SLAVE)
      pgs2.smr.wait_role(Smr.SMR.LCONN)

      # check
      r2 = pgs3.be.get(0)
      assert r == r2

      # current configuration (pgs1, pgs3)
      pgs1.smr.confset('slave_idle_timeout_msec', 100)
      pgs3.smr.wait_role(Smr.SMR.LCONN)

    finally:
      if pgs1 is not None:
	pgs1.kill_smr()
	pgs1.kill_be()
      if pgs2 is not None:
	pgs2.kill_smr()
	pgs2.kill_be()
      if pgs3 is not None:
	pgs3.kill_smr()
	pgs3.kill_be()
      if cm is not None:
	cm.remove_workspace()

if __name__ == '__main__':
  unittest.main()
