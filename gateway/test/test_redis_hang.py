# 
# Copyright 2017 Naver Corp.
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
import redis
import socket
import time
import testenv
import threading
from datetime import datetime

class buzz_client(threading.Thread):
    def __init__(self, counts = 1000, timeout = 1.0):
        threading.Thread.__init__(self)
        self.counts = counts
        self.timeout = timeout
        self.sleepsec = timeout - 0.1
        self.okrun = True

    def quit(self):
        self.okrun = False

    def run(self):
        # make pipelined raw request
        commands = []
        for i in range(100):
            commands.append("SET %d %d\r\n" % (i, i))
        # counts times pipelined request with timeout
        cli = testenv.gw_client()
        for count in range(self.counts):
            if not self.okrun:
                return
            err = False
            try:
                cli.settimeout(self.timeout)
                resp = cli.do_pipelined_raw_request(commands)
                time.sleep(self.sleepsec)
            except socket.timeout:
                err = True
            except:
                err = True
            finally:
                if err:
                    cli = testenv.gw_client()
                time.sleep(self.sleepsec)

def timestr():
    return str(datetime.now())

class TestTest(unittest.TestCase):
    def test_config(self):
        conf = testenv.Config("test")
        buzz = None
        try:
            conf.setup()

            buzz = buzz_client(counts = 1000)
            buzz.start()

            st = time.time()
            redis1 = testenv.redis1_client()
            redis1.sock.sendall('debug sleep 10\r\n') 

            gw = testenv.gw_client()
            gw.do_inline_request('dbsize')
            et = time.time()
            assert (et - st < 6.0)
        finally:
            conf.teardown()
            if buzz is not None:
                buzz.quit()
            
if __name__ == '__main__':
    unittest.main()
