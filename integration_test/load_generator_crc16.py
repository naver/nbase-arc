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

import threading
import telnetlib
import crc16

class Crc16Client(threading.Thread):
    def __init__(self, id, ip, port, timeout, verbose=False):
        threading.Thread.__init__(self)
        self.id = id
        self.ip = ip
        self.port = port
        self.key = "crc16_%d" % id
        self.con = None
        self.timeout = timeout
        self.crc_idx = 0
        self.crc_val = 0
        self.verbose = verbose
        self.consistency = True

    def quit(self):
        self.quit = True

    def run(self):
        while self.quit == False:
            self.do()

    def do(self):
        if self.con == None:
            if self.connect() == False:
                return

            # Check crc
            rcrc = self.get_crc(False)
            if rcrc == None:
                return

            # Sync crc
            lcrc = crc16.crc16_buff(str(self.crc_idx), self.crc_val)
            if rcrc == self.crc_val or rcrc == lcrc:
                self.crc_val = rcrc
            if rcrc == lcrc:
                self.inc_crc_idx()

        # Next crc
        rcrc = self.get_crc(True)
        if rcrc == None:
            return

        lcrc = crc16.crc16_buff(str(self.crc_idx), self.crc_val)
        self.inc_crc_idx()
        self.crc_val = lcrc

        if lcrc != rcrc:
            self.consistency = False

        if self.verbose:
            print "%d l_%d r_%d" % (self.crc_idx, lcrc, rcrc)

    def connect(self):
        try:
            self.con = telnetlib.Telnet(self.ip, self.port, self.timeout)
            return True
        except:
            if self.verbose:
                print 'connect fail %s:%d' % (self.ip, self.port)
            self.con = None
            return False

    def get_crc(self, next):
        try:
            rcrc = None
            if next:
                self.con.write("crc16 %s %d\r\n" % (self.key, self.crc_idx))
                response = self.con.read_until("\r\n", self.timeout)
                if response != '':
                    rcrc = int(response[1:-2])
            else:
                self.con.write("get %s\r\n" % (self.key))
                response = self.con.read_until("\r\n", self.timeout)
                if response != '$-1\r\n':
                    response = self.con.read_until("\r\n", self.timeout)
                    if response != '':
                        rcrc = int(response)
                else:
                    return None

            if response == '':
                self.con.close()
                self.con = None
                return None

            return rcrc
        except:
            if self.verbose:
                print 'read timeout %s:%d' % (self.ip, self.port)
            self.con = None
            return None

    def inc_crc_idx(self):
        self.crc_idx += 1
        if self.crc_idx > 50000:
            self.crc_idx = 0

    def is_consistency(self):
        return self.consistency


