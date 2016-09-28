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

import os
import sys
import subprocess
import signal
import time
import Conn, Conf

class Proc(object):

    def __init__(self, port_off):
        self._proc = None
        self._conn = None
        self._port_off = port_off

    def start(self, args, sin=None, sout=None, serr=None):
        old_path = os.path.abspath(os.getcwd())
        os.chdir(self.pgs.dir)
        try:
            self._proc = subprocess.Popen(args, stdin=sin, stdout=sout, stderr=serr)
            time.sleep(0.2) # dirty guard
        finally:
            os.chdir(old_path)

    def kill(self, sigkill = False):
        try:
            if self._conn is not None:
                self._conn.disconnect()
            if self._proc is not None:
                if sigkill:
                    self._proc.kill()
                else:
                    try:
                        self._proc.terminate()
                        count = 0
                        while (self._proc.poll() == None and count < 10):
                            time.sleep(0.1)
                            count = count + 1
                        if(self._proc.poll() == None):
                            self._proc.kill()
                            self._proc.wait()
                    except:
                        pass
        finally:
            self._proc = None
            self._conn = None

    def init_conn(self):
        if self._conn is not None:
            return
        elif self._proc is None:
            raise Exception('Process terminated')

        # check proc
        if self._proc.poll() != None:
            ##import pdb; pdb.set_trace()
            raise Exception('Process terminated with exit code: %d' % self._proc.poll())

        try_count = 0
        conn = None
        e = None
        while try_count < 60: # 0.5 * 60 = 30 sec.
            try:
                conn = Conn.Conn(self.pgs.host, self.pgs.base_port + self._port_off)
                conn.lazy_connect()
                break
            except:
                e = sys.exc_info()[0]
                time.sleep(0.5)
                try_count = try_count + 1

        if conn == None:
            self._conn = None
            raise Exception('Failed to connect:%s' % str(e))
        else:
            self._conn = conn

    def pin(self, tool, infilename):
        infile = os.path.join(Conf.BASE_DIR, 'pin', infilename)
        if not os.path.exists(infile):
            raise Exception('%s does not exists' % infile)
        cmd = '%s -pid %d -t %s/%s.so -i %s' % (Conf.PIN, self._proc.pid, Conf.PINTOOL_BASE, tool, infile)
        os.system(cmd)
