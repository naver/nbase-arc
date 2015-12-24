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

from fabric.api import *
from fabric.colors import *
from fabric.contrib.console import *
import sys
import traceback
import telnetlib

class RedisCmd():
    ip = None
    port = None
    conn = None

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

    def __enter__(self):
        self._init()
        return self

    def __exit__(self, type, value, traceback):
        pass

    def _init(self):
        self.REQ_TPS = 'info stats'
        self.RES_TPS = 'instantaneous_replied_ops_per_sec'
        self.REQ_KEY_COUNT = 'info keyspace'
        self.RES_KEY_COUNT = 'instantaneous_replied_ops_per_sec'
        self.REQ_RSS = 'info memory'
        self.RES_RSS = 'used_memory_rss'

    def command(self, req, res):
        try:
            self.conn = None
            self.conn = telnetlib.Telnet(self.ip, self.port)
            self.conn.write(req + '\r\n')
            reply = self.conn.read_until('\r\n', 3)
            if reply == '':
                warn(red('[%s:%d] Redis reply is Null.' % (self.ip, self.port)))
                return -1

            size = int(reply[1:])
            
            readlen = 0
            while readlen <= size:
                reply = self.conn.read_until('\r\n', 3)
                readlen += len(reply)
                reply.strip()
                if reply.find(':'):
                    tokens = reply.split(':')
                    if tokens[0].strip() == res:
                        return int(tokens[1])

            warn(red('[%s:%d] info_redis_disconns() fail.' % (self.ip, self.port)))
            return -1
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
            return -1
        finally:
            if self.conn != None:
                self.conn.close()

    def info_tps(self):
        return self.command(self.REQ_TPS, self.RES_TPS)

    def info_key_count(self):
        try:
            self.conn = None
            self.conn = telnetlib.Telnet(self.ip, self.port)

            self.conn.write('info keyspace\r\n')
            self.conn.read_until('\r\n')
            self.conn.read_until('\r\n')
            reply = self.conn.read_until('\r\n')
            if reply == '\r\n':
                return 0
            else:
                return int(reply.split(',')[0].split('=')[1])

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
            return -1
        finally:
            if self.conn != None:
                self.conn.close()

    # return : rss(Byte)
    def info_rss(self):
        return self.command(self.REQ_RSS, self.RES_RSS)

