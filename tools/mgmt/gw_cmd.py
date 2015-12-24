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

class GwCmd():
    ip = None
    port = None
    conn = None

    REQ_GW_NUM_OF_CLNTS = 'info gateway'
    RES_GW_NUM_OF_CLNTS = 'gateway_connected_clients'
    REQ_GW_COMMANDS_PROCESSED = 'info gateway'
    RES_GW_COMMANDS_PROCESSED = 'gateway_total_commands_processed'
    REQ_GW_REDIS_DISCONNS_1_1 = 'info cluster'
    RES_GW_REDIS_DISCONNS_1_1 = 'inactive_connections'
    REQ_GW_REDIS_DISCONNS_1_2 = 'info gateway'
    RES_GW_REDIS_DISCONNS_1_2 = 'gateway_disconnected_redis'

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

    def __enter__(self):
        self.conn = None
        self.conn = telnetlib.Telnet(self.ip, self.port)
        return self

    def __exit__(self, type, value, traceback):
        if self.conn != None:
            self.conn.close()

    def command(self, req, res):
        try:
            readbuf = self.request(req)
            if readbuf.find('-ERR') != -1:
                warn(red('-ERR'))
                return True, -1
            elif readbuf == '':
                warn(red('[%s:%d] Gateway reply is Null.' % (self.ip, self.port)))
                return True, -1

            reply_value = None
            for line in readbuf.split('\r\n'):
                line.strip()
                if line.find(':'):
                    tokens = line.split(':')
                    if tokens[0].strip() == res:
                        reply_value = int(tokens[1])
            if reply_value != None:
                return True, reply_value 

            return True, -1
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)

            if self.conn != None:
                self.conn.close()
                self.conn = telnetlib.Telnet(self.ip, self.port)
            return False, -1

    def request(self, req):
        try:
            self.conn.write(req + '\r\n')
            reply = self.conn.read_until('\r\n', 3)
            if len(reply) == '':
                warn(red('[%s:%d] Reply is Null.' % (self.ip, self.port)))
                return None

            if reply[1:-2].isdigit() == False:
                return reply

            size = int(reply[1:])
            
            readlen = 0
            readbuf = ''
            while readlen <= size:
                reply = self.conn.read_until('\r\n', 3)
                readlen += len(reply)
                readbuf += reply

            return readbuf
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)

            if self.conn != None:
                self.conn.close()
                self.conn = telnetlib.Telnet(self.ip, self.port)
            return None

    def info_redis_discoons(self):
        # 1.2 or higher version of gateway
        success, value = self.command(self.REQ_GW_REDIS_DISCONNS_1_2, self.RES_GW_REDIS_DISCONNS_1_2)
        if success == False:
            warn(red('[%s:%d] info_redis_disconns() fail.' % (self.ip, self.port)))
            return -1
        if value != -1:
            return value

        # 1.1 or lower version of gateway
        success, value = self.command(self.REQ_GW_REDIS_DISCONNS_1_1, self.RES_GW_REDIS_DISCONNS_1_1)
        if success == False:
            warn(red('[%s:%d] info_redis_disconns() fail.' % (self.ip, self.port)))
            return -1
        return value

    def info_num_of_clients(self):
        success, value = self.command(self.REQ_GW_NUM_OF_CLNTS, self.RES_GW_NUM_OF_CLNTS)
        if success == False:
            warn(red('[%s:%d] info_num_of_clients() fail.' % (self.ip, self.port)))
            return -1
        return value

    def info_total_commands_processed(self):
        success, value = self.command(self.REQ_GW_COMMANDS_PROCESSED, self.RES_GW_COMMANDS_PROCESSED)
        if success == False:
            warn(red('[%s:%d] info_total_commands_processed() fail.' % (self.ip, self.port)))
            return -1
        return value

