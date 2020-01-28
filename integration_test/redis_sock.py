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

from socket import *

class RedisClient:
    ''' simple redis client that works only in request/response mode '''
    def __init__( self, ip, port ):
        self.ip = ip
        self.port = port
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect((ip, port))

    def close(self):
        self.sock.close()

    def io_read(self, len = -1):
        s = self.sock
        if len == -1: # read a line that ends with \r\n
	    prev_c = c = None
            ret = ''
            while True:
		prev_c = c
                c = s.recv(1)
                if prev_c == '\r' and c == '\n': 
		  return ret[:-1]
		ret = ret + c
        else:
            return s.recv(len)

    def parse_response_(self):
        payload = self.io_read()
        prefix, data = payload[0], payload[1:].strip()
        if prefix == "+": # ok
            return True, data
        elif prefix == "-": # error
            return False, data
        elif prefix == ":": # integer reply
            length = int(data)
            return True, length
        elif prefix == "$": # bulk reply
            length = int(data)
            if length == -1:
                return True, None
            nextchunk = self.io_read(length+2)
	    return True, nextchunk[:-2]
        elif prefix == "*": # multibulk reply
            count = int(data)
            if count == -1:
                return True, None
            resp = []
            for i in range (0, count):
                ok, r = self.parse_response_()
                resp.append(r)
            return True, resp

    def do_request(self, cmd):
        assert (cmd.endswith('\r\n'))
        self.sock.send(cmd)
        return self.parse_response_()
