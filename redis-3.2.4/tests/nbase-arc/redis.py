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
from itertools import imap
import re

class RedisClient:
    ''' simple (and slow) redis client that works only in request/response mode '''
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
                if not c:
                    raise 'socket receive error'
                if prev_c == '\r' and c == '\n': 
		  return ret[:-1]
		ret = ret + c
        else:
            c = s.recv(len)
            if not c:
                raise 'socket receive error'
            return c

    def read_response(self):
        payload = self.io_read()
        prefix, data = payload[0], payload[1:].strip()
        if prefix == "+": # ok
            return data
        elif prefix == "-": # error
            return data
        elif prefix == ":": # integer reply
            len = int(data)
            return len
        elif prefix == "$": # bulk reply
            len = int(data)
            if len == -1:
                return None
            nextchunk = self.io_read(len+2)
	    return nextchunk[:-2]
        elif prefix == "*": # multibulk reply
            count = int(data)
            if count == -1:
                return None
            resp = []
            for i in range (0, count):
                r = self.read_response()
                resp.append(r)
            return resp
        else:
            raise 'read_response: Protocol error'

    def raw_write(self, data):
        self.sock.sendall(data)

    def do_raw_request(self, data):
        ''' send request confirming RESP '''
        self.sock.sendall(data)
        return self.read_response()

    def do_inline_request(self, cmd):
        ''' inline request'''
        return self.do_raw_request(cmd + '\r\n')

    def do_generic_request(self, *args):
        '''array of bulk request'''
        rqst = ''.join(('*', str(len(args)),'\r\n'))
        for arg in imap(encode, args):
            rqst = ''.join((rqst, '$', str(len(arg)), '\r\n', arg, '\r\n'))
        return self.do_raw_request(rqst)

def encode(value):
    "Return a bytestring representation of the value"
    if isinstance(value, bytes):
        return value
    elif isinstance(value, (int, long)):
        value = str(value)
    elif isinstance(value, float):
        value = repr(value)
    elif not isinstance(value, basestring):
        value = unicode(value)
    if isinstance(value, unicode):
        value = value.encode('utf-8')
    return value
    
def rr_assert_equal (l1, r2):
    if str(l1) != str(r2):
        print("\n===ASSERT EQUAL===")
        print(str(l1))
        print(str(r2))
        print("==================")
    assert str(l1) == str(r2)

def rr_assert_substring(subs, r):
    assert re.search(subs, r) != None

def rr_toint(r):
    return int(r)
