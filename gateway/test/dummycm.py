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

import os
import asyncore
import asynchat
import socket
from optparse import OptionParser
import testenv

class CmHandler(asynchat.async_chat):
    def __init__(self, sock, addr, conf):
        asynchat.async_chat.__init__(self, sock=sock)
        self.sock = sock
        self.addr = addr
        self.ibuffer = []
        self.set_terminator("\r\n")
        self.conf = conf

    def collect_incoming_data(self, data):
        self.ibuffer.append(data)

    def found_terminator(self):
        cmd = "".join(self.ibuffer)
        self.ibuffer = []
        args = cmd.split()
        if len(args) == 0:
            return
        elif args[0] == "quit":
            self.close_when_done()
            return
        elif args[0] == "cluster_ls" and len(args) == 1:
            self.push('''{"state":"success","data":{"list":["test"]}}\r\n''')
        elif args[0] == "get_cluster_info" and len(args) == 2:
            if args[1] in self.conf:
                self.push(self.conf[args[1]])
            else:
                self.push("-ERR no such cluster:%s\r\n" % args[1])
        else:
            self.push("-ERR bad command:%s\r\n" % cmd)

class DummyCm(asyncore.dispatcher):
    def __init__(self, host, port, conf):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.conf = conf
        self.listen(5)

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            handler = CmHandler(sock, addr, self.conf)

def _parse_option():
    p = OptionParser()
    p.add_option("-a", "--addr", type="string", dest="addr", help="service ip address", default="127.0.0.1")
    p.add_option("-p", "--port", type = "int", dest="port", help="service port", default=2211)
    (options, args) = p.parse_args()
    return options

if __name__ == '__main__':
    opt = _parse_option()
    cm = DummyCm(opt.addr, opt.port, testenv.CLUSTER_CONF)
    asyncore.loop()
