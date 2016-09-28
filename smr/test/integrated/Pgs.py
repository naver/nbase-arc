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
import shutil
import subprocess
import Smr, Be, Conf, Log

'''
Partition Group Server interface
'''
class PGS:
    def __init__(self, id, host, base_port, base_dir):
        self.id = id
        self.host = host
        self.base_port = base_port
        self.dir = os.path.join(base_dir, str(id) + '-' + str(base_port))
        self.smr = None
        self.be = None
        self.mgen = -1
        os.makedirs(self.dir)
        Log.createlog(self.dir)

    def __str__(self):
        return '(PGS id:%d host:%s base_port:%d mgen:%d)' % (self.id, self.host, self.base_port, self.mgen)

    def kill(self):
        self.kill_be()
        self.kill_smr()

    def lazy_create_smr(self):
        if self.smr == None:
            self.smr = Smr.SMR(self)

    def start_smr(self, quiet=False):
        self.lazy_create_smr()
        so = None
        if quiet:
            so = open(os.devnull, 'wb')
        self.smr.start(Conf.get_smr_args(self), sout=so)

    def kill_smr(self, sigkill = False):
        if self.smr != None:
            self.smr.kill(sigkill)
            self.smr = None

    def lazy_create_be(self):
        if self.be == None:
            self.be = Be.BE(self)

    def start_be(self, quiet=False):
        self.lazy_create_be()
        so = None
        if quiet:
            so = open(os.devnull, 'wb')
        self.be.start(Conf.get_be_args(self), sout=so)

    def kill_be(self):
        if self.be != None:
            self.be.kill()
            self.be = None
