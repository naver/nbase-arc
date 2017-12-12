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
import asyncore
import asynchat
import os
import socket
import subprocess
import shutil
import tempfile
import time
import redis


# --------------------------
# Test configuration (fixed)
# --------------------------
CWD = os.getcwd()

REDIS_BIN = os.path.join(os.path.abspath("../../redis-3.2.9/src"), "redis-server")
if 'REDIS_BIN' in os.environ:
    REDIS_BIN = os.environ['REDIS_BIN']

GW_BIN = os.path.join(os.path.abspath("../"), "redis-gateway")
if 'GW_BIN' in os.environ:
    GW_BIN = os.environ['GW_BIN']

CM_DIR = os.path.abspath("./")
CM_BIN = os.path.join(CM_DIR, "dummycm.py")

CLUSTER_CONF = {
        "test" : "8192\r\n0 4096 1 4096\r\n0 100\r\n1 200\r\n100:127.0.0.1:7100:7109 200:127.0.0.1:7200:7209\r\n\r\n"
}
def gw_client():
    return redis.RedisClient("localhost", 6000)

def redis1_client():
    return redis.RedisClient("localhost", 7109)

def redis2_client():
    return redis.RedisClient("localhost", 7209)

class Config:
    def __init__(self, name, basedir=CWD, redisbin = REDIS_BIN, gwbin = GW_BIN, cmbin = CM_BIN):
        self.name = name
        self.basedir = basedir
        self.redisbin = redisbin
        self.gwbin = gwbin
        self.cmbin = cmbin
        self.procs = []
        self.dir = None

    def setup(self):
        # working directory
        if not os.path.isdir (self.basedir):
            raise Exception("%s is not a directory" % self.basedir)
        self.dir = tempfile.mkdtemp(prefix=self.name + "-", dir=self.basedir)
        # starts procs
        self.procs.append(start_proc(self.dir, "cm", ["python", self.cmbin]))
        self.procs.append(start_proc(self.dir, "pgs100", [self.redisbin, "--port", "7109"]))
        self.procs.append(start_proc(self.dir, "pgs200", [self.redisbin, "--port", "7209"]))
        self.procs.append(start_proc(self.dir, "gw", [self.gwbin, "-w", "1", "-p", "6000", "-c", "127.0.0.1", "-b", "2211", "-n", "test"]))
        time.sleep(1.0) # dirty wait

    def teardown(self):
        for proc in self.procs:
            try:
                kill_proc(proc)
            finally:
                pass
        if self.dir:
            shutil.rmtree(self.dir)
            self.dir = None

# -------------------------------
# Simple subprocess proc handlers
# -------------------------------
def start_proc(basedir, logprefix, args):
    print("START PROC", args)
    basedir = os.path.abspath(basedir)
    if not os.path.isdir(basedir):
        raise Exception('%s is not a directory' % basedir)
    orgdir = os.path.abspath(os.getcwd())
    try:
        os.chdir(basedir)
        sout = open(os.path.join(basedir, '%s_stdout' % logprefix), 'w')
        serr = open(os.path.join(basedir, '%s_stderr' % logprefix), 'w')
        proc = subprocess.Popen(args, stdin=None, stdout=sout, stderr=serr)
        time.sleep(0.2) # dirty guard
        return proc
    finally:
        os.chdir(orgdir)

def kill_proc(proc, sigkill = False):
    ''' kill process gracefully opend with subprocess.Popen '''
    if sigkill:
        proc.kill()
    else:
        try:
            proc.terminate()
            count = 0
            while (proc.poll() == None and count < 10):
                time.sleep(0.1)
                count = count + 1
            if(proc.poll() == None):
                proc.kill()
                proc.wait()
        except:
            pass

# ---------
# Utilities
# ---------
def tstop(msg = 'Temporty stop.'):
    try:
        raw_input('\n' + msg + ' Enter to continue...\n')
    except:
        pass
