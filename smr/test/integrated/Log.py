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
import subprocess
import shlex
import Util, Conf

Logs = set()

def createlog(path):
    if Conf.USE_MEM_LOG:
        os.system('%s createlog %s' % (Conf.LOG_UTIL_BIN_PATH, path))
        Logs.add(path)

def syncdeletelog(path):
    if path in Logs:
        os.system('%s synclog %s' % (Conf.LOG_UTIL_BIN_PATH, path))
        os.system('%s deletelog %s' % (Conf.LOG_UTIL_BIN_PATH, path))
        Logs.discard(path)

def atExit():
    while len(Logs) > 0:
        path = Logs.pop()
        os.system('%s deletelog %s' % (Conf.LOG_UTIL_BIN_PATH, path))

def communicate(cmd):
    popen = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return popen.communicate()

def datadump_communicate(path, begin_seq, end_seq = None, oflag = 'Tshld'):
    prog = Conf.LOG_UTIL_BIN_PATH
    if end_seq != None:
        cmd = '%s -o %s datadump %s %d %d' % (prog, oflag, path, begin_seq, end_seq)
    else:
        cmd = '%s -o %s datadump %s %d' % (prog, oflag, path, begin_seq)
    return communicate(cmd)

def dumpbytime_communicate(path, begin_time, end_time = None, oflag = 'Tshld'):
    prog = Conf.LOG_UTIL_BIN_PATH
    if end_time != None:
        cmd = '%s -o %s dumpbytime %s %d %d' % (prog, oflag, path, begin_time, end_time)
    else:
        cmd = '%s -o %s dumpbytime %s %d' % (prog, oflag, path, begin_time)
    return communicate(cmd)

def mincorelog_communicate (path, seq = None):
    prog = Conf.LOG_UTIL_BIN_PATH
    if seq != None:
        cmd = '%s mincorelog %s %d' % (prog, path, seq)
    else:
        cmd = '%s mincorelog %s' % (prog, path)
    return communicate(cmd)

def decachelog_communicate (path, seq = None, force = False):
    prog = Conf.LOG_UTIL_BIN_PATH

    if seq == None:
        cmd = '%s decachelog %s' % (prog, path)
    else:
        if force == True:
            suffix = " force"
        else:
            suffix = ""
        cmd = '%s decachelog %s %d%s' % (prog, path, seq, suffix)

    return communicate(cmd)
