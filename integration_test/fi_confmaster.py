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

import util
import itertools
import json

cmds = [['ra', 'lconn', True],
        ['ra', 'lconn', False],
        ['ra', 'setquorum', True],
        ['ra', 'setquorum', False],
        ['me', 'master', True],
        ['me', 'master', False],
        ['me', 'lconn', True],
        ['me', 'lconn', False],
        ['qa', 'setquorum', True],
        ['qa', 'setquorum', False],
        ['yj', 'slave', True],
        ['yj', 'slave', False],
        ['bj', 'slave', True],
        ['bj', 'slave', False],
        ['mg', 'setquorum', True],
        ['mg', 'setquorum', False]
        ]

class ConfmasterWfFi:
    combinations = []
    index = 0

    """
    wf : array of strings
         ex) ['ra', 'qa', 'me', 'yj', 'bj', 'mg']
    where : array of strings
            ex) ['lconn', 'slave', 'master', 'setquorum']
    successFail : array of booleans
            ex) [True, False]
    """
    def __init__(self, wf, where, successFail, num):
        s = filter(lambda x: x[0] in wf and  x[1] in where and x[2] in successFail, cmds)
        for perm in itertools.combinations(s, num):
            for p in perm:
                self.combinations.append(p)

    def len(self):
        return len(self.combinations)

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.combinations):
            raise StopIteration
        else:
            item = self.combinations[self.index]
            self.index += 1
            return item

    def init(self):
        self.index = 0

    def get(self, index):
        if index < 0 or index >= len(self.combinations):
            return None
        else:
            return self.combinations[index]

"""
parameter:
    ip: confmaster ip
    port: confmaster port
raise: ValueError, socket.error
return: True of False
"""
def fi_add(fi, count, ip, port):
    cmd = ('fi_add %s %s %s %d' % (fi[0], fi[1], fi[2], count)).lower()
    reply = util.cm_command(ip, port, cmd)
    ret = json.loads(reply)
    state = ret['state']
    if 'success' == state:
        return True
    else:
        util.log("fi_add fail. cmd: \"%s\", reply: \"%s\"" % (cmd, reply))
        return False

"""
parameter:
    ip: confmaster ip
    port: confmaster port
raise: socket.error
return: an integer. 
        It returns a count of fault injection when success,
        otherwise returns -1.
"""
def fi_count(fi, ip, port):
    cmd = ('fi_count %s %s' % (fi[0], fi[1])).lower()
    reply = util.cm_command(ip, port, cmd)
    try:
        return int(reply)
    except ValueError as e:
        util.log("fi_count fail. cmd: \"%s\", reply: \"%s\"" % (cmd, reply))
        return -1

