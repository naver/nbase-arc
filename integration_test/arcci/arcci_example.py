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

import time
from arcci import *

def print_all_reply(rqst):
    while True:
        reply = api.get_reply(rqst)
        if reply is None: break
        print reply

api = ARC_API("localhost:2181", "testCluster0")

count = 10000
while count > 0:
    count -= 1

    rqst = api.create_request()
    api.append_command(rqst, "lpush mylist foo")
    api.append_command(rqst, "lpush mylist foo")
    api.append_command(rqst, "lpush mylist foo")
    api.do_request(rqst, 1000)
    #print_all_reply(rqst)
    api.free_request(rqst)

count = 10000
while count > 0:
    count -= 1

    rqst = api.create_request()
    api.append_command(rqst, "ping")
    api.do_request(rqst, 1000)
    #print_all_reply(rqst)
    api.free_request(rqst)

count = 10000
while count > 0:
    count -= 1

    rqst = api.create_request()
    api.append_command(rqst, "lrange mylist 0 10")
    api.do_request(rqst, 1000)
    #print_all_reply(rqst)
    api.free_request(rqst)

api.destroy()
