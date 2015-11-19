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
