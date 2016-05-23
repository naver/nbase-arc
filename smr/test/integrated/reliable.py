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
import random
import traceback
import sys
import time
import json
from optparse import OptionParser
import socket
import Conf, Cm, Pgs, Smr, Be, Util, Client, Conn

def setup_pgs():
    for id in range (0, gCtx.opt.copy):
        # launch pgs
        pgs = Pgs.PGS(id, 'localhost', gCtx.opt.base_port + id * 10, gCtx.cm.dir)
        pgs.start_smr()
        pgs.smr.wait_role(Smr.SMR.NONE)
        pgs.start_be()
        pgs.smr.wait_role(Smr.SMR.LCONN)
        gCtx.pgs_ary.append(pgs)
        # make client
        client = Client.Client()
        client.slotid = id
        client.add(id, 'localhost', pgs.base_port + 9)
        client.size(gCtx.opt.size, id)
        client.tps(gCtx.opt.tps, id)
        gCtx.client_ary.append(client)

def teardown_pgs():
    while len(gCtx.client_ary) > 0:
        client = gCtx.client_ary.pop(0)
        client.kill()
    while len(gCtx.pgs_ary) > 0:
        pgs = gCtx.pgs_ary.pop(0)
        pgs.kill_smr()
        pgs.kill_be()

def setup_cluster(name):
    debug = True
    conn = Conn.Conn(gCtx.opt.cm_host, gCtx.opt.cm_port)
    try:
        conn.do_request('cluster_add %s 0:1' % gCtx.opt.cluster_name, debug)
        conn.do_request('pg_add %s 0' % gCtx.opt.cluster_name, debug)
        conn.do_request('slot_set_pg %s 0:8191 0' % gCtx.opt.cluster_name, debug)
        pm_name = socket.gethostname()
        pm_ip = socket.gethostbyname(pm_name)
        for pgs in gCtx.pgs_ary:
            # pgs_add <cluster_name> <pgsid> <pgid> <pm_name> <pm_ip> <base_port> <backend_port>
            conn.do_request('pgs_add %s %d %d %s %s %d %d' % (gCtx.opt.cluster_name, pgs.id, 0, pm_name, pm_ip, pgs.base_port, pgs.base_port + 9), debug)
            conn.do_request('pgs_join %s %d' % (gCtx.opt.cluster_name, pgs.id), debug)
    except:
        print (sys.exc_info()[0])
        raise
    finally:
        conn.disconnect()

def teardown_cluster(name, force=False):
    debug = True
    conn = Conn.Conn(gCtx.opt.cm_host, gCtx.opt.cm_port)
    try:
        for pgs in gCtx.pgs_ary:
            conn.do_request('pgs_leave %s %d forced' % (gCtx.opt.cluster_name, pgs.id), debug)
            time.sleep(0.1)
            conn.do_request('pgs_del %s %d' % (gCtx.opt.cluster_name, pgs.id), debug)
        conn.do_request('pg_del %s 0' % gCtx.opt.cluster_name, debug)
        conn.do_request('cluster_del %s' % gCtx.opt.cluster_name, debug)
    except:
        print (sys.exc_info()[0])
        raise
    finally:
        conn.disconnect()

def start_clients():
    for client in gCtx.client_ary:
        client.start(client.slotid)

def stop_clients():
    while len(gCtx.client_ary) > 0:
        client = gCtx.client_ary.pop(0)
        client.stop(client.slotid)

def dirty_progress():
    runtime = 0
    ckpt_interval = 0
    prev_resp = {}
    for client in gCtx.client_ary:
        prev_resp[client.slotid] = -1

    while runtime < gCtx.opt.runtime:
        time.sleep(1)
        runtime = runtime + 1
        ckpt_interval = ckpt_interval + 1
        if ckpt_interval >= gCtx.opt.ckpt_interval:
            for pgs in gCtx.pgs_ary:
                print 'ckeckpoint backend %d' % pgs.base_port
                pgs.be.ckpt()
            ckpt_interval = 0
        if runtime % 10 == 0:
            # check progress
            for client in gCtx.client_ary:
                stat = client.stat(client.slotid)
                print ">>%d %s" % (client.slotid, stat)
                resp = int(stat['resp'])
                reconn = int(stat['reconn'])
                assert reconn == 0
                assert resp > prev_resp[client.slotid]
                prev_resp[client.slotid] = resp
            for pgs in gCtx.pgs_ary:
                bang = random.randrange(0, 4) # 0,1,2,3
                if bang == 1:
                    pgs.smr.role_lconn()
                elif bang == 2:
                    pgs.smr.delay(5000)
    return


def reliable_failover():
    try:
        gCtx.cm = Cm.CM("reliable_failover")
        gCtx.cm.create_workspace()
        setup_pgs()
        setup_cluster('reliable_failover')
        # wait initial green
        while True:
            time.sleep(0.5)
            allGreen = True
            for pgs in gCtx.pgs_ary:
                role = pgs.smr.get_role()
                if role != Smr.SMR.MASTER and role != Smr.SMR.SLAVE:
                    allGreen = False
                    break
            if allGreen:
                break
        for pgs in gCtx.pgs_ary:
            pgs.be.init_conn()

        print ("\n\n\n============================================================== START")
        start_clients()
        dirty_progress()
        stop_clients()
        print ("============================================================== END\n\n\n")
    except:
        traceback.print_exc()
        e = sys.exc_info()[0]
        print e
        Util.tstop('CHECK EXCEPTION')
    finally:
        try:
            teardown_cluster('reliable_failover', True)
        except:
            pass
        try:
            teardown_pgs()
        except:
            pass
        if gCtx.cm is not None:
            gCtx.cm.remove_workspace()

def parse_option():
    p = OptionParser()
    p.add_option("-r", "--runtime", type="float", dest="runtime", help = "run time (seconds)", default=10)
    p.add_option("-c", "--copy", type="int", dest="copy", help="copy", default = 2)
    p.add_option("-q", "--quorum", type="int", dest="quorum", help="quorum", default = 1)
    p.add_option("--cm-host", type="string", dest="cm_host", help="cm host", default="localhost")
    p.add_option("--cluster-name", type="string", dest="cluster_name", help="cluster name", default="smr_integrated_reliable")
    p.add_option("--cm-port", type="int", dest="cm_port", help="cm port", default=1122)
    p.add_option("--tps", type = "int", dest="tps", help="tps", default=10000)
    p.add_option("--base-port", type = "int", dest="base_port", help="base port", default=1900)
    p.add_option("--data-size", type = "int", dest="size", help="data size", default=4000)
    p.add_option("--checkpoint-interval-sec", type="int", dest="ckpt_interval", help="checkpoint", default=100)
    p.add_option("--smr-opt-x", type="string", dest="smr_opt_x", help="smr option x", default= None)
    p.add_option("--smr-bin-path", type="string", dest="smr_bin_path", help="smr bin to use", default= None)
    p.add_option("--use-mem-log", action="store_true", dest="use_mem_log", help="use_mem_log", default=False)
    (options, args) = p.parse_args()
    return options;

class Context:
    def __init__(self):
        self.opt = parse_option()
        self.cm = None
        self.conn = None
        self.pgs_ary = []
        self.client_ary = []

if __name__ == '__main__':
    global gCtx
    gCtx = Context()
    if gCtx.opt.use_mem_log:
        Conf.USE_MEM_LOG = True
    if gCtx.opt.smr_opt_x:
        Conf.SMR_OPT_X = gCtx.opt.smr_opt_x
    if gCtx.opt.smr_bin_path:
        Conf.OVERRIDE_SMR_BIN_PATH = gCtx.opt.smr_bin_path
    reliable_failover()
