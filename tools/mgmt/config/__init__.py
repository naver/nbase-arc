import os
import sys
import tempfile
import traceback
from fabric.colors import *
from fabric.api import *
from conf_mnode import *
from conf_dnode import *
from conf_cluster import *
from conf_redis import *
from conf_gateway import *

def check_config():
    # Check attributes
    attrs = [
             # Management node
             "CONF_MASTER_IP", 
             "CONF_MASTER_PORT", 
             "CONF_MASTER_MGMT_CONS",
             "LOCAL_BINARY_PATH", 
             "MIN_TIME_TO_ATTEMPT_MIG2PC",
             "USERNAME", 
             "REMOTE_NBASE_ARC",
             "REMOTE_BIN_DIR", 
             "REMOTE_PGS_DIR", 
             "REMOTE_GW_DIR",
             "ARC_BASH_PROFILE",
             "SHELL",

             # Data node
             "REDIS_VERSION", 
             "GW_VERSION",
             "SMR_VERSION", 
             "GW_BASE_PORT",
             "PGS_BASE_PORT",
             "BGSAVE_BASE",
             "ID_GAP",
             "CRONSAVE_BASE_HOUR",
             "CRONSAVE_BASE_MIN",
             "NUM_WORKERS_PER_GATEWAY",
             "NUM_CLNT_MIN",
             "CLIENT_TIMEOUT"]


    ok = True
    for attr in attrs:
        if attr in globals() == False:
            print "%s is not defined in config." % magenta(attr)
            ok = False
        elif globals()[attr] == None:
            print "%s is empty." % magenta(attr)
            ok = False

    return ok

# redis configuration to file
# parameter : conf = {"redis_config_key" : "redis_config_value", ...}
# return : path of temporary-redis-config-file
def make_redis_conf_file(conf):
    try:
        (fd, filename) = tempfile.mkstemp()
        tfile = os.fdopen(fd, "w")
        for e in sorted(conf.iteritems(), key=lambda (k,v): (k)):
            k = e[0]
            v = e[1]
            if k == "cronsave":
                for save in v:
                    tfile.write("%s %d %d" % (k, save[0], save[1]) + os.linesep)
            elif k == "client-output-buffer-limit":
                for o in v:
                    tfile.write("%s %s" % (k, o) + os.linesep)
            else:
                tfile.write("%s %s" % (k, v) + os.linesep)
        tfile.close()
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
        return None

    return filename

# make redis configuartion
# return : conf = {"redis_config_key" : "redis_config_value", ...}
def make_redis_conf(cluster_name, smr_base_port, redis_port, cronsave_hour, cronsave_min, cronsave_num=1):
    try:
        conf = {}
        for e in REDIS_CONFIG:
            if e[0] == 'cronsave':
                base = cronsave_hour * 60 + cronsave_min + (smr_base_port - PGS_BASE_PORT)
                intv = (24 * 60) / cronsave_num  
                save = []
                for i in range(cronsave_num):
                    t = base + intv * i
                    h = (t / 60) % 24
                    m = (t % 60) % 60
                    save.append([m,h])
                conf["cronsave"] = save
            elif e[0] == "smr-local-port":
                conf[e[0]] = str(smr_base_port)
            elif e[0] == "port":
                conf[e[0]] = str(redis_port)
            else:
                conf[e[0]] = e[1]

        cluster_conf = get_cluster_conf(cluster_name)
        if cluster_conf != None:
            for k, v in cluster_conf['redis'].iteritems():
                if k == "client-output-buffer-limit":
                    for confv in conf[k]:
                        find = False
                        for i in range(len(confv)):
                            if confv[i].split(" ")[0] == v.split(" ")[0]:
                                confv[i] = v
                                find = True
                        if find == False:
                            confv.append(v)
                else:
                    conf[k] = v
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
        return None

    return conf

def get_cluster_conf(cluster_name):
    conf = filter(lambda x: x['cluster_name'] == cluster_name, CLUSTER_CONFIG)
    if len(conf) > 1:
        warn(red('Too many configurations for %s' % cluster_name))
        return None
    elif len(conf) == 0:
        return None
    return conf[0]

def get_gw_additional_option():
    if GW_ADDITIONAL_OPTION.has_key("opt"):
        return GW_ADDITIONAL_OPTION["opt"]
    else:
        return ""

def make_bash_profile_file(arc_path):
    try:
        (fd, filename) = tempfile.mkstemp()
        tfile = os.fdopen(fd, "w")
        tfile.write("export NBASE_ARC_HOME=%s" % arc_path + os.linesep)
        tfile.write("export PATH=$NBASE_ARC_HOME/bin:$PATH")
        tfile.close()
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
        return None

    return filename


