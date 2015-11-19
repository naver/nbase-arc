from ctypes import *
import time
import glob
import ast
import os
import util

# struct arc_conf_s
class arc_conf_s(Structure):
    _fields_ = [("num_conn_per_gw", c_int),
                ("init_timeout_millis", c_int),
                ("log_level", c_int),
                ("log_file_prefix", c_char_p),
                ("max_fd", c_int),
                ("conn_reconnect_millis", c_int),
                ("zk_reconnect_millis", c_int),
                ("zk_session_timeout_millis", c_int)]

# typedef enum arc_log_level_t
ARC_LOG_LEVEL_NOLOG = 0
ARC_LOG_LEVEL_ERROR = 1
ARC_LOG_LEVEL_WARN = 2
ARC_LOG_LEVEL_INFO = 3
ARC_LOG_LEVEL_DEBUG = 4

# typedef enum reply type
ARC_REPLY_ERROR = 1
ARC_REPLY_STATUS = 2
ARC_REPLY_INTEGER = 3
ARC_REPLY_STRING = 4
ARC_REPLY_ARRAY = 5
ARC_REPLY_NIL = 6

# typedef enum error
ARC_OK = 0,
ARC_ERR_NOMEM = -1          # memory allocation failed 
ARC_ERR_BAD_ADDRFMT = -2    # bad address format
ARC_ERR_TOO_DEEP_RESP = -3  # repsonses have too deep depth
ARC_ERR_TIMEOUT = -4        # request timeout
ARC_ERR_FE_INTERNAL = -5    # API front end internal error
ARC_ERR_BE_DISCONNECTED = -6    # backend disconnected
ARC_ERR_BAD_CONF = -7       # bad configuration
ARC_ERR_SYSCALL = -8        # system call failed
ARC_ERR_ARGUMENT = -9       # bad argument 
ARC_ERR_BAD_RQST_STATE = -10    # bad request state
ARC_ERR_CONN_CLOSED = -11   # connection closed
ARC_ERR_GW_PROTOCOL = -12   # bad protocol (may be not a gateway)
ARC_ERR_TOO_BIG_DATA = -13  # too big data (more than 1G)
ARC_ERR_BE_INTERNAL = -14   # backend internal error
ARC_ERR_BAD_BE_JOB = -15    # backend pipe error
ARC_ERR_ZOOKEEPER = -16     # zookeeper api call failed
ARC_ERR_AE_ADDFD = -17      # too big (many) file descriptors
ARC_ERR_BAD_ZKDATA = -18    # bad zookeeper znode data
ARC_ERR_GENERIC = -98       # unidentified error (should not be seen)
ARC_ERR_BACKEND = -100      # err is from backend
ARC_ERR_PARTIAL = -101      # err is from bakend and partial result exists 

# struct arc_reply
class arc_reply(Structure):pass
class arc_reply_error(Structure):
    _fields_ = [("len", c_int),
                ("str", c_void_p)]
class arc_reply_status(Structure):
    _fields_ = [("len", c_int),
                ("str", c_void_p)]
class arc_reply_integer(Structure):
    _fields_ = [("val", c_longlong)]
class arc_reply_string(Structure):
    _fields_ = [("len", c_int),
                ("str", c_void_p)]
class arc_reply_array(Structure):
    _fields_ = [("len", c_int),
                ("elem", c_void_p)]
class arc_reply_union(Union):
    _fields_ = [("error", arc_reply_error),
                ("status", arc_reply_status),
                ("integer", arc_reply_integer),
                ("string", arc_reply_string),
                ("array", arc_reply_array)]
arc_reply._fields_ = [("type", c_int),
                      ("d", arc_reply_union)]

class ARC_API():
    def __init__(self,
                 zkAddr,
                 clusterName,
                 gwAddrs = None,
                 connectionPerGateway = 4,
                 timeoutMillis = 3000,
                 logLevel = ARC_LOG_LEVEL_DEBUG,
                 logFilePrefix = None,
                 so_path = "libarcci.so"):

        self.arcci = cdll.LoadLibrary(so_path)
        self.conf = arc_conf_s()
        self.arcci.arc_init_conf(byref(self.conf))

        self.conf.num_conn_per_gw = c_int(connectionPerGateway)
        self.conf.init_timeout_millis = c_int(timeoutMillis)
        self.conf.log_level = c_int(logLevel)
        self.conf.log_file_prefix = c_char_p(logFilePrefix)

        self.arcci.arc_new_zk.restype = c_void_p
        self.arcci.arc_new_gw.restype = c_void_p
        self.arcci.arc_create_request.restype = c_void_p

        if gwAddrs == None:
            self.arc = cast(self.arcci.arc_new_zk(zkAddr, clusterName, byref(self.conf)), c_void_p)
        else:
            self.arc = cast(self.arcci.arc_new_gw(gwAddrs, byref(self.conf)), c_void_p)

    def destroy(self):
        self.arcci.arc_destroy(self.arc)

    def create_request(self):
        return cast(self.arcci.arc_create_request(), c_void_p)

    def free_request(self, rqst):
        self.arcci.arc_free_request(rqst)

    def append_command(self, rqst, format, *args):
        return self.arcci.arc_append_command(rqst, format, *args)

    def do_request(self, rqst, timeout_millis):
        be_errno = c_int()
        return self.arcci.arc_do_request(self.arc, rqst, timeout_millis, byref(be_errno))

    def get_reply(self, rqst):
        be_errno = c_int()
        ptr = c_void_p()
        ret = self.arcci.arc_get_reply(rqst, byref(ptr), byref(be_errno))
        if ptr.value is None: return be_errno.value, None
        res = self.decode_reply(ptr)
        self.arcci.arc_free_reply(ptr)
        return be_errno.value, res

    def decode_reply(self, ptr):
        reply = cast(ptr, POINTER(arc_reply))
        content = reply.contents
        if content.type == ARC_REPLY_ERROR:
            return (content.type, string_at(content.d.error.str, content.d.error.len))
        elif content.type == ARC_REPLY_STATUS:
            return (content.type, string_at(content.d.status.str, content.d.status.len))
        elif content.type == ARC_REPLY_INTEGER:
            return (content.type, content.d.integer.val)
        elif content.type == ARC_REPLY_STRING:
            return (content.type, string_at(content.d.string.str, content.d.string.len))
        elif content.type == ARC_REPLY_ARRAY:
            array_ptr = cast(content.d.array.elem,
                       POINTER(POINTER(arc_reply) * content.d.array.len))
            arr = []
            for i in array_ptr.contents:
                a = self.decode_reply(i)
                arr.append(a)

            return (content.type, arr)
        elif content.type == ARC_REPLY_NIL:
            return (content.type, None)

def make_gw_addrs(cluster):
    addrs = ''
    for s in cluster['servers']:
        addrs += '%s:%d,' % (s['ip'], s['gateway_port'])
    addrs = addrs[:-1]

    return addrs

class LogReader():
    def __init__(self, log_file_prefix):
        self.log_file_prefix = log_file_prefix

        log_file_list = sorted(glob.glob(self.log_file_prefix + "*"), key=os.path.getmtime, reverse=True)
        util.log('log_file:%s' % log_file_list[0])

        self.file = open(log_file_list[0], 'r')
        pass

    def readline(self):
        line = self.file.readline()
        if not line:
            return None

        return line.strip()

"""
parameter
    gw_info : a dict of gateway information.
        {ip":"127.0.0.1","port":8200}
    api : an object of ARC_API
"""
def check_gateway_added(gw_info, api, try_cnt=5):
    MSG_GATEWAY_ADD_ZK = 'Got zookeeper node'
    MSG_GATEWAY_ADD_GW_PREFIX_FMT = 'Connection [%s:%d('
    MSG_GATEWAY_ADD_GW_POSTFIX = '->2'

    log_reader = LogReader(api.conf.log_file_prefix)

    i = 0
    while i < try_cnt:
        i += 1

        while True:
            line = log_reader.readline()
            if line == None: 
                break

            if line.find(MSG_GATEWAY_ADD_ZK) != -1:
                gw = line.split('data:')[1]
                gw = ast.literal_eval(gw)

                if gw == gw_info:
                    return True
            else:
                find_str = MSG_GATEWAY_ADD_GW_PREFIX_FMT % (gw_info['ip'], gw_info['port'])

                if line.find(find_str) != -1 and line.find(MSG_GATEWAY_ADD_GW_POSTFIX) != -1:
                    return True

        time.sleep(1)

    return False

"""
parameter
    gw_info : a dict of gateway information.
        {ip":"127.0.0.1","port":8200}
    api : an object of ARC_API
"""
def check_gateway_deleted(gw_info, api, try_cnt=5):
    MSG_GATEWAY_DEL_ZK = 'is set to unused'
    MSG_GATEWAY_DEL_GW_PREFIX_FMT = 'Connection [%s:%d('
    MSG_GATEWAY_DEL_GW_POSTFIX = '->0'

    log_reader = LogReader(api.conf.log_file_prefix)

    i = 0
    while i < try_cnt:
        i += 1

        while True:
            line = log_reader.readline()
            if line == None: 
                break

            if line.find(MSG_GATEWAY_DEL_ZK) != -1:
                # msg ex) Gateway:[127.0.0.1:10000(id:10,vn:2)] is set to unused
                gw_str = line.split('Gateway:[')[1].split('(')[0].split(":")
                gw = {"ip":gw_str[0],"port":int(gw_str[1])}

                if gw == gw_info:
                    return True
            else:
                find_str = MSG_GATEWAY_DEL_GW_PREFIX_FMT % (gw_info['ip'], gw_info['port'])

                if line.find(find_str) != -1 and line.find(MSG_GATEWAY_DEL_GW_POSTFIX) != -1:
                    return True

        time.sleep(1)

    return False

