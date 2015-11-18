from fabric.api import *
from fabric.colors import *
from fabric.contrib.console import *
import sys
import traceback
import telnetlib
import pdb

class GwCmd():
    ip = None
    port = None
    conn = None
    version = None

    def __init__(self, ip, port, version):
        self.ip = ip
        self.port = port
        self.version = version
	#self.version = 1.1

    def __enter__(self):
        self._init_version()
        self.conn = None
        self.conn = telnetlib.Telnet(self.ip, self.port)
        return self

    def __exit__(self, type, value, traceback):
        if self.conn != None:
            self.conn.close()

    def _init_version(self):
        self.REQ_GW_REDIS_DISCONNS = ''
        self.RES_GW_REDIS_DISCONNS = ''
        self.REQ_GW_NUM_OF_CLNTS = 'info gateway'
        self.RES_GW_NUM_OF_CLNTS = 'gateway_connected_clients'
        self.REQ_GW_COMMANDS_PROCESSED = 'info gateway'
        self.RES_GW_COMMANDS_PROCESSED = 'gateway_total_commands_processed'
        if self.version == 1.1:
            self.REQ_GW_REDIS_DISCONNS = 'info cluster'
            self.RES_GW_REDIS_DISCONNS = 'inactive_connections'
        elif self.version == 1.2:
            self.REQ_GW_REDIS_DISCONNS = 'info gateway'
            self.RES_GW_REDIS_DISCONNS = 'gateway_disconnected_redis'

    def command(self, req, res):
        try:
            self.conn.write(req + '\r\n')
            reply = self.conn.read_until('\r\n', 3)
            if reply.find('-ERR No available redis-server for executing info command') != -1:
                warn(red('-ERR No available redis-server for executing info command'))
                return -1
            elif reply == '':
                warn(red('[%s:%d] Gateway reply is Null. version:%f' % (self.ip, self.port, self.version)))
                return -1

            size = int(reply[1:])
            
            readlen = 0
            reply_value = None
            while readlen <= size:
                reply = self.conn.read_until('\r\n', 3)
                readlen += len(reply)
                reply.strip()
                if reply.find(':'):
                    tokens = reply.split(':')
                    if tokens[0].strip() == res:
                        reply_value = int(tokens[1])
            if reply_value != None:
                return reply_value 

            warn(red('[%s:%d] info_redis_disconns() fail. version:%f' % (self.ip, self.port, self.version)))
            return -1
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)

            if self.conn != None:
                self.conn.close()
                self.conn = telnetlib.Telnet(self.ip, self.port)
            return -1

    def is_valid(self):
        if self.info_redis_discoons() == -1:
            return False
        else:
            return True

    def info_redis_discoons(self):
        return self.command(self.REQ_GW_REDIS_DISCONNS, self.RES_GW_REDIS_DISCONNS)

    def info_num_of_clients(self):
        return self.command(self.REQ_GW_NUM_OF_CLNTS, self.RES_GW_NUM_OF_CLNTS)

    def info_total_commands_processed(self):
        return self.command(self.REQ_GW_COMMANDS_PROCESSED, self.RES_GW_COMMANDS_PROCESSED)

