import traceback
import threading
import telnet
import crc16
import util
import sys
import pdb
import random
from arcci.arcci import *


class LoadGenerator(threading.Thread):
  quit = False

  def __init__( self, id, telnet_ip, telnet_port, timeout=3 ):
    threading.Thread.__init__( self )

    self.timeout = timeout

    self.ip = telnet_ip
    self.port = telnet_port

    self.key = 'load_generator_key_%d' % (id)

    self.server = telnet.Telnet( '0' )
    self.server.connect( self.ip, self.port )

    cmd = 'set %s 0\r\n' % self.key
    self.server.write(cmd)
    self.server.read_until('\r\n', self.timeout)
    self.value = 0

    self.consistency = True

  def quit( self ):
    self.quit = True

  def getKey( self ):
    return self.key

  def isConsistent( self ):
    return self.consistency

  def run( self ):
    i = 0
    pipelined_multikey_cmd = 'pipelined_multikey 0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789'
    pipelined_multikey_cmd += '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789'
    pipelined_multikey_cmd += '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789 '
    pipelined_multikey_cmd += '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789 '
    pipelined_multikey_cmd += '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789 '

    pipelined_multikey_cmd += pipelined_multikey_cmd;
    pipelined_multikey_cmd += pipelined_multikey_cmd;

    pipelined_multikey_cmd = 'mset %s\r\n' % pipelined_multikey_cmd
    pipelined_multikey_cmd += pipelined_multikey_cmd;
    pipelined_multikey_cmd += pipelined_multikey_cmd;
    pipelined_multikey_cmd += pipelined_multikey_cmd;

    while self.quit is not True:
      if i > 50000:
        i = 0
      i = i + 1
      
      try:
        self.server.write( pipelined_multikey_cmd )
        response = self.server.read_until( '\r\n', self.timeout )
        response = self.server.read_until( '\r\n', self.timeout )
        response = self.server.read_until( '\r\n', self.timeout )
        response = self.server.read_until( '\r\n', self.timeout )
        response = self.server.read_until( '\r\n', self.timeout )
        response = self.server.read_until( '\r\n', self.timeout )
        response = self.server.read_until( '\r\n', self.timeout )
        response = self.server.read_until( '\r\n', self.timeout )
      except:
        #util.log( 'Connection closed in LoadGenerator:%s' % pipelined_multikey_cmd )
        self.consistency = False
        return
        
      cmd = 'mset 1%s 1 2%s 2 3%s 3 4%s 4 5%s 5 6%s 6\r\n' % (self.key, self.key, self.key, self.key, self.key, self.key)
      try:
        self.server.write( cmd )
        response = self.server.read_until( '\r\n', self.timeout )
      except:
        #util.log( 'Connection closed in LoadGenerator:%s' % cmd )
        self.consistency = False
        return
      
      cmd = 'mget 1%s 2%s 3%s 4%s 5%s 6%s\r\n' % (self.key, self.key, self.key, self.key, self.key, self.key)
      try:
        self.server.write( cmd )
        for read_loop in range(13):
          response = self.server.read_until( '\r\n', self.timeout )
      except:
        #util.log( 'Connection closed in LoadGenerator:%s' % cmd )
        self.consistency = False
        return

      cmd = 'del 1%s 2%s 3%s 4%s 5%s 6%s\r\n' % (self.key, self.key, self.key, self.key, self.key, self.key)
      try:
        self.server.write( cmd )
        response = self.server.read_until( '\r\n', self.timeout )
      except:
        #util.log( 'Connection closed in LoadGenerator:%s' % cmd )
        self.consistency = False
        return

#      cmd = 'info all\r\ninfo all\r\ninfo all\r\n'
#      try:
#        self.server.write( cmd )
#        for read_loop in range(3):
#          response = self.server.read_until( '\r\n\r\n' )
#          response = self.server.read_until( '\r\n\r\n' )
#          response = self.server.read_until( '\r\n\r\n' )
#          response = self.server.read_until( '\r\n\r\n' )
#          response = self.server.read_until( '\r\n\r\n' )
#          response = self.server.read_until( '\r\n\r\n' )
#          response = self.server.read_until( '\r\n\r\n' )
#          response = self.server.read_until( '\r\n\r\n' )
#      except:
#        util.log( 'Connection closed in LoadGenerator:%s' % cmd )
#        self.consistency = False
#        return
      
      cmd = 'crc16 %s %d\r\n' % (self.key, i)
      try:
        self.server.write( cmd )
        response = self.server.read_until( '\r\n', self.timeout )
      except:
        #util.log( 'Connection closed in LoadGenerator:%s' % cmd )
        self.consistency = False
        return

      
      self.value = crc16.crc16_buff(str(i), self.value)
      try:
        if (int(response[1:-2]) != self.value):
          if self.consistency:
              self.consistency = False
      except ValueError:
        #util.log( 'Value Error in LoadGenerator, ret:%s' % response[:-2] )
        self.consistency = False
        return


def is_reply_ok(reply):
    if reply[0] == ARC_REPLY_ERROR:
        return False
    elif reply[0] == ARC_REPLY_STATUS:
        return True
    elif reply[0] == ARC_REPLY_INTEGER:
        return True
    elif reply[0] == ARC_REPLY_STRING:
        return True
    elif reply[0] == ARC_REPLY_ARRAY:
        for r in reply[1]:
            return is_reply_ok(r)
    elif reply[0] == ARC_REPLY_NIL:
        return True

class LoadGenerator_ARCCI_FaultTolerance(threading.Thread):
    def __init__(self, id, api, verbose=False):
        threading.Thread.__init__(self)
        self.id = id
        self.api = api
        self.timeout = 3000
        self.verbose = verbose

        self.key = 'LoadGenerator_ARCCI_FaulTolerance_%d' % self.id
        self.i = 0
        self.kv = {}
        self.err_cnt = 0
        self.cont = True

    def quit(self):
        self.cont = False 

    def get_err_cnt(self):
        return self.err_cnt

    def process(self):
        rqst = self.api.create_request()
        if rqst == None:
            return False

        key = "%s%d" % (self.key, self.i % 100)
        value = "%d" % self.i
        cmd = 'set %s %s' % (key, value)
        self.api.append_command(rqst, cmd)

        try:
            ret = self.api.do_request(rqst, self.timeout)
            if ret != 0:
                self.err_cnt += 1
                return False

            be_errno, reply = self.api.get_reply(rqst)
            if be_errno < 0 or reply == None:
                self.err_cnt += 1
                return False

            if reply[0] != ARC_REPLY_STATUS:
                print reply
                self.err_cnt += 1
                return False
            elif reply[0] == ARC_REPLY_ERROR:
                print reply
                self.err_cnt += 1
                return False

            self.kv[key] = value

        except:
            if self.verbose:
                util.log('Connection closed in LoadGenerator:%s, except' % cmd)
                util.log(sys.exc_info())
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
            self.consistency = False
            return False

        return True

    def run(self):
        try:
            while self.cont:
                self.process()

            util.log('End LoadGenerator_ARCCI_FaultTolerance')
        except:
            util.log('Exception LoadGenerator_ARCCI_FaultTolerance')

        finally:
            self.api.destroy()

class LoadGenerator_ARCCI(threading.Thread):
    def __init__(self, id, api, timeout_second=3, verbose=False):
        threading.Thread.__init__(self)
        self.api = api
        self.timeout = timeout_second * 1000
        self.verbose = verbose

        self.cont = True
        self.consistency = True
        self.value = 0
        self.i = 0
        self.key = 'load_generator_key_%d' % (id)

        self.pipelined_multikey_cmd = 'pipelined_multikey 0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789'
        self.pipelined_multikey_cmd += '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789'
        self.pipelined_multikey_cmd += '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789 '
        self.pipelined_multikey_cmd += '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789 '
        self.pipelined_multikey_cmd += '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789 '
        self.pipelined_multikey_cmd += self.pipelined_multikey_cmd;
        self.pipelined_multikey_cmd += self.pipelined_multikey_cmd;
        self.pipelined_multikey_cmd = 'mset %s\r\n' % self.pipelined_multikey_cmd
        self.pipelined_multikey_cmd += self.pipelined_multikey_cmd;
        self.pipelined_multikey_cmd += self.pipelined_multikey_cmd;
        self.pipelined_multikey_cmd += self.pipelined_multikey_cmd;

    def quit( self ):
        self.cont = False

    def is_reply_ok(self, reply):
        if reply[0] == ARC_REPLY_ERROR:
            return False
        elif reply[0] == ARC_REPLY_STATUS:
            return True
        elif reply[0] == ARC_REPLY_INTEGER:
            return True
        elif reply[0] == ARC_REPLY_STRING:
            return True
        elif reply[0] == ARC_REPLY_ARRAY:
            for r in reply[1]:
                return is_reply_ok(r)
        elif reply[0] == ARC_REPLY_NIL:
            return True

    def run(self):
        loop_cnt = 0;

        try:
            if self.init_crc() == False:
                return False
                
            while self.cont:
                loop_cnt += 1
                if self.process() == False:
                    return False

            self.api.destroy()

        except:
            util.log('Connection closed in LoadGenerator:%s, except' % cmd)
            util.log(sys.exc_info())
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
            return False

    def init_crc(self):
        # Init CRC
        rqst = self.api.create_request()
        if rqst == None:
            self.consistency = False
            return False

        cmd = 'set %s 0' % self.key
        self.api.append_command(rqst, cmd)

        try:
            ret = self.api.do_request(rqst, self.timeout)
            if ret != 0:
                self.consistency = False
                return False

            be_errno, reply = self.api.get_reply(rqst)
            if be_errno < 0 or reply == None:
                self.consistency = False
                return False

            if reply[0] != ARC_REPLY_STATUS:
                self.consistency = False
                return False
        except:
            util.log('Connection closed in LoadGenerator:%s, except' % cmd)
            util.log(sys.exc_info())
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
            self.consistency = False
            return False

        return True

    def process(self):
        if self.i > 50000:
            self.i = 0
        self.i += 1

        # Pipelined multikey - Request
        rqst = self.api.create_request()
        if rqst == None:
            self.consistency = False
            return False

        self.api.append_command(rqst, self.pipelined_multikey_cmd)

        # Pipelined multikey - Check reply
        try:
            ret = self.api.do_request(rqst, self.timeout)
            if ret != 0:
                self.consistency = False
                return False

            while True:
                be_errno, reply = self.api.get_reply(rqst)
                if be_errno < 0 or reply == None:
                    if be_errno < 0:
                        self.consistency = False
                        return False
                    break

                if is_reply_ok(reply) == False:
                    self.consistency = False
                    return False

        except:
            if self.verbose:
                util.log( 'Connection closed in LoadGenerator:%s' % self.pipelined_multikey_cmd )
            self.consistency = False
            return False

        # Multi - MSET
        rqst = self.api.create_request()
        if rqst == None:
            self.consistency = False
            return False

        cmd = 'mset 1%s 1 2%s 2 3%s 3 4%s 4 5%s 5 6%s 6' % (self.key, self.key, self.key, self.key, self.key, self.key)
        self.api.append_command(rqst, cmd)

        try:
            ret = self.api.do_request(rqst, self.timeout)
            if ret != 0:
                self.consistency = False
                return False

            while True:
                be_errno, reply = self.api.get_reply(rqst)
                if be_errno < 0 or reply == None:
                    if be_errno < 0:
                        self.consistency = False
                        return False
                    break

                if is_reply_ok(reply) == False:
                    self.consistency = False
                    return False

        except:
            if self.verbose:
                util.log( 'Connection closed in LoadGenerator:%s' % cmd )
            self.consistency = False
            return False

        # Multi - MGET
        rqst = self.api.create_request()
        if rqst == None:
            self.consistency = False
            return False

        cmd = 'mget 1%s 2%s 3%s 4%s 5%s 6%s' % (self.key, self.key, self.key, self.key, self.key, self.key)
        self.api.append_command(rqst, cmd)

        try:
            ret = self.api.do_request(rqst, self.timeout)
            if ret != 0:
                self.consistency = False
                return False

            while True:
                be_errno, reply = self.api.get_reply(rqst)
                if be_errno < 0 or reply == None:
                    if be_errno < 0:
                        self.consistency = False
                        return False
                    break

                if is_reply_ok(reply) == False:
                    self.consistency = False
                    return False

        except:
            if self.verbose:
                util.log( 'Connection closed in LoadGenerator:%s' % cmd )
            self.consistency = False
            return False

        # Multi - DEL
        rqst = self.api.create_request()
        if rqst == None:
            self.consistency = False
            return False

        cmd = 'del 1%s 2%s 3%s 4%s 5%s 6%s' % (self.key, self.key, self.key, self.key, self.key, self.key)
        self.api.append_command(rqst, cmd)

        try:
            ret = self.api.do_request(rqst, self.timeout)
            if ret != 0:
                self.consistency = False
                return False

            while True:
                be_errno, reply = self.api.get_reply(rqst)
                if be_errno < 0 or reply == None:
                    if be_errno < 0:
                        self.consistency = False
                        return False
                    break

                if is_reply_ok(reply) == False:
                    self.consistency = False
                    return False

        except:
            if self.verbose:
                util.log( 'Connection closed in LoadGenerator:%s' % cmd )
            self.consistency = False
            return False

        # CRC
        rqst = self.api.create_request()
        if rqst == None:
            self.consistency = False
            return False

        cmd = 'crc16 %s %d' % (self.key, self.i)
        self.api.append_command(rqst, cmd)

        try:
            ret = self.api.do_request(rqst, self.timeout)
            if ret != 0:
                self.consistency = False
                return False

            be_errno, reply = self.api.get_reply(rqst)
            if be_errno < 0 or reply == None:
                if be_errno < 0:
                    self.consistency = False
                    return False

            if reply[0] != ARC_REPLY_INTEGER:
                self.consistency = False
                return False
            
            # CRC - Check consistency
            self.value = crc16.crc16_buff(str(self.i), self.value)
            try:
                if (reply[1] != self.value):
                    if self.verbose:
                        util.log('Value Error in LoadGenerator, cmd:"%s", reply:%s, value:%d' % (cmd, reply[1], self.value))
                    self.consistency = False
                    return False

            except ValueError:
                if self.verbose:
                    util.log( 'Value Error in LoadGenerator, ret:%s' % response[:-2] )
                self.consistency = False
                return False

        except:
            if self.verbose:
                util.log('Connection closed in LoadGenerator:%s, except' % cmd)
                util.log(sys.exc_info())
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
            self.consistency = False
            return False

        return True

class LoadGenerator_ARCCI_Affinity(threading.Thread):
    ## Constructor of LoadGenerator_ARCCI_Affinity
    #  @param arcci arc_t handle
    #  @param pattern request pattern. <singlekey | range-singlekey | range-multikey | pipeline-singlekey | pipeline-multikey>
    #  @param verbose if it is True, logging verbosely
    def __init__(self, arcci, pattern='singlekey', verbose=False):
        threading.Thread.__init__(self)
        self.arcci = arcci
        self.timeout = 3000
        self.pattern = pattern
        self.verbose = verbose

        self.key = 'LoadGenerator_ARCCI_Affinity'
        self.i = 0
        self.err_cnt = 0
        self.cont = True
        self.op_rotate = 'write'       # 'write' | 'read' | 'both'

    def quit(self):
        self.cont = False 

    def get_err_cnt(self):
        return self.err_cnt

    def get_arcci(self):
        return self.arcci

    def process(self):
        if self.op_rotate == 'write':
            self.op_rotate = 'read'
        elif self.op_rotate == 'read':
            self.op_rotate = 'both'
        elif self.op_rotate == 'both':
            self.op_rotate = 'write'

        rqst = self.arcci.create_request()
        if rqst == None:
            return False

        try:
            # Make request and command(s)
            if self.pattern == 'singlekey':
                rand = random.random() * 10000

                if self.op_rotate == 'read':
                    cmd = 'get %s_%d' % (self.key, rand)
                else:
                    cmd = 'set %s_%d %s' % (self.key, rand, rand)

                self.arcci.append_command(rqst, cmd)

            elif self.pattern == 'range-multikey':
                kv = ''
                for i in xrange(10):
                    rand = random.random() * 10000
                    if self.op_rotate == 'read':
                        kv += '%s_%d ' % (self.key, rand)
                    else:
                        kv += '%s_%d %s ' % (self.key, rand, rand)

                if self.op_rotate == 'read':
                    self.arcci.append_command(rqst, 'mget %s' % kv.strip())
                else:
                    self.arcci.append_command(rqst, 'mset %s' % kv.strip())

            elif self.pattern == 'range-singlekey':
                kv = ''
                rand = random.random() * 10000
                for i in xrange(10):
                    if self.op_rotate == 'read':
                        kv += '%s_%d ' % (self.key, rand)
                    else:
                        kv += '%s_%d %s ' % (self.key, rand, rand)

                if self.op_rotate == 'read':
                    self.arcci.append_command(rqst, 'mget %s' % kv.strip())
                else:
                    self.arcci.append_command(rqst, 'mset %s' % kv.strip())

            elif self.pattern == 'pipeline-singlekey':
                rand = random.random() * 10000
                for i in xrange(10):

                    if self.op_rotate == 'read':
                        cmd = 'get %s_%d' % (self.key, rand)
                    elif self.op_rotate == 'write':
                        cmd = 'set %s_%d %s' % (self.key, rand, rand)
                    elif self.op_rotate == 'both':
                        if i % 2:
                            cmd = 'get %s_%d' % (self.key, rand)
                        else:
                            cmd = 'set %s_%d %s' % (self.key, rand, rand)

                    self.arcci.append_command(rqst, cmd)

            elif self.pattern == 'pipeline-multikey':
                for i in xrange(10):
                    rand = random.random() * 10000

                    if self.op_rotate == 'read':
                        cmd = 'get %s_%d' % (self.key, rand)
                    elif self.op_rotate == 'write':
                        cmd = 'set %s_%d %s' % (self.key, rand, rand)
                    elif self.op_rotate == 'both':
                        if i % 2:
                            cmd = 'get %s_%d' % (self.key, rand)
                        else:
                            cmd = 'set %s_%d %s' % (self.key, rand, rand)

                    self.arcci.append_command(rqst, cmd)

            # Send request
            ret = self.arcci.do_request(rqst, self.timeout)
            if ret != 0:
                self.err_cnt += 1
                return False

            # Receive reply
            be_errno, reply = self.arcci.get_reply(rqst)
            if be_errno < 0 or reply == None:
                self.err_cnt += 1
                return False

            # Handle result
            if reply[0] != ARC_REPLY_STATUS:
                self.err_cnt += 1
                return False
            elif reply[0] == ARC_REPLY_ERROR:
                self.err_cnt += 1
                return False

        except:
            if self.verbose:
                util.log('Connection closed in LoadGenerator:%s, except' % cmd)
                util.log(sys.exc_info())
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
            self.consistency = False
            return False

        return True

    def run(self):
        try:
            while self.cont:
                self.process()

            util.log('End LoadGenerator_ARCCI_Affinity')

        except:
            util.log('Exception LoadGenerator_ARCCI_Affinity')
            util.log(sys.exc_info()[0])

        finally:
            self.arcci.destroy()
