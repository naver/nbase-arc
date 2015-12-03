# TODO
# 1. start rpc with the update_port


from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import signal
import os
import stat
import xmlrpclib
import time
import sys
import getopt


ps = None 
pst = None 
thrd = None
proxyserver_filename = './proxyserver_skel_in_testserver.py' 


class RequestHandler(SimpleXMLRPCRequestHandler):
  rpc_paths = ('/RPC2',)


"""@param rpc XMLRPCServer in xml_rpc_server.py
   @param  thrd Worker in proxyserver_skel.py"""
def close_proxyserver_rpc( rpc, thrd ):
  rpc.shutdown()

  addr = 'http://%s:%d' % (rpc.get_ip(), rpc.get_port())
  print addr
  s = xmlrpclib.ServerProxy( addr )

  try_cnt = 0
  max_try_cnt = 3
  while try_cnt < max_try_cnt:
    s.rpc_null_operation_for_terminating_event_loop()
    time.sleep( 1 )
    if rpc.is_quit():
      break

  if rpc.is_quit() is not True:
    return -1

  rpc.server_close()
  del rpc

  return 0


def rpc_init_proxyserver_skel( ip, port, data ):
  print 'ip : %s, port : %d' % (ip, port)
  global proxyserver_filename 
  f_proxy = open( proxyserver_filename, 'w' )
  f_proxy.write( data )
  f_proxy.close()

  import proxyserver_skel_in_testserver 
  reload( proxyserver_skel_in_testserver )
  global ps
  ps, pst = proxyserver_skel_in_testserver.start( ip, port )
  if ps is None or pst is None:
    print 'failed to start proxyserver'
    return -1

  return 0


def rpc_cleanup_old_proxyserver_skel():
  print 'rpc_cleanup_old_proxyserver_skel;'
  global ps
  global pst
  global proxyserver_filename

  if ps is None:
    return 0

  try:
    os.remove( proxyserver_filename )
  except OSError:
    print ''

  ret = close_proxyserver_rpc( ps, pst )
  return ret

 
def rpc_update( o_binary, file_name ):
  f = open( file_name, 'wb' )
  f.write( o_binary.data )
  f.close()

  return 0


def signal_handler( *args ):
  global ps
  if ps is not None:
    if close_proxyserver_rpc( ps, pst ) is not 0:
      exit( -1 )
  exit( 0 )

USAGE = """usage: python updater_start.py
option:
    no options
quit:
    Ctrl-C
"""

def main(argv):
  try:
    opts, args = getopt.getopt(argv, '', [])
  except getopt.GetoptError:
    print USAGE
    sys.exit(-2)

  signal.signal( signal.SIGINT, signal_handler )

  server = SimpleXMLRPCServer(   ("localhost", 8000)
                               , requestHandler=RequestHandler)
  server.register_introspection_functions()

  server.register_function( rpc_init_proxyserver_skel )
  server.register_function( rpc_cleanup_old_proxyserver_skel )
  server.register_function( rpc_update )

  # Run the server's main loop
  server.serve_forever()


main(sys.argv[1:])

