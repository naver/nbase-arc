from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import util
import config


def rpc_null_operation_for_terminating_event_loop():
  return 0


class RequestHandler(SimpleXMLRPCRequestHandler):
  rpc_paths = ('/RPC2',)


class XMLRPCServer:
  request_quit = False
  quit = False

  def __init__( self, ip, port ):
    self.ip = ip
    self.port = port
    self.server = SimpleXMLRPCServer(   (ip, port)
                                      , requestHandler=RequestHandler)

    self.server.register_introspection_functions()
    self.server.register_function( rpc_null_operation_for_terminating_event_loop )

  def register_function( self, func ):
    self.server.register_function( func )

  def shutdown( self ):
    self.request_quit = True

  def is_quit( self ):
    return self.quit

  def server_close( self ):
    self.server.server_close()

  def serve_forever( self ):
    while not self.request_quit :
      self.server.handle_request()
    self.quit = True
    if config.opt_use_memlog:
      util.remove_all_memory_logs()

  def get_ip( self ):
    return self.ip

  def get_port( self ):
    return self.port

