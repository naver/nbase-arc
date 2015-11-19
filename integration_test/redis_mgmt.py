import telnet


class Redis(telnet.Telnet):
  def __init__( self, name ):
    self.name = name
    self.t = None



