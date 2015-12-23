import telnet

class Gateway(telnet.Telnet):
    def __init__( self, name ):
        self.name = name
        self.t = None
