import telnet


class SMR(telnet.Telnet):
    def __init__( self, name ):
        self.name = name
