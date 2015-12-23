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

import telnetlib
import socket
import util
import re
import string


class Telnet:
    def __init__( self, name ):
        self.name = name
        self.t = None

    def connect( self, ip, port, timeout=3, verbose=True ):
        try:
            self.t = telnetlib.Telnet( ip, port, timeout )
        except socket.error:
            if verbose:
                util.log('server(%s:%d) is unavailable.' % (ip, port))
            return -1
        return 0

    def disconnect( self ):
        self.t.close()
        self.t = None

    def write( self, cmd ):
        self.t.write( cmd )

    def read_until( self, expected, timeout=3 ):
        return self.t.read_until( expected, timeout )

    """
    Returns:
        If success, it returns a python dictionary that contains redis information,
        otherwise it returns None.
    """
    def info(self, section):
        self.write('info %s\r\n' % section)

        # Read first line and get length of reply
        first_line = self.read_until('\r\n')
        if len(first_line) == 0:
            return None

        data_len = int(first_line[1:])
        recv_len = 0

        # Initialize dict and regular expressions
        dict = {}
        p_kv = re.compile('(.+):(.+)')
        p_v = re.compile('([^,]+)=([^,]+)')

        while recv_len < data_len:
            # Read each line
            line = self.read_until('\r\n')
            recv_len += len(line)

            if len(line.strip()) == 0:
                continue
            if line[0] == "#":
                continue

            # Tokenize each line
            toks = p_kv.findall(line)[0]

            # Get information key and data
            k = toks[0]
            if len(toks) > 1:
                v = p_v.findall(toks[1].strip())
                if len(v) == 0:
                    v = toks[1].strip()
            else:
                v = None

            # Insert key/value into dictionary
            try:
                dict[k] = string.atoi(v)
            except:
                try:
                    dict[k] = string.atof(v)
                except:
                    dict[k] = v

        return dict
