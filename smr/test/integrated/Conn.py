import socket
import io
import sys, traceback

class Conn(object):
  def __init__(self, host, port, end_marker = '\r\n'):
    self.host = host
    self.port = int(port)
    self.end_marker = end_marker
    self._sock = None
    self._buffer = None
    self.bytes_read = 0
    self.bytes_written = 0

  def _purge_buffer(self):
      self._buffer.seek(0)
      self._buffer.truncate()
      self.bytes_written = 0
      self.bytes_read = 0

  def _lazy_connect(self, timeout = 1000):
    if self._sock is not None:
      return
    sock = None
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.settimeout(timeout)
    sock.connect((self.host, self.port))
    self._sock = sock
    self._buffer = io.BytesIO()
    self.bytes_read = 0
    self.bytes_written = 0


  def _read_from_sock(self, length=None):
    buf = self._buffer
    buf.seek(self.bytes_written)
    tr = 0
    while True:
      data = self._sock.recv(1024)
      if isinstance(data, bytes) and len(data) == 0:
        raise Exception('Connection closed')
      buf.write(data)
      self.bytes_written += len(data)
      tr += len(data)

      if length is not None and length > tr:
        continue
      break

  def _already_read(self):
    return self.bytes_written - self.bytes_read

  def _read_line(self):
    buf = self._buffer
    buf.seek(self.bytes_read)
    data = buf.readline()

    while not data.endswith(self.end_marker):
      self._read_from_sock()
      buf.seek(self.bytes_read)
      data = buf.readline()

    self.bytes_read += len(data)

    if self.bytes_read == self.bytes_written:
      self._purge_buffer()
    return data[:-1*len(self.end_marker)]

  def _read_bulk(self, length):
    length = length +len (self.end_marker) 
    if length > self._already_read():
      self._read_from_sock(length - self._already_read())

    self._buffer.seek(self.bytes_read)
    data = self._buffer.read(length)
    self.bytes_read += len(data)
    if self.bytes_read == self.bytes_written:
      self._purge_buffer()
    return data[:-2]

  def _send_request(self, command):
    self._lazy_connect()
    self._sock.sendall(command + self.end_marker)
    return True

  def do_request(self, command):
    self._send_request(command)
    line = self._read_line()

    if line[0] == '$':
      length = int(line[1:])
      data = self._read_bulk(length)
    else:
      data = line
    return data.split(self.end_marker)

  def disconnect(self):
    if self._sock is None:
      return
    try:
      self._sock.close()
    except:
      pass
    self._sock = None
    self._purge_buffer()


if __name__ == '__main__':
  conn = Conn('localhost', 6379)
  resp = conn.do_request("info all")
  print resp
