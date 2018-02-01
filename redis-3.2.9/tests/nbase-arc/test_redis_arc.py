import unittest
import time
import redis
import sys
import random

access_port = 6379

def _mid_null(s):
    return s + '\x00' + s

class TestRedisArc (unittest.TestCase):
    def setUp(self):
        self.conn = redis.RedisClient('localhost', access_port)
        self.r = self.conn.do_generic_request
        self.assert_equal = redis.rr_assert_equal
        self.send = self.conn.raw_write
        self.recv = self.conn.read_response

    def tearDown(self):
        self.conn.close()


    def test_pipelined_bping(self):
        # make bping flags
        isbping = []
        for i in range (0, 1000):
            isbping.append(random.random() >= 0.5)

        # send pipelined request
        for i in range (0, 1000):
            if isbping[i]:
                self.send('BPING\r\n')
            else:
                self.send('SET key%d %d\r\n')

        # receive response
        for i in range (0, 1000):
            if isbping[i]:
                self.assert_equal('PONG', self.recv())
            else:
                self.assert_equal('OK', self.recv())

if __name__ == '__main__':
    if len(sys.argv) == 2:
        access_port = int(sys.argv.pop())
    elif len(sys.argv) > 2:
        raise 'only <port> argument is allowed'
    unittest.main()
