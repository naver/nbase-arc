import unittest
import time
import redis
import sys

access_port = 6379

def _mid_null(s):
    return s + '\x00' + s

class TestProcotol (unittest.TestCase):
    def setUp(self):
        self.conn = redis.RedisClient('localhost', access_port)
        self.r = self.conn.do_generic_request
        self.assert_equal = redis.rr_assert_equal
        self.assert_subs = redis.rr_assert_substring
        self.send = self.conn.raw_write
        self.recv = self.conn.read_response

    def tearDown(self):
        self.conn.close()


    def test_multikey_commands_with_null_character(self):
        k1 = _mid_null('key1')
        v1 = _mid_null('val1')
        k2 = _mid_null('key2')
        v2 = _mid_null('val2')
        self.r('mset', k1, v1, k2, v2)
        self.assert_equal([v1, v2], self.r('mget', k1, k2))
        pass

    def test_commands_pipelining(self):
        rqst = "SET 1 1\r\nSET 2 2\r\nSET 3 3\r\nSET 4 4\r\nGET 1\r\nGET 2\r\nGET 3\r\nGET 4\r\nGET 1\r\n"
        self.send(rqst)
        self.assert_equal('OK', self.recv())
        self.assert_equal('OK', self.recv())
        self.assert_equal('OK', self.recv())
        self.assert_equal('OK', self.recv())
        self.assert_equal(1, self.recv())
        self.assert_equal(2, self.recv())
        self.assert_equal(3, self.recv())
        self.assert_equal(4, self.recv())
        self.assert_equal(1, self.recv())
        pass

    def test_big_pipe_with_order_check(self):
        pipeline_size = 100000
        count = 0
        while count < pipeline_size:
            self.send('SET key 00000%d0000\r\n' % count)
            self.send('GET key\r\n')
            count = count + 1

        count = 0
        while count < pipeline_size:
            self.assert_equal('OK', self.recv())
            self.assert_equal('00000%d0000' % count, self.recv())
            count = count + 1

if __name__ == '__main__':
    if len(sys.argv) == 2:
        access_port = int(sys.argv.pop())
    elif len(sys.argv) > 2:
        raise 'only <port> argument is allowed'
    unittest.main()
