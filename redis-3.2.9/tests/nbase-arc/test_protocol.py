import unittest
import time
import redis
import sys
from functools import wraps
from multiprocessing import Process

access_port = 6379

# below timeout codes are from stackoverflow (questions/14366761)
class TimeoutError(Exception):
        pass
def timeout(seconds=5, error_message="Timeout"):
    def decorator(func):
        def wrapper(*args, **kwargs):
            process = Process(None, func, None, args, kwargs)
            process.start()
            process.join(seconds)
            if process.is_alive():
                process.terminate()
                raise TimeoutError(error_message)
        return wraps(func)(wrapper)
    return decorator

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

    def test_handle_an_empty_query(self):
        self.send('\r\n')
        self.assert_equal('PONG', self.r('ping'))
    
    def test_negative_multibulk_length(self):
        self.send("*-10\r\n")
        self.assert_equal('PONG', self.r('ping'))

    def test_out_of_range_multibulk_length(self):
        self.send("*20000000\r\n")
        self.assert_subs(".*invalid multibulk length.*" , self.recv())

    def test_wrong_multibulk_payload_header(self):
        self.send("*3\r\n$3\r\nSET\r\n$1\r\nx\r\nfooz\r\n")
        self.assert_subs(".*expected '\$', got 'f'.*", self.recv())

    def test_negative_multibulk_payload_length(self):
        self.send("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$-10\r\n")
        self.assert_subs(".*invalid bulk length.*", self.recv())

    def test_out_of_range_multibulk_payload_length(self):
        self.send("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$2000000000\r\n")
        self.assert_subs(".*invalid bulk length.*", self.recv())

    def test_nonnumber_multibulk_payload_length(self):
        self.send("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$blabla\r\n")
        self.assert_subs(".*invalid bulk length.*", self.recv())

    def test_multibulk_request_not_followd_by_bulk_arguments(self):
        self.send("*1\r\nfoo\r\n")
        self.assert_subs(".*expected '\$', got 'f'.*", self.recv())

    def test_generic_wrong_number_of_args(self):
        self.assert_subs(".*wrong.*arguments.*ping.*", self.r('ping', 'x', 'y', 'z'))

    def test_unbalanced_number_of_quotes(self):
        self.send('set """test-key""" test-value\r\n')
        self.send('ping\r\n')
        self.assert_subs(".*unbalanced.*", self.recv())

    @timeout(1)
    def test_protocol_desync_regiression(self):
        for seq in ['\x00', '*\x00', '$\x00']:
            self.send(seq)
            while True:
                try:
                    payload = (1024*'A') + '"\n"'
                    self.send(payload)
                except:
                    break
            r = self.recv()
            self.assert_subs(".*Protocol error.*", r)
            self.tearDown()
            self.setUp()

if __name__ == '__main__':
    if len(sys.argv) == 2:
        access_port = int(sys.argv.pop())
    elif len(sys.argv) > 2:
        raise 'only <port> argument is allowed'
    unittest.main()
