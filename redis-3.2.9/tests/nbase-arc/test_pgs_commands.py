import unittest
import time
import redis
import sys
import commands_generic

access_port = 6379
class TestPgsCommands (unittest.TestCase):
    def setUp(self):
        self.conn = redis.RedisClient('localhost', access_port)

    def tearDown(self):
        self.conn.close()

    def test_commands(self):
        commands_generic.do_commands(self.conn, False)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        access_port = int(sys.argv.pop())
    elif len(sys.argv) > 2:
        raise 'only <port> argument is allowed'
    unittest.main()
