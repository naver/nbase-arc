import unittest
import random
import telnetlib
import default_cluster
import util
import config
import redis_mgmt
import gateway_mgmt

class TestProtocolError(unittest.TestCase):
    cluster = config.clusters[2]

    @classmethod
    def setUpClass(cls):
        return 0

    @classmethod
    def tearDownClass(cls):
        return 0

    def setUp(self):
        util.set_process_logfile_prefix( 'TestProtocolError_%s' % self._testMethodName )
        ret = default_cluster.initialize_starting_up_smr_before_redis(self.cluster)
        if ret is not 0:
            util.log('failed to test_basic_op.initialize')
            default_cluster.finalize(self.cluster)

    def tearDown(self):
        if default_cluster.finalize(self.cluster) is not 0:
            util.log('failed to test_basic_op.finalize')

    def random_string(self):
        population = 'abcdefghijklmnopqrstuvwxyz'   \
                     'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        return ''.join(random.sample(population, 10))

    def server_connect(self, server_mgmt, host, port):
        ret = server_mgmt.connect(host, port)
        self.assertEquals(ret, 0, 'failed : connect to server(%s:%d)' % (host, port))

    def server_reconnect(self, server_mgmt):
        host = server_mgmt.t.host
        port = server_mgmt.t.port
        server_mgmt.disconnect()
        self.server_connect(server_mgmt, host, port)

    def send_command_and_expect(self, server, cmd, expect):
        server.write(cmd)
        ret = server.read_until(expect)
        self.assertEquals(ret, expect, "Server didn't respond properly. expect:%s, ret:%s" % (expect, ret))

    def _test_inline_with_double_quote(self, server, optional = None):
        util.print_frame()

        prefix = self.random_string()
        self.send_command_and_expect(server
                , 'set '                                                                                \
                  '"%s_key_in_double_quote with ( \\x5a\\x7A\\n\\r\\t\\b\\a\'\\")" '                    \
                  '"data_in_double_quote with ( \\x5a\\x7A\\n\\r\\t\\b\\a\'\\")"\r\n' % prefix
                , '+OK\r\n')
        self.send_command_and_expect(server
                , 'get "%s_key_in_double_quote with ( \\x5a\\x7A\\n\\r\\t\\b\\a\'\\")"\r\n' % prefix
                , '$38\r\ndata_in_double_quote with ( \x5a\x7A\n\r\t\b\a\'")\r\n')
        if (optional):
            self.send_command_and_expect(optional
                , 'get "%s_key_in_double_quote with ( \\x5a\\x7A\\n\\r\\t\\b\\a\'\\")"\r\n' % prefix
                , '$38\r\ndata_in_double_quote with ( \x5a\x7A\n\r\t\b\a\'")\r\n')

        self.send_command_and_expect(server
                , '"" set "" %s_with_empty_args "" val          \r\n' % prefix
                , '+OK\r\n')
        self.send_command_and_expect(server
                , '    ""    get    ""      %s_with_empty_args     ""  \r\n' % prefix
                , '$3\r\nval\r\n')


    def _test_inline_with_single_quote(self, server, optional = None):
        util.print_frame()

        prefix = self.random_string()
        self.send_command_and_expect(server
                , "set "                                                                                \
                  "'%s_key_in_single_quote with ( \\x5a\\x7A\\n\\r\\t\\b\\a\"\\')' "                    \
                  "'data_in_single_quote with ( \\x5a\\x7A\\n\\r\\t\\b\\a\"\\')'\r\n" % prefix
                , '+OK\r\n')
        self.send_command_and_expect(server
                , "get '%s_key_in_single_quote with ( \\x5a\\x7A\\n\\r\\t\\b\\a\"\\')'\r\n" % prefix
                , "$49\r\ndata_in_single_quote with ( \\x5a\\x7A\\n\\r\\t\\b\\a\"')\r\n")
        if (optional):
            self.send_command_and_expect(optional
                , "get '%s_key_in_single_quote with ( \\x5a\\x7A\\n\\r\\t\\b\\a\"\\')'\r\n" % prefix
                , "$49\r\ndata_in_single_quote with ( \\x5a\\x7A\\n\\r\\t\\b\\a\"')\r\n")

        self.send_command_and_expect(server
                , "'' set '' %s_with_empty_args '' val          \r\n" % prefix
                , "+OK\r\n")
        self.send_command_and_expect(server
                , "    ''    get    ''      %s_with_empty_args     ''  \r\n" % prefix
                , "$3\r\nval\r\n")

    def _test_protocol_error_with_quote(self, server, optional = None):
        util.print_frame()

        self.send_command_and_expect(server
                , 'set "key_without_closing_quote value\r\n'
                , '-ERR Protocol error: unbalanced quotes in request\r\n')
        self.server_reconnect(server)
        if (optional):
            self.send_command_and_expect(optional
                , "set 'key_without_closing_quote value\r\n"
                , '-ERR Protocol error: unbalanced quotes in request\r\n')
            self.server_reconnect(optional)

        self.send_command_and_expect(server
                , "set 'key_without_closing_quote value_without_opening_quote'\r\n"
                , "-ERR wrong number of arguments for 'set' command\r\n")
        if (optional):
            self.send_command_and_expect(optional
                , 'set "key_without_closing_quote value_without_opening_quote"\r\n'
                , "-ERR wrong number of arguments for 'set' command\r\n")

        self.send_command_and_expect(server
                , 'set key value_without_opening_quote"\r\n'
                , '-ERR Protocol error: unbalanced quotes in request\r\n')
        self.server_reconnect(server)
        if (optional):
            self.send_command_and_expect(optional
                , "set key value_without_opening_quote'\r\n"
                , '-ERR Protocol error: unbalanced quotes in request\r\n')
            self.server_reconnect(optional)

    def _test_consistency(self, original, replica):
        util.print_frame()

        prefix = self.random_string()
        self.send_command_and_expect(original
                , "set %s_send_to_original value\r\n" % prefix
                , "+OK\r\n")
        self.send_command_and_expect(replica
                , "get %s_send_to_original\r\n" % prefix
                , "$5\r\nvalue\r\n")
        self.send_command_and_expect(original
                , "get %s_send_to_original\r\n" % prefix
                , "$5\r\nvalue\r\n")

        self.send_command_and_expect(replica
                , "set %s_send_to_replica value\r\n" % prefix
                , "+OK\r\n")
        self.send_command_and_expect(original
                , "get %s_send_to_replica\r\n" % prefix
                , "$5\r\nvalue\r\n")
        self.send_command_and_expect(replica
                , "get %s_send_to_replica\r\n" % prefix
                , "$5\r\nvalue\r\n")

    def _test_inline_protocol_error(self, server):
        util.print_frame()

        REDIS_INLINE_MAX_SIZE = 1024*64
        self.send_command_and_expect(server
                , 'set key %s\r\n' % ('x' * (REDIS_INLINE_MAX_SIZE * 3))
                , '-ERR Protocol error: too big inline request\r\n')
        self.server_reconnect(server)

        self.send_command_and_expect(server
                , 'set key value\nget key\n'
                , '+OK\r\n$5\r\nvalue\r\n')

    def _test_multibulk_protocol_error(self, server):
        util.print_frame()

        REDIS_INLINE_MAX_SIZE = 1024*64
        self.send_command_and_expect(server
                , '*%s\r\n' % ('1' * (REDIS_INLINE_MAX_SIZE * 3))
                , '-ERR Protocol error: too big mbulk count string\r\n')
        self.server_reconnect(server)

        self.send_command_and_expect(server
                , '*%s\r\n' % ('1' * (1024*48))
                , '-ERR Protocol error: invalid multibulk length\r\n')
        self.server_reconnect(server)

        self.send_command_and_expect(server
                , '*%d\r\n' % (1024*1024 + 1)
                , '-ERR Protocol error: invalid multibulk length\r\n')
        self.server_reconnect(server)

        self.send_command_and_expect(server
                , '*%d\r\n!\r\n' % (1024*1024)
                , "-ERR Protocol error: expected '$', got '!'\r\n")
        self.server_reconnect(server)

        self.send_command_and_expect(server
                , '*not_a_number\r\n'
                , '-ERR Protocol error: invalid multibulk length\r\n')
        self.server_reconnect(server)

        self.send_command_and_expect(server
                , '*-1\r\nset normal command\r\n'
                , '+OK\r\n')

        self.send_command_and_expect(server
                , '*2\r\n$3\r\nget\r\n$%s\r\n' % ('1' * (REDIS_INLINE_MAX_SIZE * 3))
                , '-ERR Protocol error: too big bulk count string\r\n')
        self.server_reconnect(server)

        self.send_command_and_expect(server
                , '*2\r\n!3\r\nget\r\n$3\r\nkey\r\n'
                , "-ERR Protocol error: expected '$', got '!'\r\n")
        self.server_reconnect(server)

        self.send_command_and_expect(server
                , '*2\r\n$-1\r\n'
                , '-ERR Protocol error: invalid bulk length\r\n')
        self.server_reconnect(server)

        self.send_command_and_expect(server
                , '*3\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$%d\r\n' % (512*1024*1024+1)
                , '-ERR Protocol error: invalid bulk length\r\n')
        self.server_reconnect(server)

        self.send_command_and_expect(server
                , '*3\r\n$3\r\nSET\r\n$1\r\na\r\n$0\r\n\r\n'
                , '+OK\r\n')

    def test_redis_protocol(self):
        util.print_frame()

        # connect to redis
        redis_config = self.cluster['servers'][0]
        redis = redis_mgmt.Redis(redis_config['id'])
        self.server_connect(redis, redis_config['ip'], redis_config['redis_port'])
        # connect to redis replica
        redis_replica_config = self.cluster['servers'][1]
        redis_replica = redis_mgmt.Redis(redis_replica_config['id'])
        self.server_connect(redis_replica, redis_replica_config['ip'], redis_replica_config['redis_port'])

        # test
        self._test_inline_with_double_quote(redis, redis_replica)
        self._test_inline_with_single_quote(redis, redis_replica)
        self._test_protocol_error_with_quote(redis, redis_replica)
        self._test_consistency(redis, redis_replica)
        self._test_inline_protocol_error(redis)
        self._test_multibulk_protocol_error(redis)
        self._test_consistency(redis, redis_replica)

        # finalize
        redis.disconnect()
        redis_replica.disconnect()

    def test_gateway_protocol(self):
        util.print_frame()

        # connect to gateway1
        gateway1_config = self.cluster['servers'][0]
        gateway1 = gateway_mgmt.Gateway(gateway1_config['id'])
        self.server_connect(gateway1, gateway1_config['ip'], gateway1_config['gateway_port'])
        # connect to gateway2
        gateway2_config = self.cluster['servers'][1]
        gateway2 = gateway_mgmt.Gateway(gateway2_config['id'])
        self.server_connect(gateway2, gateway2_config['ip'], gateway2_config['gateway_port'])

        # test
        self._test_inline_with_double_quote(gateway1, gateway2)
        self._test_inline_with_single_quote(gateway1, gateway2)
        self._test_protocol_error_with_quote(gateway1, gateway2)
        self._test_consistency(gateway1, gateway2)
        self._test_inline_protocol_error(gateway1)
        self._test_multibulk_protocol_error(gateway1)
        self._test_consistency(gateway1, gateway2)

        # finalize
        gateway1.disconnect()
        gateway2.disconnect()
