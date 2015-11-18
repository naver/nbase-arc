import os

try:
    # CM
    CONF_MASTER_IP = "127.0.0.1"
    CONF_MASTER_PORT = 1122

    # Dump util
    DUMP_UTIL_VERSION = '1.2.1'
    DUMP_UTIL_FILENAME = 'dump-util-%s' % DUMP_UTIL_VERSION
    DUMP_TO_BASE32HEX_FILENAME = 'dump2base32hex-%s.so' % DUMP_UTIL_VERSION
    DUMP_TO_TEXT_FILENAME = 'dump2text-%s.so' % DUMP_UTIL_VERSION

    # Remote binary path
    REMOTE_NBASE_ARC = '~/nbase-arc'
    REMOTE_BIN_DIR = REMOTE_NBASE_ARC + '/bin'

    # Local binary path
    NBASE_ARC_HOME = os.environ.get('NBASE_ARC_HOME')
    LOCAL_BINARY_PATH = NBASE_ARC_HOME + '/bin'

    # Remote machine information
    USERNAME = 'username'
except:
    pass

