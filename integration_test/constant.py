
homedir = './bin'
logdir = '%s/log' % (homedir)

# Binary names
SMR = 'smr-replicator'
GW = 'redis-gateway'
REDIS = 'redis-arc'
CLUSTER_UTIL = 'cluster-util'
DUMP_UTIL = 'dump-util'
DUMP_UTIL_PLUGIN = 'dump2json_base32hex.so'
LOG_UTIL = 'smr-logutil'
CAPI_SO_FILE = 'libarcci.so'
CAPI32_SO_FILE = 'libarcci32.so'
CAPI_TEST_SERVER = 'capi-server'
CAPI32_TEST_SERVER = 'capi-server32'
CC = 'confmaster-1.0.0-SNAPSHOT-jar-with-dependencies.jar'
CM_DEFAULT_PORT = 1122
CM_PROPERTY_FILE_NAME = 'cc.properties'
CM_EXEC_SCRIPT = 'confmaster-integrationtest.sh'

# Binary directoryes
SMR_DIR = '%s/smr' % (homedir)
GW_DIR = '%s/gw' % (homedir)
REDIS_DIR = '%s/redis' % (homedir)
REDIS_CHECK_POINT_FILE_NAME = 'dump.rdb'
CLUSTER_UTIL_DIR = '%s/redis' % (homedir)
DUMP_UTIL_DIR = '%s/redis' % (homedir)
LOG_UTIL_DIR = '%s/smr' % (homedir)
CAPI_DIR = '%s/redis' % (homedir)
CC_DIR = '%s/confmaster' % (homedir)
ARCCI_DIR = "../api/arcci/"
ARCCI_SO_PATH = "../api/arcci/.obj64/lib/libarcci.so"
ARCCI32_SO_PATH = "../api/arcci/.obj32/lib/libarcci.so"

ROLE_LCONN = '1'
ROLE_MASTER = '2'
ROLE_SLAVE = '3'

CC_LEADER = 'leader'
CC_FOLLOWER = 'follower'

