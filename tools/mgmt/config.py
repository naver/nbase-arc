import os

try:
    # CM
    CONF_MASTER_IP = "127.0.0.1"
    CONF_MASTER_PORT = 1122

    # Version
    OLD_VERSION = 1.2
    NEW_VERSION = 1.2
    NBASE_ARC_VERSION = '1.2.1'
    NBASE_GW_VERSION = '1.2.1'
    SMR_VERSION = '1.2.1'

    # Local binary path
    NBASE_ARC_HOME = os.environ.get('NBASE_ARC_HOME')
    LOCAL_BINARY_PATH = NBASE_ARC_HOME + '/bin'
    REDIS_CONFIG_FILE = LOCAL_BINARY_PATH + '/conf/port.cluster_name.conf.%.1f' % NEW_VERSION
    ARC_BASH_PROFILE = 'bash.nbase-arc'

    # Remote machine information
    USERNAME = 'username'
    REMOTE_NBASE_ARC = '~/nbase-arc' 
    REMOTE_BIN_DIR = REMOTE_NBASE_ARC + '/bin'
    REMOTE_PGS_DIR = REMOTE_NBASE_ARC + '/pgs'
    REMOTE_GW_DIR = REMOTE_NBASE_ARC + '/gw'

    # Deploy
    GW_BASE_PORT = 6000
    PGS_BASE_PORT = 7000
    BGSAVE_BASE = 7000              # to calculate bgsave time
    ID_GAP = 100
    CRONSAVE_BASE_HOUR = 3
    CRONSAVE_BASE_MIN = 0
    GW_ADDITIONAL_OPTION = [
        {
            "version" : 1.2,
            "opt" : "-d"
        }
    ]

    # CM information
    NUM_CM = 3                      # the number of cm

    # Gateway information
    NUM_WORKERS_PER_GATEWAY = 16    # gateway worker thread count
    NUM_CLNT_MIN = 5                # the minimum number of gateway's clients
    CLIENT_TIMEOUT = 3              # java client timeout (seconds)

    # Migration
    MIN_TIME_TO_ATTEMPT_MIG2PC = 0.05   # Minimum time to try mig2pc

    # Shell
    SHELL = '/bin/bash --rcfile ~/.%s -i -c' % ARC_BASH_PROFILE

except:
    pass
