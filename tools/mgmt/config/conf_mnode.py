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

import os

# CM
CONF_MASTER_IP = None
CONF_MASTER_PORT = None
CONF_MASTER_MGMT_CONS = 3

# Local binary path
LOCAL_BINARY_PATH = '~/nbase-arc/bin'

# Migration
MIN_TIME_TO_ATTEMPT_MIG2PC = 0.05   # Minimum time to try mig2pc in second

# Remote
USERNAME = None

REMOTE_NBASE_ARC = '~/nbase-arc' 
REMOTE_BIN_DIR = REMOTE_NBASE_ARC + '/bin'
REMOTE_PGS_DIR = REMOTE_NBASE_ARC + '/pgs'
REMOTE_GW_DIR = REMOTE_NBASE_ARC + '/gw'

# Shell
ARC_BASH_PROFILE = 'bash.nbase-arc'
SHELL = '/bin/bash --rcfile ~/.%s -i -c' % ARC_BASH_PROFILE

