import os

CWD = os.getcwd()
BASE_DIR = CWD
SMR_DIR = os.path.abspath("../../replicator")
BE_DIR = os.path.abspath("../cs")
LOG_DIR = os.path.abspath("../../smr")

SMR_BIN_PATH = os.path.join(SMR_DIR, "smr-replicator")
BE_BIN_PATH = os.path.join(BE_DIR, "smr-server")
CLIENT_BIN_PATH = os.path.join(BE_DIR, "smr-client")
LOG_UTIL_BIN_PATH = os.path.join(LOG_DIR, "smr-logutil")

PIN = None
PINTOOL_BASE = None
try:
  PIN = os.environ['PIN']
  PINTOOL_BASE = os.environ['PINTOOL_BASE']
except:
  pass

OVERRIDE_SMR_BIN_PATH = None
VALGRIND_SMR = False
VALGRIND_BE = False
  
SMR_OPT_X = None
USE_MEM_LOG = os.path.exists("/tmp/opt_use_memlog")
##
## some global flags
##

def get_smr_args(pgs):
  args = []
  if VALGRIND_SMR:
    args.append('valgrind')
    args.append('-v')
    args.append('--leak-check=full')
    args.append('--show-reachable=yes')
  if OVERRIDE_SMR_BIN_PATH:
    args.append(OVERRIDE_SMR_BIN_PATH)
  else:
    args.append(SMR_BIN_PATH)
  args.append('-d')
  args.append(pgs.dir)
  args.append('-b')
  args.append(str(pgs.base_port))
  if SMR_OPT_X:
    args.append('-x')
    args.append(SMR_OPT_X)
  return args

def get_be_args(pgs):
  args = []
  args.append(BE_BIN_PATH)
  args.append('-p')
  args.append(str(pgs.base_port))
  args.append('-s')
  args.append(str(pgs.base_port + 9))
  return args

def get_client_args():
  args = []
  args.append(CLIENT_BIN_PATH)
  return args
