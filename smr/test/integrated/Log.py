import os
import Util, Conf

Logs = set()

def createlog(path):
  if Conf.USE_MEM_LOG:
    os.system('%s createlog %s' % (Conf.LOG_UTIL_BIN_PATH, path))
    Logs.add(path)

def deletelog(path):
  if path in Logs:
    os.system('%s deletelog %s' % (Conf.LOG_UTIL_BIN_PATH, path))
    Logs.discard(path)

def atExit():
  while len(Logs) > 0:
    path = Logs.pop()
    os.system('%s deletelog %s' % (Conf.LOG_UTIL_BIN_PATH, path))
