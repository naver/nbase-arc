import os
import telnetlib
import sys

# run unittests
unit_tests = [
    './unit/test-filter',
    './unit/test-rbtree',
    './unit/test-stream',
    './unit/test-log',
    './unit/test-dlist',
    './unit/test-slowlog',
    './unit/test-memdev',
    ]

for test in unit_tests:
  ret = os.system(test)
  if ret == 0:
    print 'test %s passed'  % test
  else:
    print 'test %s error %d' % (test, ret)
    sys.exit(-1)

sys.exit(0)
