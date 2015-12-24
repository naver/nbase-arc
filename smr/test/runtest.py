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
