# Copyright 2017 Naver Corp.
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
import unittest
import testenv
import redis

class TestTest(unittest.TestCase):
    def test_config(self):
        conf = testenv.Config("test")
        try:
            conf.setup()
            cli = redis.RedisClient("localhost", 6000)
            assert cli.do_inline_request('ping') == 'PONG'
        finally:
            conf.teardown()

if __name__ == '__main__':
    unittest.main()
