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
import tempfile
import shutil
import Pg, Pgs, Conf, Log

class CM:
    def __init__(self, name):
        self.name = name
        self.dir = None

    def create_workspace(self):
        base_dir = Conf.BASE_DIR
        if not os.path.exists (base_dir):
            raise Exception('Base directory %s does not exist' % base_dir)
        self.dir = tempfile.mkdtemp(prefix=self.name + "-", dir=base_dir)

    def remove_workspace(self):
        if self.dir:
            Log.atExit()
            shutil.rmtree(self.dir)
        self.dir = None
