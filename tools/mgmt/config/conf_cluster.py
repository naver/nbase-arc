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

CLUSTER_CONFIG = [
    {
        "cluster_name" : "input_cluster_name",
        "redis" :
        {
            "hash-max-ziplist-entries" : "1024",
            "memory-hard-limit-percentage" : 80
        },
        "smr" :
        {
            "use_memlog" : True
        }
    },
]

