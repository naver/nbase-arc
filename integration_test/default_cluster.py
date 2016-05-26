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

import time
import testbase
import util


def initialize_starting_up_smr_before_redis( cluster, verbose=2, conf=None ):
    if conf == None:
        conf = {'smr_log_delete_delay':86400,
                'cm_context':''}
    if conf.has_key('smr_log_delete_delay') == False:
        conf['smr_log_delete_delay'] = 86400
    if conf.has_key('cm_context') == False:
        conf['cm_context'] = ''

    if testbase.cleanup_zookeeper_root() is not 0:
        util.log('failed to cleanup_zookeeper_root')
        return -1

    if testbase.cleanup_processes() is not 0:
        util.log('failed to cleanup_test_environment')
        return -1

    for server in cluster['servers']:
        if testbase.cleanup_pgs_log_and_ckpt( cluster['cluster_name'], server ) is not 0:
            util.log( 'failed to cleanup_pgs_data' )
            return -1

    for server in cluster['servers']:
        if testbase.request_to_start_cm( server['id'], server['cm_port'], conf['cm_context'] ) is not 0:
            util.log('failed to request_to_start_cm')
            return -1

    if testbase.initialize_cluster( cluster ) is not 0:
        util.log('failed to setup_znodes')
        return -1

    for server in cluster['servers']:
        if testbase.request_to_start_smr( server, verbose=verbose, log_delete_delay=conf['smr_log_delete_delay'] ) is not 0:
            return -1

    for server in cluster['servers']:
        if testbase.request_to_start_redis( server, check=False ) is not 0:
            return -1

    for server in cluster['servers']:
        if testbase.wait_until_finished_to_set_up_role( server ) is not 0:
            return -1

    for server in cluster['servers']:
        if testbase.request_to_start_gateway( cluster['cluster_name'], server, cluster['servers'][0] ) is not 0:
            util.log('failed to request_to_start_gateway')
            return -1

    return 0


def initialize_for_test_confmaster( cluster ):
    if testbase.cleanup_zookeeper_root() is not 0:
        util.log('failed to cleanup_zookeeper_root')
        return -1

    if testbase.cleanup_processes() is not 0:
        util.log('failed to cleanup_test_environment')
        return -1

    for server in cluster['servers']:
        if testbase.cleanup_pgs_log_and_ckpt( cluster['cluster_name'], server ) is not 0:
            util.log( 'failed to cleanup_pgs_data' )
            return -1

    for server in cluster['servers']:
        if testbase.request_to_start_cm( server['id'], server['cm_port'] ) is not 0:
            util.log('failed to request_to_start_cm')
            return -1

    return 0


def finalize( cluster ):
    for server in cluster['servers']:
        if testbase.kill_all_processes( server ) is not 0:
            util.log('failed to kill_all_processes')
            return -1
    return 0
