import util
import json
import pprint
import itertools
import constant
from zookeeper import *
from arcci.arcci import *

class ZoneConfigChecker:
    __servers = None
    __arch = None

    def __init__(self, servers, arch=64):
        self.__servers = servers
        self.__arch = arch

    def initial_check(self):
        ZooKeeperCli.start(CLI_RESTART)

        self.__cmi = get_zone_conf_from_cm(self.__servers)
        self.__zki = get_zone_conf_from_zk()
        diffs = compare_zone_conf(self.__servers, self.__cmi, self.__zki)
        if len(diffs) != 0:
            util.log('configurations mismatch error!\n%s' % pprint.pformat(diffs))
            return False
        return True

    def final_check(self):
        ZooKeeperCli.start(CLI_RESTART)

        if util.await(30)(
                lambda x: x,
                lambda : exception_to_value(lambda : check_confmaster(self.__servers), False)) == False:
            util.log('confmaster error!')
            return False

        if self.__check_cluster_state() == False:
            util.log('cluster state error!')
            return False

        if self.__check_redis_operations() == False:
            util.log('redis operation error!')
            return False

        if util.await(120)(lambda x: x, lambda :self.__check_zone_config()) == False:
            return False

        if check_gateway_affinity(self.__cmf) == False:
            util.log('gateway affinity error!')
            return False

        return True

    def __check_zone_config(self):
        self.__cmf = get_zone_conf_from_cm(self.__servers)
        self.__zkf = get_zone_conf_from_zk()
        diffs = []
        diffs.extend(compare_zone_conf(self.__servers, self.__cmi, self.__cmf))
        diffs.extend(compare_zone_conf(self.__servers, self.__zki, self.__zkf))
        if len(diffs) != 0:
            util.log('configurations mismatch error!\n%s' % pprint.pformat(diffs))
            return False
        else:
            util.log('configurations match')
            return True

    def __check_cluster_state(self):
        leader_cm = classify_cm(self.__servers)['leader'][0]
        for cluster in self.__cmi['cluster']:
            ret = util.await(30)(
                lambda x: x,
                lambda : util.check_cluster(cluster['clusterName'].encode('ascii'), leader_cm['ip'], leader_cm['port'], None, True))
            if ret == False:
                return False
        return True

    def __check_redis_operations(self):
        if self.__arch == 32:
            so_path = constant.ARCCI32_SO_PATH
        elif self.__arch == 64:
            so_path = constant.ARCCI_SO_PATH

        for cluster in self.__cmi['cluster']:
            arc_api = ARC_API('127.0.0.1:2181', cluster['clusterName'].encode('ascii'), logFilePrefix = 'bin/log/arcci_log%d' % self.__arch, so_path = so_path)
            try:
                for i in xrange(10):
                    rqst = arc_api.create_request()

                    key = 'config_check_operation_test_%d' % i
                    arc_api.append_command(rqst, "set %s %d" % (key, i))

                    ret = arc_api.do_request(rqst, 3000)

                    be_errno, reply = arc_api.get_reply(rqst)
                    if be_errno != 0:
                        util.log('api.get_reply error!')
                        return False
                    if reply[0] != ARC_REPLY_STATUS:
                        util.log('api.get_reply error! reply: %s' % str(reply[0]))
                        return False

                    arc_api.free_request(rqst)
            finally:
                arc_api.destroy()
        return True

"""
zone_conf = {
    'cm' : {
        'leader' : { 'ip' : <string>, 'port' : <int> },
        'follower' : [ { 'ip' : <string>, 'port' : <int> }, ...  ],
        'heartbeat' : [ { 'ip' : <string>, 'port' : <int> }, ...  ]
    },
    'cluster' : [
        {
            'clusterName' : <string>,
            'clusterData' : <dict>,
            'pgList' : [<dict>, ...],
            'pgsList' : [<dict>, ...],
            'gwList' : [<dict>, ...],
            'gwLookup' : <dict>
        }, ...
    ]
}
"""
def get_zone_conf_from_cm(servers):
    zone_conf = {'cm' : None, 'cluster' : []}

    # Confmaster
    cm_conf = classify_cm(servers)
    cm_conf['heartbeat'] = sorted(map(lambda x: {'ip' : '0.0.0.0', 'port' : x['cm_port']}, servers), key = lambda x: x['port'])
    zone_conf['cm'] = cm_conf

    # Cluster
    leader_cm = cm_conf['leader'][0]
    cluster_conf = []
    for cluster_name in util.cluster_ls(leader_cm['ip'], leader_cm['port'])[1]:
        cluster_conf.append(
                get_cluster_conf_from_cm(
                    leader_cm['ip'], leader_cm['port'], cluster_name.encode('ascii')))

    zone_conf['cluster'] = sorted(cluster_conf, key=lambda x: x['clusterName'])

    return zone_conf

def get_cluster_conf_from_cm(cm_ip, cm_port, cluster_name):
    # Cluster, PG, PGS, GW
    dump = cluster_dump_to_dict(util.cluster_dump(cm_ip, cm_port, cluster_name)[1])

    # GW lookup
    dump['gwLookup'] = [{'gwId' : gw['gwId'], 'ip' : gw['pm_IP'], 'port' : gw['port']} for gw in dump['gwList']]
    return dump

def get_zone_conf_from_zk():
    zone_conf = {'cm' : None, 'cluster' : []}

    # Confmaster
    zone_conf['cm'] = get_confmaster_conf_from_zk()

    # Cluster
    for cluster_name in util.zk_get_children('/RC/CLUSTER')[1]:
        zone_conf['cluster'].append(get_cluster_conf_from_zk(cluster_name))

    return zone_conf

def get_confmaster_conf_from_zk():
    cm_dict = map(lambda x: json.loads(x['data']), 
            sorted(util.zk_get_children_with_data('/RC/CC/LE'), key=lambda x: int(x['name'][2:])))
    cm_dict = map(lambda x: {'ip':x['ip'], 'port':int(x['port'])}, cm_dict)
    return {
        'leader': cm_dict[0:1],
        'follower' : cm_dict[1:],
        'heartbeat' : sorted(map(
            lambda x: {'ip':x['name'].split(':')[0], 'port':int(x['name'].split(':')[1])}, 
            util.zk_get_children_with_data('/RC/CC/FD')), key = lambda x: (x['ip'], x['port'])),
    }

def get_cluster_conf_from_zk(cluster_name):
    # Cluster
    cluster_conf = {
        'clusterName' : cluster_name,
        'clusterData' : json.loads(util.zk_cmd('get /RC/CLUSTER/%s' % cluster_name)['data'])
    }

    # PG
    cluster_conf['pgList'] = __znode_to_dict(
            'pgId', util.zk_get_children_with_data('/RC/CLUSTER/%s/PG' % cluster_conf['clusterName']))

    # PGS
    cluster_conf['pgsList'] = __znode_to_dict(
            'pgsId', util.zk_get_children_with_data('/RC/CLUSTER/%s/PGS' % cluster_conf['clusterName'])) 

    # GW
    cluster_conf['gwList'] = __znode_to_dict(
            'gwId', util.zk_get_children_with_data('/RC/CLUSTER/%s/GW' % cluster_conf['clusterName'])) 

    # GW lookup
    cluster_conf['gwLookup'] = __znode_to_dict(
            'gwId', util.zk_get_children_with_data('/RC/NOTIFICATION/CLUSTER/%s/GW' % cluster_conf['clusterName'])) 

    return cluster_conf 

def __znode_to_dict(id, znode_list):
    return sorted(map(lambda x: dict(json.loads(x['data']), **{id: int(x['name'])}), znode_list), key = lambda x: x[id])

def cluster_dump_to_dict(dump):
    dump['clusterData'] = json.loads(dump['clusterData'])
    dump['pgList'] = map(lambda x: dict(json.loads(x['pgData']), **{'pgId': int(x['pgId'])}), dump['pgList'])
    dump['gwList'] = map(lambda x: dict(json.loads(x['gwData']), **{'gwId': int(x['gwId'])}), dump['gwList'])
    dump['pgsList'] = map(lambda x: dict(json.loads(x['pgsData']), **{'pgsId': int(x['pgsId'])}), dump['pgsList'])
    return dump

def classify_cm(servers):
    cm_leaders = []
    cm_followers = []
    for s in servers:
        ret = json.loads(util.cm_command('0.0.0.0', s['cm_port'], 'cluster_ls'), encoding='ascii')['state']
        if ret == 'success':
            cm_leaders.append({'ip':'0.0.0.0', 'port':s['cm_port']})
        elif ret == 'redirect':
            cm_followers.append({'ip':'0.0.0.0', 'port':s['cm_port']})
    return {'leader':cm_leaders, 'follower':cm_followers}

def compare_dict(x, y, ignore_keys=[]):
    """
    return: list of different values in the form of tuples (key, x[key], y[key])
    """

    if x == None:
        x = {}
    if y == None:
        y = {}

    diffs = []
    kx = set(x).difference(ignore_keys)
    ky = set(y).difference(ignore_keys)

    map(lambda k: diffs.append((k, x[k] if x.has_key(k) else None, y[k] if y.has_key(k) else None)), 
        kx.symmetric_difference(ky))
    if len(diffs) != 0:
        return diffs

    map(lambda k: diffs.append((k, x[k], y[k])) if x[k] != y[k] else None, kx)
    return diffs

def compare_zone_conf(servers, zone_conf1, zone_conf2):
    """
    return: list of differences between zone_conf1 and zone_conf2 in the form of tuple(key, v1, v2)
    """

    diffs = []

    # Confmaster
    for x, y in itertools.izip_longest(zone_conf1['cm']['leader'], zone_conf2['cm']['leader']):
        diffs.extend(map(lambda t: ('confmaster.leader.%s' % t[0], t[1], t[2]), compare_dict(x, y)))
    for x, y in itertools.izip_longest(zone_conf1['cm']['follower'], zone_conf2['cm']['follower']):
        diffs.extend(map(lambda t: ('confmaster.follower.%s' % t[0], t[1], t[2]), compare_dict(x, y)))
    for x, y in itertools.izip_longest(zone_conf1['cm']['heartbeat'], zone_conf2['cm']['heartbeat']):
        diffs.extend(map(lambda t: ('confmaster.heartbeat.%s' % t[0], t[1], t[2]), compare_dict(x, y)))

    # Cluster
    if len(zone_conf1['cluster']) != len(zone_conf2['cluster']):
        return [('cluster.count', len(conf1), len(conf2))]

    conf1 = sorted(zone_conf1['cluster'], lambda x: x['clusterName'])
    conf2 = sorted(zone_conf2['cluster'], lambda x: x['clusterName'])

    for i in xrange(len(conf1)):
        diffs.extend(compare_cluster_conf(conf1[i], conf2[i]))

    return diffs

def check_confmaster(servers):
    """
    return: list of wrong configuration in the form of tuple(key, v1, v2)
    """

    cm = classify_cm(servers)
    x = {
            'leader_count' : len(cm['leader']),
            'follower_count' : len(cm['follower']),
        }
    expected = {
            'leader_count' : 1,
            'follower_count' : len(servers) - 1,
        }

    diffs = compare_dict(x, expected)
    if len(diffs) != 0:
        util.log('confmaster configuration error!\n%s' % pprint.pformat(diffs))
        return False
    return True

def check_gateway_affinity(zone_conf):
    for cluster_conf in zone_conf['cluster']:
        expected = get_gateway_affinity_cluster(cluster_conf)
        affinity = json.loads(util.zk_cmd('get /RC/NOTIFICATION/CLUSTER/%s/AFFINITY' % cluster_conf['clusterName'].encode('ascii'))['data'])
        for x, y in zip(expected, affinity):
            if len(compare_dict(x, y)) != 0:
                util.log('wrong gateway affinity of %s, %s != %s' % (cluster_conf['clusterName'], x, y))
                return False
    return True

def get_gateway_affinity_cluster(cluster_conf):
    alist = []
    for gw in sorted(cluster_conf['gwList'], key = lambda x: str(x['gwId'])):
        alist.append({
            'affinity' : get_gateway_affinity(cluster_conf, gw),
            'gw_id' : gw['gwId']
        })
    return alist

def get_gateway_affinity(cluster_conf, gw):
    ALL = (3, 'A')
    READ = (1, 'R')
    NONE = (0, 'N')

    gw_affinity = [NONE] * 8192

    for pgs in cluster_conf['pgsList']:
        atype = NONE
        if pgs['pm_Name'] == gw['pm_Name'] and pgs['pm_IP'] == gw['pm_IP']:
            if pgs['smr_Role'] == 'M':
                atype = ALL
            elif pgs['smr_Role'] == 'S':
                atype = READ

        for i in xrange(8192):
            if pgs['pg_ID'] == cluster_conf['clusterData']['pn_PG_Map'][i]:
                if atype[0] > gw_affinity[i][0]:
                    gw_affinity[i] = atype

    return util.rle_list_to_string(util.encode_to_rle(map(lambda x: x[1], gw_affinity)))

def compare_cluster_conf(conf1, conf2):
    """
    return: list of differences between conf1 and conf2 in the form of tuple(key, v1, v2)
    """

    diffs = []

    cluster_name = conf1['clusterName']
    diffs.extend(compare_dict(conf1['clusterData'], conf2['clusterData']))

    # Gateway
    for x, y in itertools.izip_longest(conf1['gwList'], conf2['gwList']):
        diffs.extend(map(lambda t: ('%s.gw.%d.%s' % (cluster_name, x['gwId'], t[0]), t[1], t[2]), 
            compare_dict(x, y)))

    # PG
    for x, y in itertools.izip_longest(conf1['pgList'], conf2['pgList']):
        diffs.extend(map(lambda t: ('%s.pg.%d.%s' % (cluster_name, x['pgId'], t[0]), t[1], t[2]), 
            compare_dict(x, y, ['master_Gen_Map', 'pgs_ID_List'])))

    # PGS
    for x, y in itertools.izip_longest(conf1['pgsList'], conf2['pgsList']):
        diffs.extend(map(lambda t: ('%s.pgs.%d.%s' % (cluster_name, x['pgsId'], t[0]), t[1], t[2]), 
            compare_dict(x, y, ['stateTimestamp', 'smr_Role', 'old_SMR_Role', 'master_Gen'])))

    # Gateway lookup
    for x, y in itertools.izip_longest(conf1['gwLookup'], conf2['gwLookup']):
        diffs.extend(map(lambda t: ('%s.gwLookup.%d.%s' % (cluster_name, x['gwId'], t[0]), t[1], t[2]), 
            compare_dict(x, y)))

    return diffs

def compare_cluster_smr_role(conf1, conf2):
    """
    return: list of differences between conf1 and conf2 in the form of tuple(key, v1, v2)
    """

    diffs = []

    for x, y in itertools.izip_longest(conf1['pgsList'], conf2['pgsList']):
        diffs.extend(map(lambda t: ('%s.pgs.%d.%s' % (conf1[i]['clusterName'], x['pgsId'], t[0]), t[1], t[2]), 
            compare_dict(x, y)))

    return diffs

def exception_to_value(func, v):
    """
    example: exception_to_value(lambda : compare_cluster_smr_role(conf1, conf2), [])
    """

    try:
        return func()
    except:
        return v
