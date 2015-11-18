#! /usr/bin/env python
import logging
import logging.config
import logging.handlers
import threading
import Queue
import subprocess
import datetime
import time
import json
import os
import signal
import traceback
import glob
import sys
import getopt
import platform
import pdb

import paramiko
from apscheduler.scheduler import Scheduler
import apscheduler.events

import config
import cm

#########################
# Gloval variables
USAGE = """usage: 
batch_daemon.py [-m <cluster_name>/<date>/<time>/<output_format>/<net_limit>/<service_url>]
ex: 
batch_daemon.py -m c1/2014:02:02/10:00:00/rdb/20/\"scp {FILE_PATH} user@hostname:~/backup/{FILE_NAME}\"
batch_daemon.py -m c1/2014:02:02/10:00:00/base32hex/20/\"scp {FILE_PATH} user@hostname:~/{MACHINE_NAME}-{CLUSTER_NAME}-{DATE}_{TIME}.{FILE_EXT}\""""

MAIN = 'MAIN >'
BACKUP = 'BACKUP >'
REMOVE = 'REMOVE >'
CONVERT = 'CONVERT >'
WORKER = 'WORKER >'
MERGE = 'MERGE >'
SEND = 'SEND >'
SCP = 'SCP >'
SSH = 'SSH >>>'
QUIT = 'QUIT > '

# Job type
J_QUIT = 'quit'
J_DUMP_TO_JSON = 'dump_to_json'
J_SEND_RDB = 'send_rdb'

# Output Format
O_BASE32HEX = 'base32hex'
O_TEXT = 'text'
O_RDB = 'rdb'

LOG_EXCEPTION = -1

DEFAULT_FILE_FORMAT = O_BASE32HEX 
FILE_FORMAT_EXT = 'ext'
FILE_FORMAT_LIB = 'lib'
FILE_FORMAT = {
        O_BASE32HEX : {FILE_FORMAT_LIB : config.LOCAL_BINARY_PATH + '/' + config.DUMP_TO_BASE32HEX_FILENAME, FILE_FORMAT_EXT : 'json'},
        O_TEXT : {FILE_FORMAT_LIB : config.LOCAL_BINARY_PATH + '/' + config.DUMP_TO_TEXT_FILENAME , FILE_FORMAT_EXT : 'text'},
        O_RDB : {FILE_FORMAT_LIB : 'None', FILE_FORMAT_EXT : 'rdb'},
}

g_logger = None
g_update_jobs_cnt = 0
g_sched = None
g_workQueue = Queue.Queue() # job format {'type':string, args:{...}}

#########################
# Util
class OutParameter(str):
    #preperty
    def stdout(self):
        return str(self)

def setup_logger(logger_name, log_file_path, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s %(levelname)6s] %(message)s')
    fileHandler = logging.handlers.RotatingFileHandler(log_file_path, maxBytes=10485760, backupCount=5, mode='a')
    fileHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)
    return l

def log(level, msg, job_id=None):
    _log_msg(g_logger, level, msg)

    if job_id != None:
        logger = logging.getLogger(job_id)
        _log_msg(logger, level, msg)

def _log_msg(logger, level, msg):
    if level == logging.DEBUG:
        logger.debug(msg)
    elif level == logging.INFO:
        logger.info(msg)
    elif level == logging.ERROR:
        logger.error(msg)
    elif level == LOG_EXCEPTION:
        #exc_type, exc_value, exc_traceback = sys.exc_info()
        #traceback.print_exception(exc_type, exc_value, exc_traceback, limit=3, file=sys.stdout)
        logger.exception(msg)

def log_debug(msg, job_id=None):
    log(logging.DEBUG, msg, job_id)

def log_info(msg, job_id=None):
    log(logging.INFO, msg, job_id)

def log_error(msg, job_id=None):
    log(logging.ERROR, msg, job_id)

def log_exception(msg, job_id=None):
    log(LOG_EXCEPTION, msg, job_id)

def print_scheduled_jobs():
    global g_sched

    jobs = g_sched.get_jobs()
    job_strs = ['Scheduled jobs:']
    for job in jobs:
        job_strs.append('\t%s' % job)
    log_info(os.linesep.join(job_strs))

def find_in_list_of_dicts(d, key_name, key):
    assert d is not None
    assert key_name is not None
    assert key is not None

    return [item for item in d if item[key_name] == key]

def find_in_map_of_dicts(d, key_name, key):
    assert d is not None
    assert key_name is not None
    assert key is not None

    return [v for k, v in d.items() if v[key_name] == key]

def json_to_str(json_data):
    return json.dumps(json_data, sort_keys=True, indent=4, separators=(',', ' : '), default=handle_not_json_format_object)

def handle_not_json_format_object(o):
    return None

def check_appdata(job):
    pass

def ssh_command(ip, username, cmd, exit_status=[0], job_id=None):
    client = None
    try:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.connect(ip, username=username)

        stdin, stdout, stderr = client.exec_command(cmd)
        for line in stdout:
            log_info(SSH + ' ' + line.strip('\n'), job_id)

        if stdout.channel.recv_exit_status() in exit_status:
            return True 
        else:
            return False
    except IOError as e:
        log_exception('%s IOError: %s' % (SSH, e), job_id)
        return False
    except paramiko.SSHException as e:
        log_exception('%s SSHException: %s' % (SSH, e), job_id)
        return False
    except NameError as e:
        log_exception('%s NameError: %s' % (SSH, e), job_id)
        return False
    except AttributeError as e:
        log_exception('%s AttributeError: %s' % (SSH, e), job_id)
        return False
    except TypeError as e:
        log_exception('%s TypeError: %s' % (SSH, e), job_id)
        return False
    except:
        log_exception('%s Unexpected exception occurs.' % SSH, job_id)
        return False
    finally:
        if client != None:
            client.close()

def scp_put(ip, username, src, dest, net_limit, job_id=None):
    net_limit = net_limit * 1024 * 8    # Convert MByte to kbit
    command = 'scp -l %d %s %s@%s:%s' % (net_limit, src, username, ip, dest)

    log_info('%s command:"%s"' % (SCP, command), job_id)
    out = scp(command)
    if out.succeeded == False:
        log_error('%s execute FAIL. COMMAND:"%s", RETURN_CODE:%d' % (SCP, command, out.returncode))
    else:
        log_info('%s execute SUCCESS. RETURN_CODE:%d' % (SCP, out.returncode))
    return out

def scp_get(ip, username, src, dest, net_limit, job_id=None):
    net_limit = net_limit * 1024 * 8    # Convert MByte to kbit
    command = 'scp -l %d %s@%s:%s %s' % (net_limit, username, ip, src, dest)

    log_info('%s command:"%s"' % (SCP, command), job_id)
    return scp(command)
    if out.succeeded != 0:
        log_error('%s execute FAIL. COMMAND:"%s", RETURN_CODE:%d' % (SCP, command, out.returncode), job_id)
    else:
        log_info('%s execute SUCCESS. RETURN_CODE:%d' % (SCP, p.returncode), job_id)
    return out

def scp(command):
    out_stream = subprocess.PIPE
    err_stream = subprocess.PIPE

    p = subprocess.Popen(command, shell=True, stdout=out_stream, stderr=err_stream)
    (stdout, stderr) = p.communicate()

    out = OutParameter(stdout.strip() if stdout else "")
    err = OutParameter(stderr.strip() if stderr else "")
    out.returncode = p.returncode
    out.stderr = err
    if p.returncode != 0:
        out.succeeded = False
    else:
        out.succeeded = True

    return out

#########################
# Misc 
def make_job_id(job, base_epoch_time):
    return datetime_to_str(epoch_to_datetime(base_epoch_time)) + '_' + job['cluster_name'] + '_%d' % job['backup_id']

def get_date_from_job_id(job_id):
    return job_id.split('_')[0]

def get_time_from_job_id(job_id):
    return job_id.split('_')[1]

def get_cluster_name_from_job_id(job_id):
    tokens = job_id.split('_')
    tokens = tokens[2 : len(tokens)-1]
    return '_'.join(tokens)

def get_job_root_path(cluster_name):
    return './job/%s' % (cluster_name)

def make_job_path(cluster_name, job_id):
    return './job/%s/%s' % (cluster_name, job_id)

def remote_pgs_dir(port):
    return '~/nbase-arc/pgs/%d' % port

def remote_smr_dir(port):
    return remote_pgs_dir(port) + '/smr'

def smr_log_dir(port):
    return remote_smr_dir(port) + '/log'

def remote_redis_dir(port):
    return remote_pgs_dir(port) + '/redis'

def backup_file_name(cluster_name, pg_id, base_epoch_time, file_ext):
    calendar = epoch_to_datetime(base_epoch_time)
    return '%s_%s_PG%04d.%s' % (cluster_name, datetime_to_str(calendar), pg_id, file_format_prop(file_ext, FILE_FORMAT_EXT))

def file_format_prop(file_ext, property):
    if file_ext in FILE_FORMAT:
        return FILE_FORMAT[file_ext][property]
    else:
        return FILE_FORMAT[DEFAULT_FILE_FORMAT][property]

def local_dir(cluster_name, epoch_time):
    calendar = epoch_to_datetime(epoch_time)
    return './%s/%02d/%02d' % (cluster_name, calendar.year, calendar.month)

def datetime_now():
    return datetime_to_str(datetime.datetime.now())

def datetime_to_str(dt):
    return dt.strftime("%Y%02m%02d_%02H%02M%02S")

def epochtime_now():
    return int(time.time() * 1000)

def epoch_to_datetime(epoch_time):
    return datetime.datetime.fromtimestamp(epoch_time / 1000)

def datetime_to_epoch(date_time):
    return (time.mktime(date_time.timetuple()) + date_time.microsecond * 1e-6) * 1000

def get_service_url(job_id, service_url, file_path):
    FILE_PATH = '{FILE_PATH}'
    FILE_NAME = '{FILE_NAME}'
    FILE_EXT = '{FILE_EXT}'
    CLUSTER_NAME = '{CLUSTER_NAME}'
    MACHINE_NAME = '{MACHINE_NAME}'
    DATE = '{DATE}'
    TIME = '{TIME}'

    # Replace FILE_PATH
    out = service_url.replace(FILE_PATH, file_path)

    file_name = os.path.basename(file_path)
    file_ext = ''
    if file_name.count('.') > 0:
        tokens = file_name.split('.')
        file_ext = tokens[len(tokens) - 1]

    # Replace FILE_PATH 
    if service_url.find(FILE_PATH) != -1:
        out = out.replace(FILE_PATH, file_path)

    # Replace FILE_NAME
    if service_url.find(FILE_NAME) != -1:
        out = out.replace(FILE_NAME, file_name )

    # Replace MACHINE_NAME
    if service_url.find(MACHINE_NAME) != -1:
        machine_name = platform.node()
        out = out.replace(MACHINE_NAME, machine_name)

    # Replace CLUSTER_NAME
    if service_url.find(CLUSTER_NAME) != -1:
        out = out.replace(CLUSTER_NAME, get_cluster_name_from_job_id(job_id))

    # Replace DATE 
    if service_url.find(DATE) != -1:
        out = out.replace(DATE, get_date_from_job_id(job_id))

    # Replace TIME
    if service_url.find(TIME) != -1:
        out = out.replace(TIME, get_time_from_job_id(job_id))

    # Replace FILE_EXT
    if service_url.find(FILE_EXT) != -1:
        out = out.replace(FILE_EXT, file_ext)

    return out

#########################
# Job
def listener_missed_job(event):
    event.job.func(*event.job.args, **event.job.kwargs)

def update_jobs():
    global g_update_jobs_cnt 
    global g_sched
    global g_old_appdata

    log_info('running...')

    out_cluster_ls = cm.cluster_ls()
    if out_cluster_ls.cm == False:
        log_info("CONFIG > mgmt-cc error.")
        return 

    # Get jobs from CM
    appdata_list = []
    for cluster_name in out_cluster_ls.json['data']['list']:
        # Appdata contains job of backup
        out = cm.appdata_get(cluster_name.encode('ascii'))
        if out.cm == False:
            log_info("CONFIG > mgmt-cc error.")
            return

        if out.appdata_list == None:
            log_info("CONFIG > Cluster '%s' doesn't have any appdata" % (cluster_name))
        else:
            for data in out.appdata_list:
                appdata_list.append(data)

    # Update jobs
    changed = False
    for appdata in appdata_list:
        period = appdata['period'].split(' ')
        y = period[5]
        day_of_week = period[4]
        mo = period[3]
        d = period[2]
        h = period[1]
        mi = period[0]

        old_appdata = find_in_list_of_dicts(g_old_appdata, "name", appdata["name"])

        if len(old_appdata) == 0:
            old_appdata = None
        else:
            old_appdata = old_appdata[0]

        try:
            # Add new job
            if old_appdata == None:
                sort_keys=True,
                log_info('CONFIG > add job : %s' % json_to_str(appdata))
                if h == 24:
                    h = 0

                if verifyAppdata(appdata):
                    g_sched.add_cron_job(backup, name=appdata['name'], args=[appdata], 
                            minute=mi, hour=h, day=d, month=mo, day_of_week=day_of_week, year=y)
                    changed = True
                else:
                    log_error('CONFIG > Invalid configuration. %s' % json_to_str(appdata))
            # Update modified job
            elif old_appdata['version'] != appdata['version']:
                for scheduled_job in g_sched.get_jobs():
                    if scheduled_job.name == appdata['name']:
                        if verifyAppdata(appdata):
                            log_info('CONFIG > update job %s' % json_to_str(appdata))
                            g_sched.unschedule_job(scheduled_job)
                            g_sched.add_cron_job(backup, name=appdata['name'], args=[appdata], 
                                    minute=mi, hour=h, day=d, month=mo, day_of_week=day_of_week, year=y)
                            changed = True
                        else:
                            log_error('CONFIG > Invalid configuration. %s' % json_to_str(appdata))
                        break
        except ValueError as e:
            log_error('CONFIG > ValueError: %s' % e)
            continue
        except KeyError as e:
            log_error('CONFIG > KeyError: %s' % e)
            continue
        except:
            log_exception("CONFIG > ")
            continue

    g_old_appdata = appdata_list

    # Handle deleted jobs
    deleted_jobs = g_sched.get_jobs()
    for sched_job in g_sched.get_jobs():
        if sched_job.name == 'update_jobs': deleted_jobs.remove(sched_job)

        job = find_in_list_of_dicts(appdata_list, "name", sched_job.name)
        if len(job) != 0:
            deleted_jobs.remove(sched_job)

    for deleted_job in deleted_jobs:
        changed = True
        log_info('CONFIG > delete job : %s' % deleted_job)
        g_sched.unschedule_job(deleted_job)

    if changed == True:
        print_scheduled_jobs()
    else:
        # Print backup jobs
        g_update_jobs_cnt += 1
        if g_update_jobs_cnt >= 10:
            print_scheduled_jobs()
            g_update_jobs_cnt = 0

def verifyAppdata(appdata):
    if appdata['output_format'] not in FILE_FORMAT.keys():
        return False
    return True

def backup(job, base_datetime=None):
    current = datetime.datetime.now()

    if base_datetime == None:
        base_time = job['base_time'].split(':')
        for i in range(len(base_time)):
            base_time[i] = int(base_time[i])

        base_datetime = datetime.datetime(current.year, current.month, current.day, base_time[0], base_time[1], base_time[2], 0)
    base_epoch_time = datetime_to_epoch(base_datetime)

    cluster_name = job['cluster_name']
    backup_id = job['backup_id']
    daemon_id = job['daemon_id']
    period = job['period']
    holding_days = job['holding_period']
    net_limit = job['net_limit']

    job_id = make_job_id(job, base_epoch_time)
    job_path = make_job_path(cluster_name, job_id)
    if os.system('mkdir -p %s' % job_path) != 0:
        log_error('%s make local backup directory FAIL. BACKUP_DIR:%s' % (BACKUP, job_path), job_id)
        return False
    log_info('%s make local backup directory SUCCESS. BACKUP_DIR:%s' % (BACKUP, job_path), job_id)

    # Set logger for this job
    jobLogger = setup_logger(job_id, job_path + '/' + job_id + '.log')

    try:
        # Start
        log_info('%s begin. JOB:%s' % (BACKUP, json_to_str(job)), job_id)
        pg_list = cm.pg_list(cluster_name)
        if pg_list.cm == False:
            log_error('%s mgmt-cc error. JOB_ID:%d' % (BACKUP, job_id), job_id)
            return False

        if pg_list.json == None:
            log_info("%s Cluster '%s' doesn't have any PG" % (BACKUP, cluster_name), job_id)
            return False

        for pg in pg_list.json:
            pg_id = pg["pg_id"]

            # Get appropriate PGS with which batch_daemon will execute timedump 
            # TODO : error handling
            target = get_worker_pgs(cluster_name, pg_id, job_id)
            if target == None:
                log_error('%s backup FAIL. JOB_ID:%s, PG_ID:%d' % (BACKUP, job_id, pg_id), job_id)
                return False

            # Backup (timedump and get dumpfile) 
            if get_timedump(job_id, job_path, cluster_name, pg_id, target, base_epoch_time, net_limit) == False:
                log_error('%s backup FAIL. JOB_ID:%s, PG_ID:%d' % (BACKUP, job_id, pg_id), job_id)
                return False

        # Remove old backup data 
        if remove_old_backup(cluster_name, backup_id, holding_days, job_id=job_id) == False:
            return False

        # Produce jobs that convert dump format to json format
        j = {'args' : { 'id'              : id, 
                        'pg_list'         : pg_list.json,
                        'job_path'        : job_path, 
                        'cluster_name'    : cluster_name, 
                        'base_epoch_time' : base_epoch_time,
                        'backup_id'       : backup_id,
                        'output_format'   : job['output_format'],
                        'service_url'     : job['service_url'],
                        'logger'          : jobLogger
                      }}
        if job['output_format'] == O_BASE32HEX or job['output_format'] == O_TEXT:
            j['type'] = J_DUMP_TO_JSON
        elif job['output_format'] == O_RDB:
            j['type'] = J_SEND_RDB
        else:
            log_error('Unhandled output_format: %s' % job['output_format'])
            assert(0)

        g_workQueue.put(j)

    except TypeError as e:
        log_exception('%s TypeError: %s' % (BACKUP, e), job_id)
    except NameError as e:
        log_exception('%s NameError: %s' % (BACKUP, e), job_id)
    except IOError as e:
        log_exception('%s IOError: %s' % (BACKUP, e), job_id)
    except ValueError as e:
        log_exception('%s ValueError: %s' % (BACKUP, e), job_id)
    except:
        log_exception('%s Unexpected exception occurs.' % BACKUP, job_id)

    return True

def get_worker_pgs(cluster_name, pg_id, job_id):
    out_pgs_list = cm.get_pgs_list(cluster_name, pg_id) 
    if out_pgs_list.cm == False:
        log_error("%s mgmt-cc error." % BACKUP, job_id)
        return None

    # First, check a slave
    slaves = find_in_map_of_dicts(out_pgs_list.json, "smr_role", "S")
    if len(slaves) > 0:
        return slaves[0]

    # If any slave isn't available, it checks the master
    master = find_in_map_of_dicts(out_pgs_list.json, "smr_role", "M")
    if len(master) > 0:
        return master[0]

    log_error('%s there is no available PGS. JOB_ID:%s, PG_ID:%d' % (BACKUP, job_id, pg_id), job_id)
    return None 

def copy_dumputil(ip, net_limit, job_id=None):
    local_dumputil_path = config.LOCAL_BINARY_PATH + '/' + config.DUMP_UTIL_FILENAME
    local_dump2json_path = config.LOCAL_BINARY_PATH + '/' + config.DUMP_TO_BASE32HEX_FILENAME 

    # Copy dumputil
    cmd = 'test -e %s' % config.REMOTE_BIN_DIR + config.DUMP_UTIL_FILENAME
    if ssh_command(ip, config.USERNAME, cmd, job_id=job_id) == False:
        out = scp_put(ip, config.USERNAME, local_dumputil_path, config.REMOTE_BIN_DIR, net_limit, job_id=job_id)
        if out.succeeded == False:
            log_error('%s copy dump-util FAIL. IP:%s, USERNAME:%s, RET_CODE:%d, OUT:"%s", ERR:"%s"' % (BACKUP, ip, config.USERNAME, out.return_code, out, out.stderr), job_id)
            return False
        log_info('%s copy dump-util SUCCESS.' % (BACKUP), job_id)

    # Copy dump2json
    cmd = 'test -e %s' % config.REMOTE_BIN_DIR + config.DUMP_TO_BASE32HEX_FILENAME 
    if ssh_command(ip, config.USERNAME, cmd, job_id=job_id) == False:
        out = scp_put(ip, config.USERNAME, local_dump2json_path, config.REMOTE_BIN_DIR, net_limit, job_id=job_id)
        if out.succeeded == False:
            log_error('%s copy dump2json FAIL. IP:%s, USERNAME:%s, RET_CODE:%d, OUT:"%s", ERR:"%s"' % (BACKUP, ip, config.USERNAME, out.return_code, out, out.stderr), job_id)
            return False
        log_info('%s copy dump2json SUCCESS.' % (BACKUP), job_id)

def get_timedump(job_id, job_path, cluster_name, pg_id, pgs, base_epoch_time, net_limit):
    log_info('%s BACKUP START, CLUSTER_NAME:%s, PG_ID:%d' % (BACKUP, cluster_name, pg_id), job_id)

    ip = pgs['ip']
    port = pgs['smr_base_port']

    # Copy dumputil
    copy_dumputil(ip, net_limit, job_id=job_id)

    # Make arguments
    smr_base_port = pgs['smr_base_port']

    log_dir = smr_log_dir(smr_base_port)
    remote_dump_dir = remote_redis_dir(smr_base_port)
    remote_output_file_path = remote_redis_dir(smr_base_port) + '/' + backup_file_name(cluster_name, pg_id, base_epoch_time, 'rdb')

    # Timedump
    cmd = "cd $NBASE_ARC_HOME;%s --dump %d %s %s %s" % (config.DUMP_UTIL_FILENAME, base_epoch_time, log_dir, remote_dump_dir, remote_output_file_path)
    log_info('%s %s' % (BACKUP, cmd), job_id)
    ret = ssh_command(ip, config.USERNAME, cmd, job_id=job_id)
    if ret == False:
        log_error('%s timedump FAIL. CLUSTER_NAME:%s, PG_ID:%d' % (BACKUP, cluster_name, pg_id), job_id)
        return False
    log_info('%s timedump SUCCESS. CLUSTER_NAME:%s, PG_ID:%d"' % (BACKUP, cluster_name, pg_id), job_id)

    # Copy remote dump into local
    backup_dir = job_path
    log_info('%s BACKUP_DIR:%s' % (BACKUP, job_path), job_id)

    remote_dump_dir = remote_output_file_path 
    remote_dump_dir = remote_dump_dir[2:]
    out = scp_get(ip, config.USERNAME, remote_dump_dir, job_path, net_limit, job_id)
    if out.succeeded == False:
        log_error('%s copy dump file FAIL. IP:%s, USERNAME:%s, REMOTE_DUMP_DIR:%s, RET_CODE:%d, OUT:"%s", ERR:"%s"' % (BACKUP, ip, config.USERNAME, remote_dump_dir, out.return_code, out, out.stderr), job_id)
        return False
    log_info('%s copy dump file SUCCESS.' % (BACKUP), job_id)

    # Remove output file in remote machine
    cmd = "rm -f %s" % remote_output_file_path 
    ret = ssh_command(ip, config.USERNAME, cmd, job_id=job_id)
    if ret == False:
        log_error('%s remove remote output file FAIL. CLUSTER_NAME:%s, PG_ID:%d' % (BACKUP, cluster_name, pg_id), job_id)
        return False
    log_info('%s remove remote output file SUCCESS. CLUSTER_NAME:%s, PG_ID:%d"' % (BACKUP, cluster_name, pg_id), job_id)

    log_info('%s BACKUP SUCCESS, CLUSTER:%s, PG_ID:%d' % (BACKUP, cluster_name, pg_id), job_id)

    return True

def remove_old_backup(cluster_name, backup_id, holding_days, job_id=None):
    # Make folder list, *_<cluster name>_<backup job id>
    job_root = get_job_root_path(cluster_name)
    search_str = '%s/*_%s_%d' % (job_root, cluster_name, backup_id)
    dirs = []

    # Make time variable, now - holding_days
    now = datetime.datetime.now()
    holding_time = now - datetime.timedelta(days=holding_days)

    # Make a list that contains folder names to be deleted
    for full_path in glob.glob(search_str):
        try:
            if os.path.isdir(full_path) == False:
                continue

            if full_path.count('/') != 3:
                continue

            dir = full_path.split('/')[3]

            year = int(dir[:4])
            month = int(dir[4:6])
            day = int(dir[6:8])

            hour = int(dir[9:11])
            minute = int(dir[11:13])
            second = int(dir[13:15])

            time = datetime.datetime(year, month, day, hour, minute, second)

            diff = time - holding_time 
            if diff < datetime.timedelta(seconds=0):
                log_info('%s remove old backup. CLUSTER_NAME:%s, BACKUP_ID:%d, PATH:%s"' % (REMOVE, cluster_name, backup_id, full_path), job_id)
                if os.system('rm -rf %s' % full_path) != 0:
                    log_error('%s remove old backup FAIL. PATH:%s' % (BACKUP, full_path), job_id)
                    return False
                log_info('%s remove old backup SUCCESS. PATH:%s' % (BACKUP, full_path), job_id)

        except:
            log_error('%s unexpected error. PATH:%s' % (REMOVE, full_path), job_id)
            continue

    return True

def handle_job_queue():
    try:
        job = g_workQueue.get(timeout=60)
    except Queue.Empty as e:
        return True

    if job['type'] == J_QUIT:
        log_info('%s quit convert thread' % WORKER)
        return False

    elif job['type'] == J_DUMP_TO_JSON:
        if convert_dump_to_json(job) == False:
            log_error('%s job FAIL. JOB:"%s"' % (WORKER, json_to_str(job)))

    elif job['type'] == J_SEND_RDB:
        if send_rdb(job) == False:
            log_error('%s job FAIL. JOB:"%s"' % (WORKER, json_to_str(job)))
    
    return True

def send_rdb(job):
    job_dir = job['args']['job_path']
    log_info('%s try to send. DIR:"%s"' % (MERGE, job_dir))
    base_epoch_time = job['args']['base_epoch_time']
    job_id = make_job_id(job['args'], base_epoch_time)
    output_format = job['args']['output_format']

    search_str = '%s/*.%s' % (job_dir, file_format_prop(output_format, FILE_FORMAT_EXT))
    merge_files = []
    for file_path in glob.glob(search_str):
        if os.path.isfile(file_path) == False:
            continue

        if file_path.count('/') != 4:
            continue

        # Send to service url
        if send_to_service_url(job_id, file_path, job['args']['service_url']) == False:
            return False

def convert_dump_to_json(job):
    job_path = job['args']['job_path']
    cluster_name = job['args']['cluster_name']
    base_epoch_time = job['args']['base_epoch_time']
    job_id = make_job_id(job['args'], base_epoch_time)
    output_format = job['args']['output_format']

    for pg in job['args']['pg_list']:
        pg_id = pg['pg_id']

        log_info('%s start CLUSTER_NAME:%s, PG_ID:%d, BASE_EPOCH_TIME:%d' % (CONVERT, cluster_name, pg_id, base_epoch_time), job_id)

        if convert(job, job_path, pg_id) == False:
            log_error('%s convert dump to json FAIL. JOB_PATH:%s' % (CONVERT, job_path), job_id)
            return False
        log_info('%s convert dump to json SUCCESS.' % (CONVERT), job_id)

    # Sort and merge backup data
    merge_output = '%s/%s.%s' % (job_path, job_id, file_format_prop(output_format, FILE_FORMAT_EXT))
    if merge_backup_data(job_path, job_id, output_format, merge_output) == False:
        return False

    # Send to service url
    if send_to_service_url(job_id, merge_output, job['args']['service_url']) == False:
        return False

    return True

def convert(job, job_path, pg_id):
    base_epoch_time = job['args']['base_epoch_time']
    cluster_name = job['args']['cluster_name']

    dump_file_path = job_path + '/' + backup_file_name(cluster_name, pg_id, base_epoch_time, 'rdb')
    output_file_path = job_path + '/' + backup_file_name(cluster_name, pg_id, base_epoch_time, job['args']['output_format'])

    out = dump_to_json(job, dump_file_path, output_file_path)
    if out.succeeded == False:
        return False
    return True

def dump_to_json(job, dump_file_path, output_file_path):
    dump_util_lib = file_format_prop(job['args']['output_format'], FILE_FORMAT_LIB)
    command = '%s --dump-iterator %s %s %s' % (config.DUMP_UTIL_FILENAME, dump_file_path, dump_util_lib, output_file_path)
    
    log_info('%s command:"%s"' % (CONVERT, command))

    out_stream = subprocess.PIPE
    err_stream = subprocess.PIPE

    p = subprocess.Popen(command, shell=True, stdout=out_stream, stderr=err_stream)
    (stdout, stderr) = p.communicate()

    out = OutParameter(stdout.strip() if stdout else "")
    err = OutParameter(stderr.strip() if stderr else "")
    out.return_code = p.returncode
    out.stderr = err
    if p.returncode != 0:
        log_error('%s execute FAIL. COMMAND:"%s", RETURN_CODE:%d' % (CONVERT, command, p.returncode))
        out.succeeded = False
    else:
        log_info('%s execute SUCCESS. RETURN_CODE:%d' % (CONVERT, p.returncode))
        out.succeeded = True

    return out

def merge_backup_data(job_dir, job_id, output_format, merge_output):
    log_info('%s try to merge. DIR:"%s"' % (MERGE, job_dir))

    # Sort 
    search_str = '%s/*.%s' % (job_dir, file_format_prop(output_format, FILE_FORMAT_EXT))
    merge_files = []
    for file_path in glob.glob(search_str):
        if os.path.isfile(file_path) == False:
            continue

        if file_path.count('/') != 4:
            continue

        sorted_file = file_path + '.sorted'

        if os.system('sort -o %s %s' % (sorted_file, file_path)) != 0:
            log_error('%s sort FAIL. PATH:%s, OUTPUT:%s' % (MERGE, file_path, sorted_file), job_id)
            return False
        log_info('%s sort SUCCESS. PATH:%s, OUTPUT:%s' % (MERGE, file_path, sorted_file), job_id)

        if os.system('rm -rf %s' % file_path) != 0:
            log_error('%s remove used json file FAIL. PATH:%s' % (MERGE, file_path), job_id)
            return False
        log_info('%s remove used json file SUCCESS. PATH:%s' % (MERGE, file_path), job_id)

        merge_files.append(sorted_file)

    # Merge
    merge_files = ' '.join(merge_files)
    if os.system('sort -o %s -m %s' % (merge_output, merge_files)) != 0:
        log_error('%s merge FAIL. FILE:%s, OUTPUT:%s' % (MERGE, merge_files, merge_output), job_id)
        return False
    log_info('%s merge SUCCESS. OUTPUT:%s, FILE:%s' % (MERGE, merge_output, merge_files), job_id)

    if os.system('rm -rf %s' % merge_files) != 0:
        log_error('%s remove used merge files FAIL. PATH:%s' % (MERGE, merge_files), job_id)
        return False
    log_info('%s remove used merge files SUCCESS. PATH:%s' % (MERGE, merge_files), job_id)

    return True

def send_to_service_url(job_id, file_path, service_url):
    if service_url == '':
        return
    elif service_url == None:
        return

    cmd = get_service_url(job_id, service_url, file_path)

    log_info("%s command:%s" % (SEND, cmd))
    if os.system(cmd) != 0:
        log_error('%s send to service url FAIL. PATH:%s, SERVICE_URL:%s' % (SEND, file_path, cmd), job_id)
        return False
    log_info('%s send to service url SUCCESS. PATH:%s, SERVICE_URL:%s' % (SEND, file_path, cmd), job_id)
    return True

#########################
# Main
def sigint_handler(*args):
    terminate()

def terminate():
    global g_sched

    log_info('%s terminate after it finishes running jobs.' % QUIT)

    log_info('%s shutdown scheduler begin.' % QUIT)
    if g_sched != None:
        g_sched.shutdown()
    log_info('%s shutdown scheduler end.' % QUIT)

    log_info('%s wait work Q begin.' % QUIT)
    j = {'type':J_QUIT}
    g_workQueue.put(j)
    log_info('%s wait work Q end.' % QUIT)

    logging.shutdown()

def init_logger():
    global g_logger
    logging.config.fileConfig('log.conf')
    g_logger = logging.getLogger('main')

def init_sched():
    global g_sched
    global g_old_appdata 
    g_old_appdata = []

    config = {'apscheduler.threadpool.core_threads':1,'apscheduler.threadpool.max_threads':1}
    g_sched = Scheduler(gconfig=config)
    g_sched.add_listener(listener_missed_job, apscheduler.events.EVENT_JOB_MISSED)

    g_sched.start()

    update_jobs()
    g_sched.add_cron_job(update_jobs, name='update_jobs', 
            minute='*', hour='*', day='*', month='*', day_of_week='*', year='*')

def init_signal():
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)

def init_option():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'm:')
    except getopt.GetoptError:
        print USAGE
        sys.exit(1)

    for o, a in opts:
        if o == '-m':
            manual_backup(a)
            sys.exit(0)
        else:
            print USAGE
            sys.exit(1)

def manual_backup(args):
    args = args.strip()
    if args.count('/') < 5:
        print USAGE
        sys.exit(1)

    init_logger()
    if cm.init() == False:
        log_error('initialize cm connection FAIL.')
        exit(1)
    log_info('initialize cm connection SUCCESS.')

    args = args.split('/')
    clusterName = args[0]
    baseDate = args[1]
    baseTime = args[2]
    outputFormat = args[3]
    netLimit = int(args[4])
    serviceURL = '/'.join(args[5:])
    backupID = 0

    job = {
            "backup_id"      : backupID,
            "cluster_name"   : clusterName,
            "daemon_id"      : 0,
            "name"           : "%s_%d" % (clusterName, backupID),
            "net_limit"      : netLimit,
            "period"         : "* * * * * *",
            "base_time"      : baseTime,
            "holding_period" : 36500,
            "output_format"  : outputFormat, 
            "service_url"    : serviceURL,
            "type"           : "backup",
            "version"        : 1
    }

    ymd = baseDate.split(':')
    hms = baseTime.split(':')
    base_datetime = datetime.datetime(int(ymd[0]), int(ymd[1]), int(ymd[2]), int(hms[0]), int(hms[1]), int(hms[2]), 0)
    backup(job, base_datetime)

    terminate()
    while handle_job_queue(): pass

def main():
    init_option()

    init_logger()
    if cm.init() == False:
        log_error('initialize cm connection FAIL.')
        exit(1)
    log_info('initialize cm connection SUCCESS.')

    init_signal()
    log_info('initialize signal handler SUCCESS.')

    init_sched()
    log_info('initialize scheduler SUCCESS.')

    log_info('start batch daemon.')

    while True:
        try:
            if handle_job_queue() == False: 
                break
        except:
            log_exception('%s unxpected exception.' % MAIN)

main()

