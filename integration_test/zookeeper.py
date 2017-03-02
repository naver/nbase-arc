import os
import json
import telnetlib
import threading
import subprocess
import util
import constant as c

class ZooKeeper:
    def __init__(self, port):
        self.port = port
        self.working_dir = './bin/zk%d' % self.port

    def start(self):
        # make zk directory
        util.local("mkdir -p %s" % self.working_dir)

        # start zk
        self.p = util.exec_proc_async(
                self.working_dir, 
                "java -jar %s/jar/zookeeper-3.4.6-fatjar.jar server %d data" % (os.getcwd(), self.port), 
                True, 
                out_handle=util.devnull)

        # wait warmup
        return util.await(10)(
                lambda reply: reply == 'imok',
                lambda : self.command('ruok'))

    def stop(self):
        self.p.terminate()

    def cleanup(self):
        util.rmr(self.working_dir)

    def command(self, word):
        t = None
        try:
            t = telnetlib.Telnet('localhost', self.port)
            t.write(word) 
            return t.read_all()
        except:
            return None
        finally:
            if t != None:
                t.close()

CLI_RESTART = 1
class ZooKeeperCli:
    __lock = threading.RLock()
    __cli_proc = None

    @classmethod
    def start(cls, mode=0):
        """
        mode: CLI_RESTART
        """
        with cls.__lock:
            if mode & CLI_RESTART:
                cls.stop()

            if cls.__cli_proc != None:
                return

            args = 'java -jar %s -z localhost:2181' % c.ZK_CLI
            cls.__cli_proc = util.exec_proc_async(c.ZK_CLI_DIR, args.split(), False, subprocess.PIPE, subprocess.PIPE, subprocess.PIPE)

    @classmethod
    def stop(cls):
        with cls.__lock:
            if cls.__cli_proc == None:
                return
            try:
                cls.__cli_proc.stdin.close()
                cls.__cli_proc.wait()
            finally:
                cls.__cli_proc = None

    @classmethod
    def execute(cls, cmd):
        with cls.__lock:
            if cls.__cli_proc == None:
                return None

            cls.__cli_proc.stdin.write(cmd + '\n')
            cls.__cli_proc.stdin.flush()
            ret = json.loads(cls.__cli_proc.stdout.readline())
            ret['data'] = ret['message']
            return ret
