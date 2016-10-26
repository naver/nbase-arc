import os
import util
import pdb
import telnetlib

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

