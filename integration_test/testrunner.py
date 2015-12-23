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

import unittest
import sys
import testbase
import config
import util
import getopt
import imp
import sys
import os
import signal
import default_cluster

def print_list_and_input(list):
    while True:
        print
        last_idx = 0
        for name in list:
            print "%2d:\t%s" % (last_idx, name)
            last_idx += 1

        print

        selection = 0
        sys.stdout.write('select: ')
        try:
            selection = int(sys.stdin.readline())
            if 0 <= selection < last_idx:
                return list[selection]
        except ValueError:
            pass

        print "\nInput error : 1~%d are only acceptable\n" % (last_idx - 1)

def get_test_methods_list(testcase, methods):
    splits = testcase.split('.')
    module = splits[0]
    classname = splits[1]

    class_attr = getattr(__import__(module), classname)
    for t in dir(class_attr):
        if callable(getattr(class_attr, t)) and t.startswith('test'):
            methods.append(t)

def has_test_method(module_attr):
    for member in dir(module_attr):
        if member.startswith('test'):
            return True
    return False

def get_test_cases_list(module, cases):
    for t in dir(module):
        if callable(getattr(module, t)):
            try:
                module_attr = getattr(module, t)
                if issubclass(module_attr, unittest.TestCase) and has_test_method(module_attr):
                    cases.append(module.__name__ + '.' + t)
            except TypeError:
                pass


def run_test_modules(module_list):
    alltests = unittest.TestSuite()
    for module in module_list:
        suite = unittest.TestLoader().loadTestsFromModule(module)
        alltests.addTest(suite)

    return unittest.TextTestRunner(verbosity=2).run(alltests)


def run_test(module, testcase, method):
    suite = None
    if testcase == None:
        suite = unittest.TestLoader().loadTestsFromModule(module)
    elif method == None:
        suite = unittest.TestLoader().loadTestsFromTestCase(getattr(module, testcase))
    else:
        suite = unittest.TestSuite()
        suite.addTest(getattr(module, testcase)(method))
    return unittest.TextTestRunner(verbosity=2).run(suite)


def prepare_cases(test_module_list, cases):
    cases.append('Exit')
    for test_module in test_module_list:
        get_test_cases_list(test_module, cases)

    if len(cases) <= 1:
        print "No test cases in module[%s]" % test_module.__name__
        return False

    cases.append('All')
    return True

def reload_all(test_module):
    for t in dir(test_module):
        attr = getattr(test_module, t)
        if type(attr) == 'module':
            print "reload module '%s'" % t
            reload(attr)

    reload(test_module)

def console(test_module_list):
    cases = []
    if not prepare_cases(test_module_list, cases):
        return

    module_name_list = ', '.join([t.__name__ for t in test_module_list])

    while True:
        print
        print '===== module [%s] =====' % module_name_list

        module_testcase = print_list_and_input(cases)

        if module_testcase == 'All':
            run_test_modules(test_module_list)
        elif module_testcase == 'Exit':
            return
        else:
            print '\n===== module.testcase [%s] =====' % module_testcase
            methods = ['Up']
            get_test_methods_list(module_testcase, methods)
            if len(methods) <= 1:
                print "No test methods in testcase[%s]" % module_testcase
                break

            methods.append('All')
            test_module = __import__(module_testcase.split('.')[0])
            testcase = module_testcase.split('.')[1]
            while True:
                method = print_list_and_input(methods)

                if method == 'All':
                    run_test(test_module, testcase, None)
                elif method == 'Up':
                    break
                else:
                    run_test(test_module, testcase, method)

def __import__(name, globals=None, locals=None, fromlist=None):
    try:
        return sys.modules[name]
    except KeyError:
        pass

    fp, pathname, description = imp.find_module(name)

    try:
        return imp.load_module(name, fp, pathname, description)
    finally:
        if fp:
            fp.close()

def signal_handler( *args ):
    exit(-1)

def cleanup_test_env(opt_skip_copy_binaries, opt_32bit_binary_test):
    # Kill processes
    if testbase.cleanup_processes() != 0:
        util.log('failed to cleanup test environment')
        return -1

    # Cleanup pgs directories
    srv_id_dict = {}
    for cluster in config.clusters:
        for server in cluster['servers']:
            id = server['id']
            if srv_id_dict.has_key(id):
                continue

            if testbase.cleanup_pgs_log_and_ckpt( cluster['cluster_name'], server ) is not 0:
                util.log( 'failed to cleanup_pgs_data' )
                return -1
            srv_id_dict[id] = True

    # Setup binaries
    if testbase.setup_binaries( config.clusters, opt_skip_copy_binaries, opt_32bit_binary_test ) != 0:
        util.log('failed to initialize testbase')
        return -1

    return 0

def load_test_modules(opt_32bit_binary_test):
    module_list = []
    if 'all' in sys.argv:
        for file_name in os.listdir('.'):
            if file_name.startswith('test_'):
                if opt_32bit_binary_test and file_name.endswith('32.py'):
                    module_list.append(__import__(file_name[:-3]))
                elif opt_32bit_binary_test == False and file_name.endswith('.py') and file_name.endswith('32.py') == False:
                    module_list.append(__import__(file_name[:-3]))
    else:
        for i in range(1, len(sys.argv)):
            file_name = sys.argv[i]
            if file_name[0] != '-' and file_name.endswith('.py'):
                module_list.append(__import__(file_name[-3:] == '.py' and file_name[:-3] or file_name))

    return module_list

def test_modules(module_list, opt_non_interactive, opt_backup_log_dir):
    if opt_non_interactive:
        test_results = []
        for module in module_list:
            ret = run_test(module, None, None)
            test_results.append(ret)

        # Summary
        print "\n\n### SUMMARY ###\n"
        errors = 0
        failures = 0
        for ret in test_results:
            errors += len(ret.errors)
            failures += len(ret.failures)

            for e in ret.errors:
                util.log(e[0])
                util.log(e[1])
                util.log('')

            for f in ret.failures:
                util.log(f[0])
                util.log(f[1])
                util.log('')

        util.log("Test done. failures:%d, errors:%d" % (failures, errors))

        if errors > 0 or failures > 0:
            if opt_backup_log_dir is not None:
                util.backup_log( opt_backup_log_dir )
            return -1
        else:
            util.log("No Error!")

    else:
        console(module_list)

    return 0


USAGE = """usage:
  python testrunner.py [options] [file ..  or all]

  python testrunner.py test_confmaster.py
  python testrunner.py -n all
  python testrunner.py -n -b all

option:
  -i <init> : Without test, run only nBase-ARC processes.
  -l <backup_log_dir> : Backup test-logs to the specified directory.
  -s <skip-copy-binareis> : Skip copying binaries. (It must be used when binaries already be deployed.)
  -b <32bit-binary-test> : Test 32bit binary
  -n <non-interactive> : Run specified tests without any interaction with a user.

Read README file for more details.
"""

def main():
    if len(sys.argv) < 2:
        print USAGE
        return -1

    signal.signal( signal.SIGINT, signal_handler )

    # Verify config
    config.verify_config()

    # Init options
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'inl:sb', ['init', 'non-interactive', 'backup_log_dir', 'skip-copy_binaries', '32bit-binary-test'])
    except getopt.GetoptError as e:
        print USAGE
        print e
        return -1

    opt_init = False
    opt_backup_log_dir = None
    opt_skip_copy_binaries = False
    opt_32bit_binary_test = False
    opt_non_interactive = False
    for opt, arg in opts:
        if opt in ("-i", '--init'):
            opt_init = True
        elif opt in ("-l", '--backup_log_dir'):
            opt_backup_log_dir = arg
        elif opt in ("-s", '--skip-copy-binareis'):
            opt_skip_copy_binaries = True
        elif opt in ("-b", '--32bit-binary-test'):
            opt_32bit_binary_test = True
        elif opt in ("-n", '--non-interactive'):
            opt_non_interactive = True

    # Clean up test environment
    if cleanup_test_env(opt_skip_copy_binaries, opt_32bit_binary_test) != 0:
        print 'Clean up test environment fail! Aborting...'
        return -1

    # When -i flag is on, it exits after setting up a cluster.
    if opt_init is True:
        if default_cluster.initialize_starting_up_smr_before_redis( config.clusters[0], verbose=2 ) is not 0:
            util.log('failed setting up servers.')
        else:
            util.log('finished successfully setting up servers.' )
        return 0

    # Load test modules
    module_list = load_test_modules(opt_32bit_binary_test)
    print "module list : "
    print module_list

    # Run test
    return test_modules(module_list, opt_non_interactive, opt_backup_log_dir)

if __name__ == '__main__':
    exit(main())
