import unittest
import sys
import test_base
import config
import util
import getopt

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

  unittest.TextTestRunner(verbosity=2).run(alltests)
    

def run_test(module, testcase, method):
  suite = None
  if testcase == None:
    suite = unittest.TestLoader().loadTestsFromModule(module)
  elif method == None:
    suite = unittest.TestLoader().loadTestsFromTestCase(getattr(module, testcase))
  else:
    suite = unittest.TestSuite()
    suite.addTest(getattr(module, testcase)(method))
  unittest.TextTestRunner(verbosity=2).run(suite)


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


import imp
import sys

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

if __name__ == '__main__':
  if len(sys.argv) < 2:
    print "you must specify test module name"
    sys.exit(1)

  module_list = []
  for i in range(1, len(sys.argv)):
    if sys.argv[i][0] != '-':
      module_list.append(__import__(sys.argv[i][-3:] == '.py' and sys.argv[i][:-3] or sys.argv[i]))

  try:
    opts, args = getopt.getopt(sys.argv[2:], 's', ['skip-copy_binaries'])
  except getopt.GetoptError:
    print 'usage: tester_startup.py [-i] [-l <backup_log_dir>] [-s]'
    sys.exit(3)

  config.verify_config()

  skip_copy_binaries = False 
  for opt, arg in opts:
    if opt in ("-s", '--skip-copy-binareis'):
      skip_copy_binaries  = True

  if 0 != test_base.initialize( config.physical_machines, config.clusters, skip_copy_binaries ):
    util.log('failed to initialize test_base')
    sys.exit(2)
  
  console(module_list)
