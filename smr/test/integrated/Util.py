import time


def tstop(msg = 'Temporty stop.'):
  try:
    raw_input('\n' + msg + ' Enter to continue...\n')
  except:
    pass
