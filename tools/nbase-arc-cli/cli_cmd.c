#include "arcci.h"
#include "sds.h"
#include "cli_cmd.h"

sds
executeCommand (arc_t * arc, int argc, char **argv, int timeout,
		unsigned raw_mode, sds * err)
{
  if (isCliCommand (argv[0]))
    {
      return executeCommandCli (arc, argc, argv, timeout, err);
    }

  if (isRedisCommand (argv[0]))
    {
      return executeCommandRedis (arc, argc, argv, timeout, err);
    }

  return executeCommandArcci (arc, argc, argv, timeout, raw_mode, err);
}
