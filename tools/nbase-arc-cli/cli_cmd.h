#ifndef _CLI_CMD_H_
#define _CLI_CMD_H_

#include "arcci.h"
#include "sds.h"

sds executeCommand (arc_t * arc, int argc, char **argv, int timeout,
		    unsigned raw_mode, sds * err);

sds executeCommandArcci (arc_t * arc, int argc, char **argv, int timeout,
			 unsigned raw_mode, sds * err);
arc_reply_t *executeCommandArcReply (arc_t * arc, int argc, char **argv,
				     int timeout, arc_request_t ** ret_rqst,
				     sds * err);

int isCliCommand (char *cmd);
sds executeCommandCli (arc_t * arc, int argc, char **argv, int timeout,
		       sds * err);

int isRedisCommand (char *cmd);
sds executeCommandRedis (arc_t * arc, int argc, char **argv,
			 int timeout, sds * err);
#endif
