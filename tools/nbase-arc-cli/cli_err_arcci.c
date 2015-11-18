#include <stdio.h>
#include "sds.h"
#include "arcci.h"

sds
sdscaterr (sds s, int be_errno)
{
  int n;

  if (!s)
    {
      return NULL;
    }

  if (errno == ARC_ERR_BACKEND || errno == ARC_ERR_PARTIAL)
    {
      n = be_errno;
    }
  else
    {
      n = errno;
    }

  switch (n)
    {
    case ARC_ERR_NOMEM:
      s = sdscat (s, "Memory allocation failed");
      break;
    case ARC_ERR_BAD_ADDRFMT:
      s = sdscat (s, "Bad address format");
      break;
    case ARC_ERR_TOO_DEEP_RESP:
      s = sdscat (s, "Repsonses have too deep depth");
      break;
    case ARC_ERR_TIMEOUT:
      s = sdscat (s, "Request timeout");
      break;
    case ARC_ERR_FE_INTERNAL:
      s = sdscat (s, "API front end internal error");
      break;
    case ARC_ERR_BE_DISCONNECTED:
      s = sdscat (s, "Backend disconnected");
      break;
    case ARC_ERR_BAD_CONF:
      s = sdscat (s, "Bad configuration");
      break;
    case ARC_ERR_SYSCALL:
      s = sdscat (s, "System call failed");
      break;
    case ARC_ERR_ARGUMENT:
      s = sdscat (s, "Bad argument");
      break;
    case ARC_ERR_BAD_RQST_STATE:
      s = sdscat (s, "Bad request state");
      break;
    case ARC_ERR_CONN_CLOSED:
      s = sdscat (s, "Connection closed");
      break;
    case ARC_ERR_GW_PROTOCOL:
      s = sdscat (s, "Bad protocol (may be not a gateway)");
      break;
    case ARC_ERR_TOO_BIG_DATA:
      s = sdscat (s, "Too big data (more than 1G)");
      break;
    case ARC_ERR_BE_INTERNAL:
      s = sdscat (s, "Backend internal error");
      break;
    case ARC_ERR_BAD_BE_JOB:
      s = sdscat (s, "Backend pipe error");
      break;
    case ARC_ERR_ZOOKEEPER:
      s = sdscat (s, "Zookeeper api call failed");
      break;
    case ARC_ERR_AE_ADDFD:
      s = sdscat (s, "Too big (many) file descriptors");
      break;
    case ARC_ERR_BAD_ZKDATA:
      s = sdscat (s, "Bad zookeeper znode data");
      break;
    case ARC_ERR_INTERRUPTED:
      s = sdscat (s, "Back-end thread is interrupted");
      break;
    case ARC_ERR_GENERIC:
      s = sdscat (s, "Unidentified error (should not be seen)");
      break;
    default:
      s = sdscatprintf (s, "Unknow error %d", n);
    }

  if (errno == ARC_ERR_BACKEND)
    {
      s = sdscat (s, "(from API backend)");
    }
  else if (errno == ARC_ERR_PARTIAL)
    {
      s = sdscat (s, "(partial success)");
    }

  return s;
}

sds
sdscatperr (sds s, const char *str, int be_errno)
{
  sds err;

  if (!s)
    {
      return NULL;
    }

  err = sdscaterr (sdsempty (), be_errno);
  s = sdscatprintf (s, "%s: %s\n", str, err);
  sdsfree (err);

  return s;
}

void
perrfp (FILE * fp, const char *str, int be_errno)
{
  sds err;

  if (!fp)
    {
      return;
    }

  err = sdscaterr (sdsempty (), be_errno);
  fprintf (fp, "%s: %s\n", str, err);
  sdsfree (err);
}

void
perr (const char *str, int be_errno)
{
  perrfp (stderr, str, be_errno);
}
