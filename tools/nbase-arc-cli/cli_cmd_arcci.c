#include <stdio.h>
#include <string.h>
#include "sds.h"
#include "arcci.h"
#include "cli_err_arcci.h"

static sds
cliFormatReplyTTY (arc_reply_t * r, const char *prefix)
{
  sds out = sdsempty ();
  switch (r->type)
    {
    case ARC_REPLY_ERROR:
      out = sdscatprintf (out, "(error) %s\n", r->d.error.str);
      break;
    case ARC_REPLY_STATUS:
      out = sdscat (out, r->d.status.str);
      out = sdscat (out, "\n");
      break;
    case ARC_REPLY_INTEGER:
      out = sdscatprintf (out, "(integer) %lld\n", r->d.integer.val);
      break;
    case ARC_REPLY_STRING:
      /* If you are producing output for the standard output we want
       * a more interesting output with quoted characters and so forth */
      out = sdscatrepr (out, r->d.string.str, r->d.string.len);
      out = sdscat (out, "\n");
      break;
    case ARC_REPLY_NIL:
      out = sdscat (out, "(nil)\n");
      break;
    case ARC_REPLY_ARRAY:
      if (r->d.array.len == 0)
	{
	  out = sdscat (out, "(empty list or set)\n");
	}
      else
	{
	  unsigned int idxlen = 0;
	  int i;
	  char _prefixlen[16];
	  char _prefixfmt[16];
	  sds _prefix;
	  sds tmp;

	  /* Calculate chars needed to represent the largest index */
	  i = r->d.array.len;
	  do
	    {
	      idxlen++;
	      i /= 10;
	    }
	  while (i);

	  /* Prefix for nested multi bulks should grow with idxlen+2 spaces */
	  memset (_prefixlen, ' ', idxlen + 2);
	  _prefixlen[idxlen + 2] = '\0';
	  _prefix = sdscat (sdsnew (prefix), _prefixlen);

	  /* Setup prefix format for every entry */
	  snprintf (_prefixfmt, sizeof (_prefixfmt), "%%s%%%dd) ", idxlen);

	  for (i = 0; i < r->d.array.len; i++)
	    {
	      /* Don't use the prefix for the first element, as the parent
	       * caller already prepended the index number. */
	      out =
		sdscatprintf (out, _prefixfmt, i == 0 ? "" : prefix, i + 1);

	      /* Format the multi bulk entry */
	      tmp = cliFormatReplyTTY (r->d.array.elem[i], _prefix);
	      if (!tmp)
		{
		  sdsfree (out);
		  sdsfree (_prefix);
		  return NULL;
		}
	      out = sdscatlen (out, tmp, sdslen (tmp));
	      sdsfree (tmp);
	    }
	  sdsfree (_prefix);
	}
      break;
    default:
      sdsfree (out);
      return NULL;
    }
  return out;
}

static sds
cliFormatReplyRaw (arc_reply_t * r)
{
  sds out = sdsempty (), tmp;
  int i;

  switch (r->type)
    {
    case ARC_REPLY_NIL:
      /* Nothing... */
      break;
    case ARC_REPLY_ERROR:
      out = sdscatlen (out, r->d.error.str, r->d.error.len);
      out = sdscatlen (out, "\n", 1);
      break;
    case ARC_REPLY_STATUS:
      out = sdscatlen (out, r->d.status.str, r->d.status.len);
      break;
    case ARC_REPLY_STRING:
      out = sdscatlen (out, r->d.string.str, r->d.string.len);
      break;
    case ARC_REPLY_INTEGER:
      out = sdscatprintf (out, "%lld", r->d.integer.val);
      break;
    case ARC_REPLY_ARRAY:
      for (i = 0; i < r->d.array.len; i++)
	{
	  if (i > 0)
	    out = sdscat (out, "\n");
	  tmp = cliFormatReplyRaw (r->d.array.elem[i]);
	  if (!tmp)
	    {
	      sdsfree (out);
	      return NULL;
	    }
	  out = sdscatlen (out, tmp, sdslen (tmp));
	  sdsfree (tmp);
	}
      break;
    default:
      sdsfree (out);
      return NULL;
    }
  return out;
}

static size_t *
makeArgvlenArray (int argc, char **argv)
{
  size_t *argvlen;
  int i;

  argvlen = malloc (argc * sizeof (size_t));
  if (!argvlen)
    {
      errno = ARC_ERR_NOMEM;
      return NULL;
    }

  for (i = 0; i < argc; i++)
    {
      argvlen[i] = strlen (argv[i]);
    }

  return argvlen;
}

arc_reply_t *
executeCommandArcReply (arc_t * arc, int argc, char **argv,
			int timeout, arc_request_t ** ret_rqst, sds * err)
{
  arc_request_t *rqst;
  arc_reply_t *reply;
  size_t *argvlen;
  int ret, be_errno;

  *ret_rqst = NULL;
  rqst = arc_create_request ();
  if (!rqst)
    {
      if (err)
	{
	  *err = sdscatperr (*err, "Execute command", 0);
	}
      return NULL;
    }

  argvlen = makeArgvlenArray (argc, argv);
  if (!argvlen)
    {
      if (err)
	{
	  *err = sdscatperr (*err, "Execute command", 0);
	}
      arc_free_request (rqst);
      return NULL;
    }

  ret = arc_append_commandcv (rqst, argc, (const char **) argv, argvlen);
  free (argvlen);
  if (ret == -1)
    {
      if (err)
	{
	  *err = sdscatperr (*err, "Execute command", 0);
	}
      arc_free_request (rqst);
      return NULL;
    }

  ret = arc_do_request (arc, rqst, timeout, &be_errno);
  if (ret == -1)
    {
      if (err)
	{
	  *err = sdscatperr (*err, "Execute command", be_errno);
	}
      arc_free_request (rqst);
      return NULL;
    }

  ret = arc_get_reply (rqst, &reply, &be_errno);
  if (ret == -1)
    {
      if (err)
	{
	  *err = sdscatperr (*err, "Execute command", be_errno);
	}
      arc_free_request (rqst);
      return NULL;
    }

  *ret_rqst = rqst;
  return reply;
}

/* if *err is NULL, don't return any error message */
sds
executeCommandArcci (arc_t * arc, int argc, char **argv,
		     int timeout, unsigned raw_mode, sds * err)
{
  arc_request_t *rqst;
  arc_reply_t *reply;
  sds out;

  reply = executeCommandArcReply (arc, argc, argv, timeout, &rqst, err);
  if (!reply)
    {
      return NULL;
    }

  if (raw_mode)
    {
      out = cliFormatReplyRaw (reply);
    }
  else
    {
      out = cliFormatReplyTTY (reply, "");
    }
  arc_free_reply (reply);
  arc_free_request (rqst);

  if (!out)
    {
      if (err)
	{
	  *err = sdscat (*err, "Unknown reply type");
	}
      return NULL;
    }

  return out;
}
