#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>

#include "gpbuf.h"

int
gpbuf_reserve (gpbuf_t * gp, int sz)
{
  char *tmp;
  int bsz;
  int used = 0;

  assert (sz > 0);
  if (gpbuf_avail (gp) >= sz)
    {
      return 0;
    }

  for (bsz = gp->ubsz; bsz + gpbuf_avail (gp) <= sz; bsz += gp->ubsz);

  tmp = malloc ((bsz + gp->sz));
  if (tmp == NULL)
    {
      return -1;
    }

  used = gp->cp - gp->bp;
  memcpy (tmp, gp->bp, used);
  if (gp->bp != gp->ub)
    {
      free (gp->bp);
    }

  gp->sz += bsz;
  gp->bp = tmp;
  gp->cp = tmp + used;
  gp->ep = tmp + gp->sz;

  return 0;
}

int
gpbuf_consume (gpbuf_t * gp, int sz)
{
  if (gp == NULL || sz < 0 || gp->cp + sz > gp->ep)
    {
      return -1;
    }
  gp->cp = gp->cp + sz;
  return 0;
}

void
gpbuf_init (gpbuf_t * gp, char *buf, int sz)
{
  assert (buf != NULL && sz > 0);

  gp->ub = buf;
  gp->ubsz = sz;
  gp->sz = sz;
  gp->bp = buf;
  gp->cp = buf;
  gp->ep = buf + sz;
}

void
gpbuf_cleanup (gpbuf_t * gp)
{
  if (gp->bp != gp->ub)
    {
      free (gp->bp);
    }
  gpbuf_init (gp, gp->ub, gp->ubsz);
}

int
gpbuf_gut (gpbuf_t * gp, char **retdata)
{
  char *bp = NULL;

  if (gp->bp != gp->ub)
    {
      bp = gp->bp;
    }
  else
    {
      bp = malloc (gp->cp - gp->bp + 1);	//+1 for null termination
      if (bp == NULL)
	{
	  return -1;
	}
      memcpy (bp, gp->bp, gp->cp - gp->bp);
      bp[gp->cp - gp->bp] = 0;
    }

  gpbuf_init (gp, gp->ub, gp->ubsz);
  *retdata = bp;
  return 0;
}

int
gpbuf_write (gpbuf_t * gp, char *buf, int count)
{
  int ret;

  if (count < 0 || buf == NULL)
    {
      return -1;
    }

  ret = gpbuf_reserve (gp, count);
  if (ret < 0)
    {
      return ret;
    }

  memcpy (gp->cp, buf, count);
  return 0;
}

int
gpbuf_printf (gpbuf_t * gp, char *format, ...)
{
  int ret;
  int rsz;
  va_list args;

  /* get size */
  va_start (args, format);
  rsz = vsnprintf (NULL, 0, format, args);
  va_end (args);
  if (rsz < 0)
    {
      return -1;
    }

  /* reserve room */
  ret = gpbuf_reserve (gp, rsz + 1);	//+1 for null termination
  if (ret < 0)
    {
      return ret;
    }

  /* print */
  va_start (args, format);
  rsz = vsnprintf (gp->cp, gpbuf_avail (gp), format, args);
  va_end (args);

  gp->cp = gp->cp + rsz;
  return 0;
}
