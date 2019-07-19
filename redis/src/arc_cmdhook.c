#ifdef NBASE_ARC
#include "arc_internal.h"

int
arc_georadius_store_hook (client * c)
{
  if (arc.cluster_mode)
    {
      addReplyError (c, "STORE option is not supported");
      return 1;
    }
  return 0;
}

#endif
