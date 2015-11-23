#### nbase-arc C API
The ARCCI (nbaseARC C Interface) is a C client library for nbase-arc.
ARCCI can be used like other Redis C client libraries by specifying a Redis IP/port, but it has unique features such as Zookeeper integration.

#### Features
* Supports RESP (REdis Serialization Protocol)
  - Concepts and APIs for request/response handling are very similar to hiredis (official Redis C API)
* Multi-thread support
  - Handles returned by `arc_new_gw` or `arc_new_zk` can be used safely by multiple threads
  - Each handle created has a back-end thread that handles event driven network I/O
* Connection pooling and load balancing
  - Each handle has a connection pool (`num_conn_per_gw` connections per gateway)
  - ARCCI tries to find the best connection for a request by comparing connection costs. The cost of a connection is calculated based on request key affinity to gateway, number of pending requests on the connection, etc.
* Timeout handling
  - Every request has specified timeout in milliseconds. ARCCI implements hashed-timer-wheel to handle timeout efficiently with low cost.
* Zookeeper integration
  - It can access gateways of a cluster by cluster name. ARCCI embeds Zookeeper C library to get/watch the gateway information on the cluster from the configuration master. By adding/removing gateway information on the configuration master side, any gateway can be added/removed from service without client side interruption.
* Windows support
* Application logging
  - By specifying `log_level` field of `arc_conf_t` at handle creation, you can get detailed ARCCI runtime information

#### Example
```
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include "arcci.h"
int
main (int argc, char *argv[])
{
  int rc, i, error_code = 0;
  arc_t *arc = NULL;
  arc_request_t *request = NULL;
  arc_conf_t conf;

  arc_init_conf (&conf);
  conf.log_level = ARC_LOG_LEVEL_DEBUG;

  // connect to Zookeeper (xxx.xxx.xxx:2181) of the configuration master
  // cluster name is 'test_cluster'
  arc = arc_new_zk ("xxx.xxx.xxx:2181", "test_cluster", &conf);
  if (arc == NULL)
    {
      error_code = errno;
      goto error_exit;
    }

  for (i = 0; i < 10000; i++)
    {
      arc_reply_t *reply;
      int be_errno;

      request = arc_create_request ();
      if (request == NULL)
        {
          error_code = errno;
          goto error_exit;
        }

      // you can append any number of commands (command pipelining)
      rc = arc_append_command (request, "SET key%d %s", i, "test_value");
      // you can append any number of commands (command pipelining)
      rc = arc_append_command (request, "SET key%d %s", i, "test_value");
      if (rc != 0)
        {
          error_code = errno;
          arc_free_request (request);
          goto error_exit;
        }

      rc = arc_do_request (arc, request, 100, &be_errno);
      if (rc != 0)
        {
          error_code = errno;
          if (error_code == ARC_ERR_BACKEND || error_code == ARC_ERR_PARTIAL)
            {
              error_code = be_errno;
            }
          goto error_exit;
        }

      rc = arc_get_reply (request, &reply, &be_errno);
      if (rc != 0)
        {
          error_code = errno;
          if (error_code == ARC_ERR_BACKEND || error_code == ARC_ERR_PARTIAL)
            {
              error_code = be_errno;
            }
          goto error_exit;
        }

      assert (reply != NULL);
      switch (reply->type)
        {
        case ARC_REPLY_ERROR:
          printf ("ERROR len:%d str:%s\n", reply->d.error.len,
                  reply->d.error.str);
          break;
        case ARC_REPLY_STATUS:
          printf ("STATUS len:%d str:%s\n", reply->d.status.len,
                  reply->d.status.str);
          break;
        case ARC_REPLY_INTEGER:
          printf ("INTEGER val:%lld\n", reply->d.integer.val);
          break;
        case ARC_REPLY_STRING:
          printf ("STRING len:%d, val:%s\n", reply->d.string.len,
                  reply->d.string.str);
          break;
        case ARC_REPLY_ARRAY:
          printf ("ARRAY len:%d\n", reply->d.array.len);
          break;
        case ARC_REPLY_NIL:
          printf ("<NULL>\n");
          break;
        default:
          assert (0);
        }
      arc_free_reply (reply);
      arc_free_request (request);
    }

  arc_destroy (arc);
  return 0;

error_exit:
  if (request != NULL)
    {
      arc_free_request (request);
    }
  if (arc != NULL)
    {
      arc_destroy (arc);
    }
  printf ("ERROR error_code:%d\n", error_code);
  return 1;
}
```
