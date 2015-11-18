#ifndef _ARCCI_H_
#define _ARCCI_H_

#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>

#ifndef _WIN32
#define ARCCI_EXPORT
#else
#define ARCCI_EXPORT __declspec(dllexport)
#endif

/**
 * arcci (C Interface for nBase-ARC)
 */

#ifdef __cplusplus
extern "C"
{
#endif

  ARCCI_EXPORT typedef void arc_t;
  ARCCI_EXPORT typedef struct arc_conf_s arc_conf_t;
  ARCCI_EXPORT typedef void arc_request_t;
  ARCCI_EXPORT typedef struct arc_reply_s arc_reply_t;

  /** back-end log level **/
  ARCCI_EXPORT typedef enum
  {
    ARC_LOG_LEVEL_NOLOG = 0,
    ARC_LOG_LEVEL_ERROR = 1,
    ARC_LOG_LEVEL_WARN = 2,
    ARC_LOG_LEVEL_INFO = 3,
    ARC_LOG_LEVEL_DEBUG = 4
  } arc_log_level_t;

  /** return constants **/
  ARCCI_EXPORT enum
  {
    ARC_OK = 0,
    ARC_ERR_NOMEM = -1,		// memory allocation failed 
    ARC_ERR_BAD_ADDRFMT = -2,	// bad address format
    ARC_ERR_TOO_DEEP_RESP = -3,	// repsonses have too deep depth
    ARC_ERR_TIMEOUT = -4,	// request timeout
    ARC_ERR_FE_INTERNAL = -5,	// API front end internal error
    ARC_ERR_BE_DISCONNECTED = -6,	// backend disconnected
    ARC_ERR_BAD_CONF = -7,	// bad configuration
    ARC_ERR_SYSCALL = -8,	// system call failed
    ARC_ERR_ARGUMENT = -9,	// bad argument 
    ARC_ERR_BAD_RQST_STATE = -10,	// bad request state
    ARC_ERR_CONN_CLOSED = -11,	// connection closed
    ARC_ERR_GW_PROTOCOL = -12,	// bad protocol (may be not a gateway)
    ARC_ERR_TOO_BIG_DATA = -13,	// too big data (more than 1G)
    ARC_ERR_BE_INTERNAL = -14,	// backend internal error
    ARC_ERR_BAD_BE_JOB = -15,	// backend pipe error
    ARC_ERR_ZOOKEEPER = -16,	// zookeeper api call failed
    ARC_ERR_AE_ADDFD = -17,	// too big (many) file descriptors
    ARC_ERR_BAD_ZKDATA = -18,	// bad zookeeper znode data
    ARC_ERR_INTERRUPTED = -19,	// back-end thread is interrupted
    ARC_ERR_GENERIC = -98,	// unidentified error (should not be seen)
    /* error is from backend */
    ARC_ERR_BACKEND = -100,	// err is from backend
    ARC_ERR_PARTIAL = -101,	// err is from bakend and partial result exists 
  };

  /** reply type enum **/
  ARCCI_EXPORT typedef enum
  {
    ARC_REPLY_ERROR = 1,
    ARC_REPLY_STATUS = 2,
    ARC_REPLY_INTEGER = 3,
    ARC_REPLY_STRING = 4,
    ARC_REPLY_ARRAY = 5,
    ARC_REPLY_NIL = 6,
  } arc_reply_type_t;

  struct ARCCI_EXPORT arc_conf_s
  {
    int num_conn_per_gw;	// number of connection per gateway (min:2, default:3)
    int init_timeout_millis;	// initialization timeout in milliseconds (min:3000, default: 10000)
    arc_log_level_t log_level;	// application log level (default: ARC_LOG_LEVEL_NOLOG)
    char *log_file_prefix;	// log file prefix (default: NULL)
    int max_fd;			// maximum number of file descriptors of the calling process (min:1024, default: 4096)
    int conn_reconnect_millis;	// gateway reconnect trial interval after disconnection (min:100, default: 1000)
    int zk_reconnect_millis;	// zookeeper reconnect trial interval after disconnection (min:100, default: 1000)
    int zk_session_timeout_millis;	// zookeeper session timeout (min:1000, default: 10000)
  };

  /** reply structure for individual redis command **/
  struct ARCCI_EXPORT arc_reply_s
  {
    arc_reply_type_t type;
    union
    {
      // ARC_REPLY_ERROR 
      struct
      {
	int len;
	char *str;
      } error;
      // ARC_REPLY_STATUS 
      struct
      {
	int len;
	char *str;
      } status;
      // ARC_REPLY_INTEGER 
      struct
      {
	long long val;
      } integer;
      // ARC_REPLY_STRING
      struct
      {
	int len;
	char *str;
      } string;
      // ARC_REPLY_ARRAY
      struct
      {
	int len;
	arc_reply_t **elem;
      } array;
    } d;
  };

/**
 * \brief Initialize arc_conf_t structure with default values.
 *
 * \param conf A pointer to arc_conf_t structure to be initialized.
 */
  extern void ARCCI_EXPORT arc_init_conf (arc_conf_t * conf);

/**
 * \brief Connect to a cluster by zookeeper gateway lookup method.
 *
 * \param hosts Comma separated host:port pairs, each corresponding zookeeper server.
 * \param cluster_name Name of the cluster.
 * \param conf Configuration.
 * \return Pointer to arc_t handle. 
 *   Otherwise NULL is returned and proper error code is set in errno.
 */
  extern arc_t ARCCI_EXPORT *arc_new_zk (const char *hosts,
					 const char *cluster_name,
					 const arc_conf_t * conf);
/**
 * \brief Connect to a cluster by gateway list.
 *
 * \param gateways Comma separated host:port pairs.
 * \param conf Cconfiguration.
 * \return Pointer to arc_t handle. 
 *   Otherwise NULL is returned and proper error code is set in errno.
 */
  extern arc_t ARCCI_EXPORT *arc_new_gw (const char *gateways,
					 const arc_conf_t * conf);

/**
 * \brief Destroy the arc_t handle and release its resources.
 *
 * After this call, arc_t handle will no longer be valid. This function will
 * interrupt the back-end thread and wait for the thread complete any outstanding jobs.
 *
 * \param arc A arc_t handle created by \ref arc_new_zk or \ref zrc_new_gw.
 */
  extern void ARCCI_EXPORT arc_destroy (arc_t * arc);

/**
 * \brief Create a request structure for send/receive REDIS commands.
 *
 * \return A pointer to arc_request_t.
 */
  extern arc_request_t ARCCI_EXPORT *arc_create_request (void);

/**
 * \brief Destroy a arc_request_t and release its resources.
 * After this call, the arc_request_t pointer will no longer be valid.
 * A request must be not in processing state. (i.e. not in \ref arc_do_request call)
 *
 * \param A pointer to arc_request_t.
 */
  extern void ARCCI_EXPORT arc_free_request (arc_request_t * rqst);

/**
 * \brief Append a REDIS command to the request.
 *
 * \param rqst A pointer to arc_request_t.
 * \param format printf style format with %b (binary) extension.
 *   When using %b you need to provide both pointer the string and the length (size_t) in bytes.
 *   rc = arc_append_command (rqst, "GET %s", mykey);
 *   rc = arc_append_command (rqst, "SET %s %b", mykey, myval, myvallen); 
 * \return 0 if successful.
 *   If error occurred, -1 is returnd and proper error code is set to errno.
 */
  extern int ARCCI_EXPORT arc_append_command (arc_request_t * rqst,
					      const char *format, ...);

/**
 * \brief Append a REDIS command to the request. (va_arg style)
 *
 * \param rqst A pointer to arc_request_t.
 * \param format printf style format with %b extension. \see arc_append_command.
 * \param ap va_list of variable arguments.
 * \return 0 if successful.
 *   If error occurred, -1 is returnd and proper error code is set to errno.
 */
  extern int ARCCI_EXPORT arc_append_commandv (arc_request_t * rqst,
					       const char *format,
					       va_list ap);

/**
 * \brief Append a REDIS command to the request. (argc, argv style)
 *
 * \param rqst A pointer to arc_request_t.
 * \param format printf style format with %b extension. \see arc_append_command.
 * \param argc Number of arguments.
 * \param argv Array of pointer to values.
 * \param argvlen Array of value size.
 * \return 0 if successful.
 *   If error occurred, -1 is returnd and proper error code is set to errno.
 */
  extern int ARCCI_EXPORT arc_append_commandcv (arc_request_t * rqst,
						int argc, const char **argv,
						const size_t * argvlen);

/**
 * \brief Send REDIS commands and get replies for each command.
 *  This methods blocks unless 1) all the replies are received 
 *  2) timeout 3) other error condition has met.
 *
 * \param arc Pointer to arc_t.
 * \param rqst Pointer to arc_request_t.
 * \param timeout_millis Timeout specified in milliseconds.
 * \param be_errno Placeholder of the back-end error code.
 * \returns 0 if successfu1. 
 *   If error occurred, -1 returned and proper error code is set to errno.
 *   When error code is ARC_ERR_BACKEND or ARC_ERR_PARTIAL, be_errno is set to
 *   back-end errno.
 */
  extern int ARCCI_EXPORT arc_do_request (arc_t * arc, arc_request_t * rqst,
					  int timeout_millis, int *be_errno);

/**
 * \brief Get next reply. If there is no more reply, then reply parameter is set to NULL.
 *  Note: At successful return, caller must free the reply by \ref arc_free_reply if it is not NULL.
 *
 * \param rqst Pointer to arc_request_t
 * \param reply Placeholder for the reply 
 * \param be_errno Placeholder of the back-end error code.
 * \returns 0 if successfu1. 
 *   If error occurred, -1 returned and proper error code is set to errno.
 *   When error code is ARC_ERR_BACKEND or ARC_ERR_PARTIAL, be_errno is set to
 *   back-end errno.
 */
  extern int ARCCI_EXPORT arc_get_reply (arc_request_t * rqst,
					 arc_reply_t ** reply, int *be_errno);

/**
 * \brief Free reply got by \ref arc_get_reply
 *
 * \param reply Pointer to arc_reply_t
 */
  extern void ARCCI_EXPORT arc_free_reply (arc_reply_t * reply);

#ifdef __cplusplus
}
#endif

#endif				/* _ARCCI_H_ */
