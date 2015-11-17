#ifndef _SMR_H_
#define _SMR_H_
#include <arpa/inet.h>
#include <endian.h>		//for htobe64

#define htonll(x) htobe64(x)
#define ntohll(x) be64toh(x)

/* constraints */
#define SMR_MAX_CLIENTS        32
#define SMR_MAX_SLAVES         SMR_MAX_CLIENTS

/* 
 * SMR protocol specification
 * 
 * [protocol message]
 * 1 byte (see below SMR_OP_XXX)
 *
 * [notation]
 * - backend : smr_be library
 * - local replicator: local replicator w.r.t backend (master or slave)
 *
 * +----------------------------------+
 * | backend <----> master replicator |
 * +----------------------------------+
 * 1) handshake
 *    --> connect
 *    --> nid (got from local replicator)
 *    <-- committed seq
 *
 * 2) --> messages 
 *    SESION_OPEN
 *    SESSION_CLOSE
 *    SESSION_DATA
 *
 * 3) <-- messages
 *    LOG_ACK (bytes logged to the master)
 *
 * +---------------------------------+
 * | backend <----> local replicator |
 * +---------------------------------+
 * 1) handshake
 *    --> connect
 *    --> seq (after which I want get notified)
 *
 * 2) --> messages
 *    SEQ_CONSUMED
 *    SEQ_CKPTED
 *
 * 3) <-- messages
 *    CONFIGURE "init"
 *    SEQ_SAFE
 *    CONFIGURE "new_master"
 *    CONFIGURE "rckpt"
 *
 * +-----------------------------------------+
 * | slave replicator <--> master replicator |
 * +-----------------------------------------+
 * 1) handshake
 *    --> connect
 *    <-- <min seq> (my minimum sequence number)
 *    <-- <max commit seq> (my committed sequence number)
 *    <-- <max seq> (my maximum sequence number)
 *    --> <nid> (my nid)
 *    --> <seq>  (from which slave wants to receive log)
 *
 * 2) --> messages
 *    SEQ_RECEIVED
 *
 * 3) <-- messages
 *    SEQ_COMMITTED
 *    EXT
 *    SESSION_CLOSE
 *    SESSION_DATA
 *    NODE_CHANGE
 */


/* 
 * Only meaningful between replicators
 * payload: any (4 byte) 
 */
#define SMR_OP_EXT    'o'

/* payload: sid (4 byte) */
#define SMR_OP_SESSION_CLOSE   'c'

/* payload: 
 *   sid       (4 byte) 
 *   hash      (4 byte) 
 *   timestamp (8 byte) 
 *   length    (4 byte) 
 *   data      (<length> byte) 
 */
#define SMR_OP_SESSION_DATA    'd'
/* special hash value that is used in migration */
#define SMR_SESSION_DATA_HASH_ALL  (-1)	// e.g.) multi key operation
#define SMR_SESSION_DATA_HASH_NONE (-2)	// e.g.) read only or bad query (needed to reply error in order)

/* payload: ack (8 byte) */
#define SMR_OP_LOG_ACK         'k'
#define SMR_OP_LOG_ACK_SZ      (1 + sizeof(long long))

/* payload: nid (2 byte) */
#define SMR_OP_NODE_CHANGE     'n'

/* payload: magic(4 byte), seq (8 byte), crc16 (2byte, seq (network byte order) */
#define SMR_OP_SEQ_COMMITTED   't'
#define SEQ_COMMIT_MAGIC 0xEBABEFAC	//  SMR_OP_SEQ_COMMITTED related constants
#define SMR_OP_SEQ_COMMITTED_SZ (1 + sizeof(int) + sizeof(long long) + sizeof(unsigned short))

/* payload: seq (8 byte) */
#define SMR_OP_SEQ_RECEIVED    '<'

/* payload: seq (8 byte) */
#define SMR_OP_SEQ_SAFE        '!'

/*
 * payload: length (4 byte), data (<length> byte) 
 *
 * data spec.: one of following sub commands 
 * 1) init <logdir> <mhost> <mport> <commit_seq> <nid>\r\n
 *   <logdir>     : log directory
 *   <mhost>      : master host for backend
 *   <mport>      : master port for backend
 *   <commit_seq> : maximum commit sequence number upto which logs are safe to execute
 *   <nid>        : node id of backend
 *
 * 2) new_master <mhost> <mport> <after_seq> <nid>
 *   <mhost>      : master host for backend
 *   <mport>      : master port for backend
 *   <after_seq>  : from this sequence new master will send log
 *   <nid>        : my nid
 *
 * 3) rckpt <behost> <beport>
 *   <behost>     : backend host from which get checkpoint
 *   <beport>     : backend port
 *   Note:
 *     When this command is processed, backend disconnect from the local replicator.
 * 
 * 4) lconn
 *    This command is sent when replicator state changed from (MASTER | SLAVE) to LCONN
 *
 * ... (more to come)
 */
#define SMR_OP_CONFIGURE       '~'

/* payload: seq (8 byte) */
#define SMR_OP_SEQ_CONSUMED    '&'

/* payload: seq (8 byte) */
#define SMR_OP_SEQ_CKPTED      'v'


#endif
