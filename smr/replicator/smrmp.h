#ifndef _SMRMP_H_
#define _SMRMP_H_

/* 
 * SMR replicator management protocol
 *
 * +------------------------+
 * | general message format |
 * +------------------------+
 * - request
 *   signle line request that ends with \r\n
 *
 * - response (subset of redis protocol)
 *   With an error message the first byte of the reply will be "-"
 *   With a single line reply the first byte of the reply will be "+"
 *   With multi-bulk reply the first byte of the reply will be "*"
 *
 * +-------------------------------------+
 * | management ptorotocol specification |
 * +-------------------------------------+
 *
 * ---------------------------------------------------------------------------
 * ==> quit
 * (no reponse and connection is closed)
 * +------------+
 * | MONITORING |
 * +------------+
 * ==>ping
 * <== +OK <role> <commit seq>
 *
 * ==> getseq log
 * <== +OK log <min> <commit> <log>
 *
 * ==> getseq client
 * <== +OK client <sent> <consumed>
 *
 * ==> getseq slave
 * <== +OK slave <nid> <sent> <acked> [slave <nid> <sent> <acked>...]
 *
 * ==> getseq all
 * <== <all of sequences are sent>
 *
 * +----------+
 * | ROLE SET |
 * +----------+
 * ---------------------------------------------------------------------------
 * ==> role master <node id> <quorum policy> [<rewind cseq>]
 * <== +OK
 *
 * ==> role slave <node id> <master host> <base port> [<rewind cseq>]
 * <== +OK
 *
 * ==> role none 
 * <== +OK
 *
 * +-----------+
 * | MIGRATION |
 * +-----------+
 * ==> migrate start <dest host> <dest base port> <seq> <tps limit> <num part> <RLE>
 * <== +OK <migration ID>
 *
 * ==> migrate interrupt
 * <== +OK
 *
 * ==> migrate info
 * <== +OK <mig id> <log seq> <mig seq> (when it is running)
 *
 * +----------------------+
 * | LIVE RECONFIGURATION |
 * +----------------------+
 * ---------------------------------------------------------------------------
 * ==> prepare to_master
 * <== +OK
 *
 * ==> rollback to_master
 * <== +OK
 *
 * ==> commit to_master <last committed seq>
 * <== +OK
 *
 * ---------------------------------------------------------------------------
 * ==> prepare to_slave <master host> <base port>
 * <== +OK
 *
 * ==> rollback to_slave
 * <== +OK
 *
 * ==> commit to_slave
 * <== +OK <last committed seq>
 *
 * ---------------------------------------------------------------------------
 * ==> prepare change_master <master host> <base port>
 * <== +OK
 *
 * ==> rollback change_master
 * <== +OK
 *
 * ==> commit change_master <last committed seq>
 * <== +OK
 *
 */

/*
 * parses request into 'array of char *'. (NULL at last element)
 * caller shoud free returned token array via smrmp_free_request function
 */
extern int smrmp_parse_msg (char *buf, int buf_sz, char ***rtok);
extern void smrmp_free_msg (char **rtok);
#endif
