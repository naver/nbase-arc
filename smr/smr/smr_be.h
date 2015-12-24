/*
 * Copyright 2015 Naver Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _SMR_BE_H_
#define _SMR_BE_H_

typedef struct smrConnector smrConnector;
typedef struct smrData smrData;

typedef struct smrCallback
{
  /* ------------------ */
  /* replication stream */
  /* ------------------ */
  int (*session_close) (void *arg, long long seq, short nid, int sid);
  int (*session_data) (void *arg, long long seq, long long timestamp,
		       short nid, int sid, int hash, smrData * data,
		       int size);
  /* ------------------------ */
  /* event notification layer */
  /* ------------------------ */
  /* 
   * This callback is called when the local replicator can't serve the log requested.
   * After this callback is returned, user must call 'smr_disconnect' 
   */
  int (*noti_rckpt) (void *arg, char *be_host, int be_port);

  /*
   * This callback is called when the local replicator can serve log
   * Backend can call 'smr_catchup' function at any time 
   */
  int (*noti_ready) (void *arg);
} smrCallback;

/* connect, disconnect */
extern smrConnector *smr_connect_tcp (int local_port, long long ckpt_seq,
				      smrCallback * cb, void *cb_arg);
extern void smr_disconnect (smrConnector * connector);
extern short smr_get_nid (smrConnector * connector);

/* local buffer operations */
extern int smr_session_close (smrConnector * connector, int sid);
extern int smr_session_data (smrConnector * connector, int sid, int hash,
			     char *data, int size);

/* feedback to the local replicator */
extern int smr_seq_ckpted (smrConnector * connector, long long seq);

/* mmaped log direct accessors */
extern int smr_release_seq_upto (smrConnector * connector, long long seq);
extern char *smr_data_get_data (smrData * data);
extern int smr_data_get_size (smrData * data);
extern int smr_data_ref (smrData * data);
extern int smr_data_unref (smrData * data);

/* 
 * you can select/poll/epoll with file descriptor that smr_get_poll_fd returns.
 * It is set to readable when smr related file descriptors are ready to process.
 * you can safely call smr_process function without blocking.
 */
extern int smr_catchup (smrConnector * connector);
extern int smr_get_poll_fd (smrConnector * connector);
/* returns number of events processed. -1 if error */
extern int smr_process (smrConnector * connector);
extern int smr_enable_callback (smrConnector * connector, int enable);
extern long long smr_get_seq (smrConnector * connector);

#endif /* _SMR_BE_H_ */
