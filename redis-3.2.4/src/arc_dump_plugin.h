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

#ifndef __DUMP_PLUGIN_H
#define __DUMP_PLUGIN_H

#define RET_OK 0
#define RET_ERR -1

#define PLUGIN_RDB_TYPE_STRING 's'
#define PLUGIN_RDB_TYPE_LIST   'l'
#define PLUGIN_RDB_TYPE_SET    'S'
#define PLUGIN_RDB_TYPE_ZSET   'z'
#define PLUGIN_RDB_TYPE_HASH   'h'
#define PLUGIN_RDB_TYPE_SSS    't'

#define PLUGIN_SSS_KV_LIST 'l'
#define PLUGIN_SSS_KV_SET  's'

/* Return value of each function indicates success or fail 
 * RET_OK  = success
 * RET_ERR = fail   */
struct dump_plugin_callback
{
  /* pctx is out parameter of ctx */
  int (*initialize) (int argc, char **argv, void **pctx);

  int (*dumpinfo) (void *ctx, int rdbver, long long smr_seqnum,
		   long long smr_mstime);
  int (*begin_key) (void *ctx, int type, char *key, int keylen,
		    long long expiretime);

  int (*string_val) (void *ctx, char *val, int vallen);
  int (*list_val) (void *ctx, char *val, int vallen);
  int (*set_val) (void *ctx, char *val, int vallen);
  int (*zset_val) (void *ctx, char *val, int vallen, double score);
  int (*hash_val) (void *ctx, char *hkey, int hkeylen, char *hval,
		   int hvallen);

  int (*begin_sss_collection) (void *ctx, char *ks, int kslen, char *svc,
			       int svclen, char *key, int keylen, int mode);
  int (*sss_val) (void *ctx, char *val, int vallen, long long val_expire);
  int (*end_sss_collection) (void *ctx, int mode);

  int (*end_key) (void *ctx, int type);

  /* finalization of ctx is required. */
  int (*finalize) (void *ctx);
};

#endif
