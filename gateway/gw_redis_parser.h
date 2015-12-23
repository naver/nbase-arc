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

#ifndef _GW_RESP_PARSER_H_
#define _GW_RESP_PARSER_H_

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>

#include "gw_config.h"
#include "gw_array.h"
#include "gw_stream_buf.h"
#include "gw_mem_pool.h"
#include "zmalloc.h"
#include "util.h"
#include "sds.h"

#define PARSE_INSUFFICIENT_DATA     0
#define PARSE_COMPLETE              1
#define PARSE_ERROR                 -1

#define PARSE_IOBUF_LEN         (1024*16)
#define PARSE_INLINE_MAX_SIZE   (1024*64)

#define TYPE_INLINE 1
#define TYPE_MULTIBULK 2
#define TYPE_BULK 3
#define TYPE_UNKNOWN 4

typedef struct ParseContext ParseContext;

mempool_hdr_t *createParserMempool (void);
void destroyParserMempool (mempool_hdr_t * mp_parse_ctx);
int getArgumentPosition (ParseContext * ctx, int index, sbuf_pos * ret_start,
			 ssize_t * ret_len);
int getArgumentCount (ParseContext * ctx);
int getParsedStr (ParseContext * ctx, int idx, char **ret_str);
int getParsedNumber (ParseContext * ctx, int idx, long long *ret_ll);
ParseContext *createParseContext (mempool_hdr_t * mp_parse_ctx,
				  sbuf_hdr * stream);
void resetParseContext (ParseContext * ctx, sbuf_hdr * stream);
void deleteParseContext (ParseContext * ctx);
int requestParser (ParseContext * ctx, sbuf_hdr * stream, sbuf ** query,
		   sds * err);
int replyParser (ParseContext * ctx, sbuf_hdr * stream, sbuf ** query,
		 sds * err);
#endif
