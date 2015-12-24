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

#ifndef _PART_FILTER_H_
#define _PART_FILTER_H_

typedef struct partFilter partFilter;

extern partFilter *create_part_filter (int num_part);
extern void destroy_part_filter (partFilter * filter);
/* returns 1 if matched, 0 if not matched, -1 if error */
extern int part_filter_get_num_part (partFilter * filter);
extern int part_filter_get (partFilter * filter, int part);
extern int part_filter_set (partFilter * filter, int part, int onoff);
extern int part_filter_set_rle (partFilter * filter, const char *spec);
extern int part_filter_set_rle_tokens (partFilter * filter, char **tokens,
				       int num_tok);
extern int part_filter_format_rle (partFilter * filter, char *buf,
				   int buf_sz);
#endif
