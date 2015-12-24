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

#ifndef _SMRMP_H_
#define _SMRMP_H_

/*
 * parses request into 'array of char *'. (NULL at last element)
 * caller shoud free returned token array via smrmp_free_request function
 */
extern int smrmp_parse_msg (char *buf, int buf_sz, char ***rtok);
extern void smrmp_free_msg (char **rtok);
#endif
