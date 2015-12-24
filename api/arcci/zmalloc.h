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

#ifndef __ZMALLOC_H
#define __ZMALLOC_H

#include <stdlib.h>

/* Do not use zmalloc at client side */
#ifndef _WIN32

#define zmalloc malloc
#define zfree free
#define zcalloc(sz) calloc(1,sz)
#define zrealloc(p,s) realloc(p,s)

#else

#ifdef CHECK_LEAK
#include <crtdbg.h>
#define DEBUG_MALLOC(size) _malloc_dbg(size, _NORMAL_BLOCK, __FILE__, __LINE__)
#define zmalloc DEBUG_MALLOC 
#define zfree free
#define zcalloc(sz) _calloc_dbg(1, sz, _NORMAL_BLOCK, __FILE__, __LINE__)
#define zrealloc(p,s) _realloc_dbg(p, s, _NORMAL_BLOCK, __FILE__, __LINE__)
#else
#define zmalloc malloc
#define zfree free
#define zcalloc(sz) calloc(1,sz)
#define zrealloc(p,s) realloc(p,s)
#endif	// _DEBUG

#endif

#endif /* __ZMALLOC_H */
