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

#ifndef _SFI_H_
#define _SFI_H_
/* 
 * Simple fault injection supporting module.
 *
 * This module is indented to be used ONLY FOR TEST PHASE.
 * Every function is void for code simplicity.
 *
 * Note:
 * This module is thread-safe, but bear in mind that callback function
 * is called with read-locked mode. When callback is not returning, functions
 * need write-lock mode (such as enable/disable etc.) is pended.
 */

/* data got from probe point */
typedef struct
{
  long arg1;
  long arg2;
  long arg3;
  long arg4;
} sfi_probe_arg;
#define sfi_probe_arg_init(a,a1,a2,a3,a4) do { \
  (a)->arg1 = a1;                              \
  (a)->arg2 = a2;                              \
  (a)->arg3 = a3;                              \
  (a)->arg4 = a4;                              \
} while(0)

typedef enum
{
  CB_ERROR = -1,
  CB_OK_DONE = 0,		//ok, delete probe
  CB_OK_HAS_MORE = 1		//ok, continue
} SFI_CB_RET;
typedef SFI_CB_RET (*sfi_callback) (const char *name, sfi_probe_arg * pargs,
				    void *arg);
typedef void (*sfi_dtor) (void *arg);

extern int sfi_enabled (void);
#ifdef SFI_ENABLED
extern void sfi_init (void);
extern void sfi_term (void);
extern void sfi_enable (const char *name, sfi_callback cb, sfi_dtor dtor,
			void *arg);
extern void sfi_disable (const char *name);
extern void sfi_disable_all (void);
extern void sfi_probe (const char *name, sfi_probe_arg * arg);
extern int sfi_probe_count (void);
#define SFI_PROBE4(name,a1,a2,a3,a4) do {  \
  sfi_probe_arg arg;                       \
  sfi_probe_arg_init(&arg,a1,a2,a3,a4);    \
  sfi_probe(name,&arg);                    \
} while (0)
#else /* SFI_ENABLED */
#define sfi_init()
#define sfi_term()
#define sfi_is_enabled()
#define sfi_enable(n,cb,dest,arg)
#define sfi_disable(n)
#define sfi_disable_all()
#define sfi_probe(n,a)
#define sif_probe_count() 0
#define SFI_PROBE4(name,a1,a2,a3,a4)
#endif /* SFI_ENABLED */

#define SFI_PROBE3(name,a1,a2,a3) SFI_PROBE4(name,a1,a2,a3,0)
#define SFI_PROBE2(name,a1,a2) SFI_PROBE4(name,a1,a2,0,0)
#define SFI_PROBE1(name,a1) SFI_PROBE4(name,a1,0,0,0)
#define SFI_PROBE(name) SFI_PROBE4(name,0,0,0,0)
#endif /* _SFI_H_ */
