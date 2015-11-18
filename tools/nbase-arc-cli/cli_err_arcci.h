#ifndef _CLI_ERR_ARCCI_H_
#define _CLI_ERR_ARCCI_H_

#include "sds.h"

sds sdscaterr (sds s, int be_errno);
sds sdscatperr (sds s, const char *str, int be_errno);
void perrfp (FILE * fp, const char *str, int be_errno);
void perr (const char *str, int be_errno);
#endif
