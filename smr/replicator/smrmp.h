#ifndef _SMRMP_H_
#define _SMRMP_H_

/*
 * parses request into 'array of char *'. (NULL at last element)
 * caller shoud free returned token array via smrmp_free_request function
 */
extern int smrmp_parse_msg (char *buf, int buf_sz, char ***rtok);
extern void smrmp_free_msg (char **rtok);
#endif
