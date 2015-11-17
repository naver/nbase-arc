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
