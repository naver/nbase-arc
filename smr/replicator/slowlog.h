#ifndef _SLOWLOG_H_
#define _SLOWLOG_H_

typedef struct slowLog_ slowLog;
struct slowLog_
{
  int cap;
  int bar;
  slowLog *esc;
  // follows opaque fields (see logImpl_ sturcture in c file)
};
#define init_slow_log(s) do {  \
  (s)->cap = 0;                \
  (s)->bar = 0;                \
  (s)->esc = NULL;             \
} while (0)

typedef struct slowLogEntry_ slowLogEntry;
struct slowLogEntry_
{
  long long id;
  long long start_ms;
  int duration;
  char sub1;
  char sub2;
};
#define init_slow_log_entry(e) do {   \
  (e)->id = 0LL;                      \
  (e)->start_ms = 0LL;                \
  (e)->duration = 0;                  \
  (e)->sub1 = '?';                    \
  (e)->sub2 = '?';                    \
} while(0)

typedef struct slowLogStat_ slowLogStat;
struct slowLogStat_
{
  int count;
  long long tot_count;
  long long sum_duration;
};
#define init_slow_log_stat(s) do {   \
  (s)->count = 0LL;                  \
  (s)->tot_count = 0LL;              \
  (s)->sum_duration = 0LL;           \
} while(0)

/* 
 * mapper function.
 * returns 1: to continue, 0: stop iteration without err,  -1: stop iteration with error 
 *
 * Note: do not call other slowlog API within slowlog_map_func. (wll cause deadlock)
 */
typedef int (*slowlog_map_func) (slowLogEntry * e, void *arg);

/* replicator (single thread semantic) use these functions */
extern slowLog *new_slowlog (int cap, int bar, slowLog * esc);
extern void delete_slowlog (slowLog * sl);
extern int slowlog_add (slowLog * sl, long long start_ms, int duration,
			char sub1, char sub2);
extern int slowlog_map (slowLog * sl, slowlog_map_func func, void *arg,
			int asc);
extern int slowlog_stat (slowLog * sl, slowLogStat * stat);
#endif
