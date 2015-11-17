#ifndef _DLIST_H_
#define _DLIST_H_

typedef struct dlisth_s dlisth;
struct dlisth_s
{
  dlisth *next;
  dlisth *prev;
};

/* dlisth_map() hook function type definition */
typedef int (*dlist_map_func) (dlisth * h, void *arg, int *cont);

extern int dlisth_map (dlisth * h, dlist_map_func func, void *arg);

#define dlisth_init(h)         \
do {                           \
  dlisth *__h = (h);           \
  (__h)->next = (__h)->prev = (__h); \
} while (0)

#define dlisth_is_empty(h) ((h)->next == (h) && (h)->prev == (h))

#define dlisth_delete(h_)              \
do {                                   \
  dlisth *__h = (h_);                  \
  (__h)->next->prev = (__h)->prev;     \
  (__h)->prev->next = (__h)->next;     \
  (__h)->next = (__h)->prev = (__h);   \
} while(0)

#define dlisth_insert_before(ih, bh)   \
do {                                   \
  dlisth *__ih = (ih);                 \
  dlisth *__bh = (bh);                 \
  (__ih)->next = (__bh);               \
  (__ih)->prev = (__bh)->prev;         \
  (__bh)->prev->next = (__ih);         \
  (__bh)->prev = (__ih);               \
} while (0)

#define dlisth_insert_after(ih, bh)    \
do {                                   \
  dlisth *__ih = (ih);                 \
  dlisth *__bh = (bh);                 \
  (__ih)->prev = (__bh);               \
  (__ih)->next = (__bh)->next;         \
  (__bh)->next->prev = (__ih);         \
  (__bh)->next = (__ih);               \
} while (0)

#endif
