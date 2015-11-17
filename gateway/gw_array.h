#ifndef _GW_ARRAY_H_
#define _GW_ARRAY_H_

#include "zmalloc.h"

#define ARRAY_HEAD(name, type, count)                                           \
struct name {                                                                   \
    int nelem;                                                                  \
    int nfixed;                                                                 \
    int nextend;                                                                \
    type fixed[(count)];                                                        \
    type *extend;                                                               \
}

#define ARRAY_INIT(head) do {                                                   \
    (head)->nelem = 0;                                                          \
    (head)->nfixed = sizeof((head)->fixed)/sizeof((head)->fixed[0]);            \
    (head)->nextend = 0;                                                        \
    (head)->extend = NULL;                                                      \
} while (/*CONSTCOND*/0)

#define ARRAY_FINALIZE(head) do {                                               \
    if ((head)->extend) {                                                       \
        zfree((head)->extend);                                                  \
    }                                                                           \
    (head)->extend = NULL;                                                      \
    (head)->nelem = 0;                                                          \
    (head)->nextend = 0;                                                        \
} while (/*CONSTCOND*/0)

#define ARRAY_PUSH(head, data) do {                                             \
    if ((head)->nelem < (head)->nfixed) {                                       \
        (head)->fixed[(head)->nelem++] = data;                                  \
    } else if ((head)->nelem - (head)->nfixed < (head)->nextend) {              \
        (head)->extend[(head)->nelem - (head)->nfixed] = data;                  \
        (head)->nelem++;                                                        \
    } else {                                                                    \
        (head)->extend =                                                        \
            zrealloc((head)->extend, sizeof((head)->fixed[0])*(head)->nelem*2); \
        (head)->nextend = (head)->nelem*2;                                      \
        (head)->extend[(head)->nelem - (head)->nfixed] = data;                  \
        (head)->nelem++;                                                        \
    }                                                                           \
} while (/*CONSTCOND*/0)

#define ARRAY_GET(head, idx)                                                    \
    (((head)->nfixed > (idx))                                                   \
        ? ((head)->fixed[(idx)])                                                \
        : ((head)->extend[(idx) - (head)->nfixed]))

#define ARRAY_GET_PTR(head, idx)                                                \
    (((head)->nfixed > (idx))                                                   \
        ? (&(head)->fixed[(idx)])                                               \
        : (&(head)->extend[(idx) - (head)->nfixed]))

#define ARRAY_SET(head, idx, value) do {                                        \
    if ((head)->nfixed > (idx)) {                                               \
        (head)->fixed[(idx)] = value;                                           \
    } else {                                                                    \
        (head)->extend[(idx) - (head)->nfixed] = value;                         \
    }                                                                           \
} while (/*CONSTCOND*/0)

#define ARRAY_CLEAR(head)       ((head)->nelem = 0)
#define ARRAY_N(head)           ((head)->nelem)

#define ARRAY_DEL(head, elem, equal) do {                                       \
    int i, found = 0;                                                           \
    for (i = 0; i < ARRAY_N((head)); i++) {                                     \
        if ((equal)(ARRAY_GET((head), i), (elem))) {                            \
            found = 1;                                                          \
            break;                                                              \
        }                                                                       \
    }                                                                           \
    if (!found) break;                                                          \
    for (i = i + 1; i < ARRAY_N((head)); i++) {                                 \
        *(ARRAY_GET_PTR((head), i-1)) = ARRAY_GET((head), i);                   \
    }                                                                           \
    if ((head)->nelem > 0) {                                                    \
        (head)->nelem--;                                                        \
    }                                                                           \
} while (/*CONSTCOND*/0)
#endif
