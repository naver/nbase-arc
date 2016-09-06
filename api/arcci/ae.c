/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdio.h>
#include <sys/types.h>
#ifndef _WIN32
#include <sys/time.h>
#include <unistd.h>
#include <poll.h>
#endif
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "ae.h"
#include "zmalloc.h"

/* Include the best multiplexing layer supported by this system.
 * The following should be ordered by performances, descending. */
#ifdef _WIN32
#include "ae_wsiocp.c"
#else
#include "ae_epoll.c"
#endif

#ifdef _WIN32
static unsigned int dictFdHash(const void *key)
{
    return (unsigned int)key;
}

static unsigned int dictFdHashKeyCompare(void *privdata, const void *key1, const void *key2)
{
    return key1 == key2;
}

static dictType fdiDictType = {
    dictFdHash,
    NULL,
    NULL,
    dictFdHashKeyCompare,
    NULL,
    NULL,
};

static fdi_t getNextFDIAvailable(aeEventLoop *eventLoop)
{
    listIter *rp_ptr;
    listNode *fdi_node;
    fdi_t fdi;

    if (listLength(eventLoop->fdiMap->recycle_pool) > 0)
    {
        rp_ptr = listGetIterator(eventLoop->fdiMap->recycle_pool, AL_START_HEAD);
        fdi_node = listNext(rp_ptr);
        fdi = (fdi_t)fdi_node->value;
        listDelNode(eventLoop->fdiMap->recycle_pool, fdi_node);
        listReleaseIterator(rp_ptr);
        return fdi;
    }
    return eventLoop->fdiMap->next_available++;
}

static fdi_t lookupFDI(aeEventLoop *eventLoop, socket_t s)
{
    dictEntry *entry = dictFind(eventLoop->fdiMap->map, (void *)s);
    if (entry == NULL)
    {
        return INVALID_FDI;
    }
    fdi_t fdi = (fdi_t)entry->v.u64;
    return fdi;
}

static fdi_t mapFDToIndex(aeEventLoop *eventLoop, socket_t s)
{
    fdi_t fdi = getNextFDIAvailable(eventLoop);
    if (dictAdd(eventLoop->fdiMap->map, (void *)s, (void *)fdi) != DICT_OK)
    {
        return INVALID_FDI;
    }
    return fdi;
}

static void unmapFDToIndex(aeEventLoop *eventLoop, socket_t s)
{
    fdi_t fdi = lookupFDI(eventLoop, s);
    if (fdi == INVALID_FDI)
    {
        return;
    }
    dictDelete(eventLoop->fdiMap->map, (void *)s);
    listAddNodeTail(eventLoop->fdiMap->recycle_pool, (void *)fdi);
}
#endif

aeEventLoop *aeCreateEventLoop(int setsize) {
    aeEventLoop *eventLoop;
    int i;

    if ((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL) goto err;
    eventLoop->events = zmalloc(sizeof(aeFileEvent)*setsize);
    eventLoop->fired = zmalloc(sizeof(aeFiredEvent)*setsize);
    if (eventLoop->events == NULL || eventLoop->fired == NULL) goto err;
    eventLoop->setsize = setsize;
    eventLoop->lastTime = time(NULL);
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;
    eventLoop->stop = 0;
    eventLoop->maxfd = EVENTLOOP_MAXFD_INITIAL_VALUE;
    eventLoop->beforesleep = NULL;
    if (aeApiCreate(eventLoop) == -1) goto err;
    /* Events with mask == AE_NONE are not set. So let's initialize the
     * vector with it. */
    for (i = 0; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
#ifdef _WIN32
    if ((eventLoop->fdiMap = zmalloc(sizeof(fd_index_map_t))) == NULL) goto err;
    eventLoop->fdiMap->map = dictCreate(&fdiDictType, NULL);
    eventLoop->fdiMap->recycle_pool = listCreate();
    if (eventLoop->fdiMap->map == NULL || eventLoop->fdiMap->recycle_pool == NULL) goto err;
    eventLoop->fdiMap->next_available = 3;	// stdin(0), stdout(1) and stderr(2)
#endif
    return eventLoop;

err:
#ifdef _WIN32
    if (eventLoop->fdiMap) {
        if (eventLoop->fdiMap->map != NULL) dictRelease(eventLoop->fdiMap->map);
        if (eventLoop->fdiMap->recycle_pool != NULL) listRelease(eventLoop->fdiMap->recycle_pool);
        zfree(eventLoop->fdiMap);
    }
#endif
    if (eventLoop) {
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop);
    }
    return NULL;
}

void aeDeleteEventLoop(aeEventLoop *eventLoop) {
    aeApiFree(eventLoop);
#ifdef _WIN32
    dictRelease(eventLoop->fdiMap->map);
    listRelease(eventLoop->fdiMap->recycle_pool);
    zfree(eventLoop->fdiMap);
#endif
    zfree(eventLoop->events);
    zfree(eventLoop->fired);
    zfree(eventLoop);
}

void aeStop(aeEventLoop *eventLoop) {
    eventLoop->stop = 1;
}

int aeCreateFileEvent(aeEventLoop *eventLoop, socket_t fd, int mask,
        aeFileProc *proc, void *clientData)
{
    aeFileEvent *fe;
    int fdi;
    
#ifdef _WIN32
    fdi = lookupFDI(eventLoop, fd);
    if (fdi == INVALID_FDI) {
        fdi = mapFDToIndex(eventLoop, fd);
        if (fdi == INVALID_FDI) {
            errno = EBADF;
            return AE_ERR;
        }
    }
#else
    fdi = fd;
#endif

    if (fdi >= eventLoop->setsize) {
        errno = ERANGE;
        return AE_ERR;
    }
    fe = &eventLoop->events[fdi];

    if (aeApiAddEvent(eventLoop, fd, mask) == -1)
        return AE_ERR;
    fe->mask |= mask;
    if (mask & AE_READABLE) fe->rfileProc = proc;
    if (mask & AE_WRITABLE) fe->wfileProc = proc;
    fe->clientData = clientData;
    if (fdi > eventLoop->maxfd)
        eventLoop->maxfd = fdi;
    return AE_OK;
}

void aeDeleteFileEvent(aeEventLoop *eventLoop, socket_t fd, int mask)
{
    aeFileEvent *fe;
    int fdi;

#ifdef _WIN32
    fdi = lookupFDI(eventLoop, fd);
    if (fdi == INVALID_FDI) {
        errno = EBADF;
        return;
    }
#else
    fdi = fd;
#endif
    
    if (fdi >= eventLoop->setsize) {
#ifdef _WIN32
        unmapFDToIndex(eventLoop, fd);
#endif
        return;
    }
    fe = &eventLoop->events[fdi];

    if (fe->mask == AE_NONE) return;
    fe->mask = fe->mask & (~mask);
    if (fdi == eventLoop->maxfd && fe->mask == AE_NONE) {
        /* Update the max fd */
        socket_t j;

        for (j = eventLoop->maxfd-1; j >= 0; j--)
            if (eventLoop->events[j].mask != AE_NONE) break;
        eventLoop->maxfd = j;
    }
#ifdef _WIN32
    if (fe->mask == AE_NONE) {
        unmapFDToIndex(eventLoop, fd);
    }
#endif
    aeApiDelEvent(eventLoop, fd, mask);
}

int aeGetFileEvents(aeEventLoop *eventLoop, socket_t fd) {
    aeFileEvent *fe;
    int fdi;

#ifdef _WIN32
    fdi = lookupFDI(eventLoop, fd);
    if (fdi == INVALID_FDI) {
        return 0;
    }
#else
    fdi = fd;
#endif

    if (fdi >= eventLoop->setsize) return 0;
    fe = &eventLoop->events[fdi];

    return fe->mask;
}

static void aeGetTime(long *seconds, long *milliseconds)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    *seconds = tv.tv_sec;
    *milliseconds = tv.tv_usec/1000;
}

static void aeAddMillisecondsToNow(long long milliseconds, long *sec, long *ms) {
    long cur_sec, cur_ms, when_sec, when_ms;

    aeGetTime(&cur_sec, &cur_ms);
    when_sec = (long)(cur_sec + milliseconds/1000);
    when_ms = cur_ms + milliseconds%1000;
    if (when_ms >= 1000) {
        when_sec ++;
        when_ms -= 1000;
    }
    *sec = when_sec;
    *ms = when_ms;
}

long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc)
{
    long long id = eventLoop->timeEventNextId++;
    aeTimeEvent *te;

    te = zmalloc(sizeof(*te));
    if (te == NULL) return AE_ERR;
    te->id = id;
    aeAddMillisecondsToNow(milliseconds,&te->when_sec,&te->when_ms);
    te->timeProc = proc;
    te->finalizerProc = finalizerProc;
    te->clientData = clientData;
    te->next = eventLoop->timeEventHead;
    eventLoop->timeEventHead = te;
    return id;
}

int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id)
{
    aeTimeEvent *te, *prev = NULL;

    te = eventLoop->timeEventHead;
    while(te) {
        if (te->id == id) {
            if (prev == NULL)
                eventLoop->timeEventHead = te->next;
            else
                prev->next = te->next;
            if (te->finalizerProc)
                te->finalizerProc(eventLoop, te->clientData);
            zfree(te);
            return AE_OK;
        }
        prev = te;
        te = te->next;
    }
    return AE_ERR; /* NO event with the specified ID found */
}

/* Search the first timer to fire.
 * This operation is useful to know how many time the select can be
 * put in sleep without to delay any event.
 * If there are no timers NULL is returned.
 *
 * Note that's O(N) since time events are unsorted.
 * Possible optimizations (not needed by Redis so far, but...):
 * 1) Insert the event in order, so that the nearest is just the head.
 *    Much better but still insertion or deletion of timers is O(N).
 * 2) Use a skiplist to have this operation as O(1) and insertion as O(log(N)).
 */
static aeTimeEvent *aeSearchNearestTimer(aeEventLoop *eventLoop)
{
    aeTimeEvent *te = eventLoop->timeEventHead;
    aeTimeEvent *nearest = NULL;

    while(te) {
        if (!nearest || te->when_sec < nearest->when_sec ||
                (te->when_sec == nearest->when_sec &&
                 te->when_ms < nearest->when_ms))
            nearest = te;
        te = te->next;
    }
    return nearest;
}

/* Process time events */
static int processTimeEvents(aeEventLoop *eventLoop) {
    int processed = 0;
    aeTimeEvent *te;
    long long maxId;
    time_t now = time(NULL);

    /* If the system clock is moved to the future, and then set back to the
     * right value, time events may be delayed in a random way. Often this
     * means that scheduled operations will not be performed soon enough.
     *
     * Here we try to detect system clock skews, and force all the time
     * events to be processed ASAP when this happens: the idea is that
     * processing events earlier is less dangerous than delaying them
     * indefinitely, and practice suggests it is. */
    if (now < eventLoop->lastTime) {
        te = eventLoop->timeEventHead;
        while(te) {
            te->when_sec = 0;
            te = te->next;
        }
    }
    eventLoop->lastTime = now;

    te = eventLoop->timeEventHead;
    maxId = eventLoop->timeEventNextId-1;
    while(te) {
        long now_sec, now_ms;
        long long id;

        if (te->id > maxId) {
            te = te->next;
            continue;
        }
        aeGetTime(&now_sec, &now_ms);
        if (now_sec > te->when_sec ||
            (now_sec == te->when_sec && now_ms >= te->when_ms))
        {
#ifdef _WIN32
            long long retval;
#else
            int retval;
#endif

            id = te->id;
            retval = te->timeProc(eventLoop, id, te->clientData);
            processed++;
            /* After an event is processed our time event list may
             * no longer be the same, so we restart from head.
             * Still we make sure to don't process events registered
             * by event handlers itself in order to don't loop forever.
             * To do so we saved the max ID we want to handle.
             *
             * FUTURE OPTIMIZATIONS:
             * Note that this is NOT great algorithmically. Redis uses
             * a single time event so it's not a problem but the right
             * way to do this is to add the new elements on head, and
             * to flag deleted elements in a special way for later
             * deletion (putting references to the nodes to delete into
             * another linked list). */
            if (retval != AE_NOMORE) {
                aeAddMillisecondsToNow(retval,&te->when_sec,&te->when_ms);
            } else {
                aeDeleteTimeEvent(eventLoop, id);
            }
            te = eventLoop->timeEventHead;
        } else {
            te = te->next;
        }
    }
    return processed;
}

/* Process every pending time event, then every pending file event
 * (that may be registered by time event callbacks just processed).
 * Without special flags the function sleeps until some file event
 * fires, or when the next time event occurrs (if any).
 *
 * If flags is 0, the function does nothing and returns.
 * if flags has AE_ALL_EVENTS set, all the kind of events are processed.
 * if flags has AE_FILE_EVENTS set, file events are processed.
 * if flags has AE_TIME_EVENTS set, time events are processed.
 * if flags has AE_DONT_WAIT set the function returns ASAP until all
 * the events that's possible to process without to wait are processed.
 *
 * The function returns the number of events processed. */
int aeProcessEvents(aeEventLoop *eventLoop, int flags)
{
    int processed = 0, numevents;

    /* Nothing to do? return ASAP */
    if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

    /* Note that we want call select() even if there are no
     * file events to process as long as we want to process time
     * events, in order to sleep until the next time event is ready
     * to fire. */
    if (eventLoop->maxfd != -1 ||
        ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))) {
        int j;
        aeTimeEvent *shortest = NULL;
        struct timeval tv, *tvp;

        if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
            shortest = aeSearchNearestTimer(eventLoop);
        if (shortest) {
            long now_sec, now_ms;

            /* Calculate the time missing for the nearest
             * timer to fire. */
            aeGetTime(&now_sec, &now_ms);
            tvp = &tv;
            tvp->tv_sec = shortest->when_sec - now_sec;
            if (shortest->when_ms < now_ms) {
                tvp->tv_usec = ((shortest->when_ms+1000) - now_ms)*1000;
                tvp->tv_sec --;
            } else {
                tvp->tv_usec = (shortest->when_ms - now_ms)*1000;
            }
            if (tvp->tv_sec < 0) tvp->tv_sec = 0;
            if (tvp->tv_usec < 0) tvp->tv_usec = 0;
        } else {
            /* If we have to check for events but need to return
             * ASAP because of AE_DONT_WAIT we need to se the timeout
             * to zero */
            if (flags & AE_DONT_WAIT) {
                tv.tv_sec = tv.tv_usec = 0;
                tvp = &tv;
            } else {
                /* Otherwise we can block */
                tvp = NULL; /* wait forever */
            }
        }

        numevents = aeApiPoll(eventLoop, tvp);
        for (j = 0; j < numevents; j++) {
#ifdef _WIN32
            const int fdi = lookupFDI(eventLoop, eventLoop->fired[j].fd);
            if (fdi == INVALID_FDI) {
                continue;
            }
            aeFileEvent *fe = &eventLoop->events[fdi];
#else
            aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];
#endif
            int mask = eventLoop->fired[j].mask;
	    socket_t fd = eventLoop->fired[j].fd;
            int rfired = 0;

	    /* note the fe->mask & mask & ... code: maybe an already processed
             * event removed an element that fired and we still didn't
             * processed, so we check if the event is still valid. */
            if (fe->mask & mask & AE_READABLE) {
                rfired = 1;
                fe->rfileProc(eventLoop,fd,fe->clientData,mask);
            }
            if (fe->mask & mask & AE_WRITABLE) {
                if (!rfired || fe->wfileProc != fe->rfileProc)
                    fe->wfileProc(eventLoop,fd,fe->clientData,mask);
            }
            processed++;
        }
    }
    /* Check time events */
    if (flags & AE_TIME_EVENTS)
        processed += processTimeEvents(eventLoop);

    return processed; /* return the number of processed file/time events */
}

/* Wait for millseconds until the given file descriptor becomes
 * writable/readable/exception */
#ifdef _WIN32
int aeWait(socket_t fd, int mask, long long milliseconds) {
    struct timeval tv;
    fd_set rfds, wfds, efds;
    int retmask = 0, retval;

    tv.tv_sec = (long)(milliseconds/1000);
    tv.tv_usec = (milliseconds%1000)*1000;
    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_ZERO(&efds);

    if (mask & AE_READABLE) FD_SET(fd,&rfds);
    if (mask & AE_WRITABLE) FD_SET(fd,&wfds);
    if ((retval = select((int)fd+1, &rfds, &wfds, &efds, &tv)) > 0) {
        if (FD_ISSET(fd,&rfds)) retmask |= AE_READABLE;
        if (FD_ISSET(fd,&wfds)) retmask |= AE_WRITABLE;
        return retmask;
    } else {
        return retval;
    }
}
#else
int aeWait(socket_t fd, int mask, long long milliseconds) {
    struct pollfd pfd;
    int retmask = 0, retval;

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    if (mask & AE_READABLE) pfd.events |= POLLIN;
    if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

    if ((retval = poll(&pfd, 1, milliseconds))== 1) {
        if (pfd.revents & POLLIN) retmask |= AE_READABLE;
        if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
	if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
        return retmask;
    } else {
        return retval;
    }
}
#endif

void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;
    while (!eventLoop->stop) {
        if (eventLoop->beforesleep != NULL)
            eventLoop->beforesleep(eventLoop);
        aeProcessEvents(eventLoop, AE_ALL_EVENTS);
    }
}

char *aeGetApiName(void) {
    return aeApiName();
}

void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep) {
    eventLoop->beforesleep = beforesleep;
}
