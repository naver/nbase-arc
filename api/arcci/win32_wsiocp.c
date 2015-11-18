/*
 * Copyright (c), Microsoft Open Technologies, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  - Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "win32fixes.h"
#include "ae.h"
#include "adlist.h"
#include "zmalloc.h"
#include <mswsock.h>
#include <Guiddef.h>
#include "win32_wsiocp.h"

static __declspec(thread) void *iocpState;
static __declspec(thread) HANDLE iocph;

static LPFN_ACCEPTEX acceptex;
static LPFN_CONNECTEX connectex;
static LPFN_GETACCEPTEXSOCKADDRS getaddrs;

#define SUCCEEDED_WITH_IOCP(result)                        \
  ((result) || (GetLastError() == ERROR_IO_PENDING))

/* for zero length reads use shared buf */
static DWORD wsarecvflags;
static char zreadchar[1];

/* Winsock requires library initialization on startup  */
int aeInitWinSock(void) {
    const GUID wsaid_connectex = WSAID_CONNECTEX;
    const GUID wsaid_acceptex = WSAID_ACCEPTEX;
    const GUID wsaid_acceptexaddrs = WSAID_GETACCEPTEXSOCKADDRS;
    WSADATA t_wsa;
    WORD wVers;
    SOCKET sock;
    DWORD bytes;
    int iError, result;

    wVers = MAKEWORD(2, 2);
    iError = WSAStartup(wVers, &t_wsa);

    if (iError != NO_ERROR || LOBYTE(t_wsa.wVersion) != 2 || HIBYTE(t_wsa.wVersion) != 2) {
        return -1; /* not done; check WSAGetLastError() for error number */
    };

    /* temporary socket */
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        return -1;
    }

    /* initialize function pointer of ConnectEx */
    result = WSAIoctl(sock,
		SIO_GET_EXTENSION_FUNCTION_POINTER,
		(void *)&wsaid_connectex,
		sizeof(GUID),
		&connectex,
		sizeof(LPFN_CONNECTEX),
		&bytes,
		NULL,
		NULL);
    if (result == SOCKET_ERROR) {
        goto init_winsock_fail_return;
    }

    /* initialize function pointer of AcceptEx */
    result = WSAIoctl(sock,
		SIO_GET_EXTENSION_FUNCTION_POINTER,
		(void *)&wsaid_acceptex,
		sizeof(GUID),
		&acceptex,
		sizeof(LPFN_ACCEPTEX),
		&bytes,
		NULL,
		NULL);
    if (result == SOCKET_ERROR) {
        goto init_winsock_fail_return;
    }

    /* initialize function pointer of GetAcceptExSockaddrs */ 
    result = WSAIoctl(sock,
	    SIO_GET_EXTENSION_FUNCTION_POINTER,
	    (void *)&wsaid_acceptexaddrs,
	    sizeof(GUID),
	    &getaddrs,
	    sizeof(LPFN_GETACCEPTEXSOCKADDRS),
	    &bytes,
	    NULL,
	    NULL);
    if (result == SOCKET_ERROR) {
        goto init_winsock_fail_return;
    }

    aeLoadGQCSEx();
    return 0;

init_winsock_fail_return:
    closesocket(sock);

    return -1;
}

/* queue an accept with a new socket */
int aeWinQueueAccept(SOCKET listensock) {
    aeCommState *commstate;
    aeCommState *acccommstate;
    DWORD result, bytes;
    SOCKET acceptsock;
    aacceptreq * areq;

    if ((commstate = aeGetCommState(iocpState, (int)listensock)) == NULL) {
        errno = WSAEINVAL;
        return -1;
    }

    acceptsock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (acceptsock == INVALID_SOCKET) {
        errno = WSAEINVAL;
        return -1;
    }

    acccommstate = aeGetCommState(iocpState, (int)acceptsock);
    if (acccommstate == NULL) {
        errno = WSAEINVAL;
        return -1;
    }

    acccommstate->masks = COMM_ATTACHED;
    /* keep accept socket in buf len until accepted */
    areq = (aacceptreq *)zmalloc(sizeof(aacceptreq));
    memset(areq, 0, sizeof(aacceptreq));
    areq->buf = (char *)zmalloc(sizeof(struct sockaddr_storage) * 2 + 64);
    areq->accept = acceptsock;
    areq->next = NULL;

    result = acceptex(listensock, acceptsock,
                            areq->buf, 0,
                            sizeof(struct sockaddr_storage),
                            sizeof(struct sockaddr_storage),
                            &bytes, &areq->ov);
    if (SUCCEEDED_WITH_IOCP(result)){
        commstate->masks |= ACCEPT_PENDING;
    } else {
        errno = WSAGetLastError();
        commstate->masks &= ~ACCEPT_PENDING;
        closesocket(acceptsock);
        acccommstate->masks = 0;
        zfree(areq);
        return -1;
    }

    return TRUE;
}

/* listen using extension function to get faster accepts */
int aeWinListen(SOCKET sock, int backlog) {
    aeCommState *commstate;
    DWORD result, bytes;

    if ((commstate = aeGetCommState(iocpState, (int)sock)) == NULL) {
        errno = WSAEINVAL;
        return SOCKET_ERROR;
    }

    aeWinSocketAttach(sock);
    commstate->masks |= LISTEN_SOCK;

    if (listen(sock, backlog) == 0) {
        if (aeWinQueueAccept(sock) == -1) {
            return SOCKET_ERROR;
        }
    }

    return 0;
}

/* return the queued accept socket */
socket_t aeWinAccept(socket_t fd, struct sockaddr *sa, socklen_t *len) {
    aeCommState *commstate;
    SOCKET acceptsock;
    int result;
    SOCKADDR *plocalsa;
    SOCKADDR *premotesa;
    int locallen, remotelen;
    aacceptreq * areq;
    SOCKET listenSock = (SOCKET)fd;

    if ((commstate = aeGetCommState(iocpState, fd)) == NULL) {
        errno = WSAEINVAL;
        return SOCKET_ERROR;
    }


    areq = commstate->reqs;
    if (areq == NULL) {
        errno = WSAEINVAL;
        return SOCKET_ERROR;
    }

    commstate->reqs = areq->next;

    acceptsock = areq->accept;

    result = setsockopt(acceptsock,
                        SOL_SOCKET,
                        SO_UPDATE_ACCEPT_CONTEXT,
                        (char*)&listenSock,
                        sizeof(listenSock));
    if (result == SOCKET_ERROR) {
        errno = WSAGetLastError();
        return SOCKET_ERROR;
    }

    locallen = *len;
    getaddrs(areq->buf,
                    0,
                    sizeof(struct sockaddr_storage),
                    sizeof(struct sockaddr_storage),
                    &plocalsa, &locallen,
                    &premotesa, &remotelen);

    locallen = remotelen < *len ? remotelen : *len;
    memcpy(sa, premotesa, locallen);
    *len = locallen;

    aeWinSocketAttach(acceptsock);

    zfree(areq->buf);
    zfree(areq);

    /* queue another accept */
    if (aeWinQueueAccept(listenSock) == -1) {
        return SOCKET_ERROR;
    }

    return acceptsock;
}

int aeWinReceiveDone_Pipe(pipe_t fd) {
        aeCommState *commstate;
        int result;

        if ((commstate = aeGetCommState(iocpState, (socket_t)fd)) == NULL) {
                errno = WSAEINVAL;
                return -1;
        }
        if ((commstate->masks & COMM_ATTACHED) == 0) {
                return 0;
        }

        /* use zero length read with overlapped to get notification
        of when data is available */
	memset(&commstate->ov_read, 0, sizeof(OVERLAPPED));

        result = ReadFile(fd, 
                          zreadchar, 
                          0,
                          NULL,
                          &commstate->ov_read);
        if (SUCCEEDED_WITH_IOCP(result == TRUE)){
                commstate->masks |= READ_QUEUED;
        }
        else {
                errno = WSAGetLastError();
		int e = WSAGetLastError();
                commstate->masks &= ~READ_QUEUED;
                return -1;
        }
        return 0;
}

/* after doing read caller needs to call done
 * so that we can continue to check for read events.
 * This is not necessary if caller will delete read events */
int aeWinReceiveDone(socket_t fd) {
    aeCommState *commstate;
    int result;
    WSABUF zreadbuf;

    if ((commstate = aeGetCommState(iocpState, fd)) == NULL) {
        errno = WSAEINVAL;
        return -1;
    }
    if ((commstate->masks & COMM_ATTACHED) == 0) {
        return 0;
    }

    /* use zero length read with overlapped to get notification
     of when data is available */
    memset(&commstate->ov_read, 0, sizeof(OVERLAPPED));

    zreadbuf.buf = zreadchar;
    zreadbuf.len = 0;
    result = WSARecv((SOCKET)fd,
                     &zreadbuf,
                     1,
                     NULL,
                     &wsarecvflags,
                     &commstate->ov_read,
                     NULL);
    if (SUCCEEDED_WITH_IOCP(result == 0)){
        commstate->masks |= READ_QUEUED;
    } else {
        errno = WSAGetLastError();
        commstate->masks &= ~READ_QUEUED;
        return -1;
    }
    return 0;
}

/* wrapper for send
* enables use of WSA Send to get IOCP notification of completion.
* returns -1  with errno = WSA_IO_PENDING if callback will be invoked later */
int aeWinSocketSend(socket_t fd, char *buf, int len, int flags,
	void *eventLoop, void *data, void *proc) {
	aeCommState *commstate;
	int result;
	asendreq *areq;

	commstate = aeGetCommState(iocpState, fd);

	if (commstate != NULL &&
		(commstate->masks & CONNECT_PENDING)) {
		aeWait(fd, AE_WRITABLE, 50);
	}

	/* use overlapped structure to send using IOCP */
	areq = (asendreq *)zmalloc(sizeof(asendreq));
	memset(areq, 0, sizeof(asendreq));
	areq->wbuf.len = len;
	areq->wbuf.buf = buf;
	areq->eventLoop = (aeEventLoop *)eventLoop;
	areq->req.data = data;
	areq->req.len = len;
	areq->req.buf = buf;
	areq->proc = (aeFileProc *)proc;

	result = WSASend((SOCKET)fd,
		&areq->wbuf,
		1,
		NULL,
		flags,
		&areq->ov,
		NULL);

	if (SUCCEEDED_WITH_IOCP(result == 0)){
		errno = WSA_IO_PENDING;
		commstate->wreqs++;
		listAddNodeTail(&commstate->wreqlist, areq);
	}
	else {
		errno = WSAGetLastError();
		zfree(areq);
	}
	return SOCKET_ERROR;
}

/* for non-blocking connect with IOCP */
int aeWinSocketConnect(socket_t fd, const struct sockaddr *sa, int len) {
    DWORD result, bytes;
    SOCKET sock = (SOCKET)fd;
    aeCommState *commstate;
    struct sockaddr_in addr;

    if ((commstate = aeGetCommState(iocpState, fd)) == NULL) {
        errno = WSAEINVAL;
        return SOCKET_ERROR;
    }

    if (aeWinSocketAttach(fd) != 0) {
        return SOCKET_ERROR;
    }

    memset(&commstate->ov_read, 0, sizeof(OVERLAPPED));
    /* need to bind sock before connectex */
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = 0;
    result = bind(sock, (struct sockaddr *)&addr, sizeof(addr));

    result = connectex(sock, sa, len, NULL, 0, NULL, &commstate->ov_read);
    if (result != TRUE) {
        result = WSAGetLastError();
        if (result == ERROR_IO_PENDING) {
            errno = WSA_IO_PENDING;
            commstate->masks |= CONNECT_PENDING;
        } else {
            errno = result;
            return SOCKET_ERROR;
        }
    }
    return 0;
}

/* for each asynch pipe, need to associate completion port */
int aeWinPipeAttach(HANDLE fd) {
        aeCommState *commstate;

	if ((commstate = aeGetCommState(iocpState, (socket_t)fd)) == NULL) {
                errno = WSAEINVAL;
                return -1;
        }
        commstate->is_pipe = 1;

	memset(&commstate->ov_read, 0, sizeof(OVERLAPPED));
        /* need to bind sock before connectex */

        /* Associate it with the I/O completion port. */
        /* Use socket as completion key. */
        if (CreateIoCompletionPort(fd,
				iocph,
				(ULONG_PTR)fd,
				0) == NULL) {
				errno = WSAGetLastError();
				return -1;
        }

        commstate->masks = COMM_ATTACHED;
        commstate->wreqs = 0;

        return 0;
}

/* for each asynch socket, need to associate completion port */
int aeWinSocketAttach(socket_t fd) {
    DWORD yes = 1;
    aeCommState *commstate;

    if ((commstate = aeGetCommState(iocpState, fd)) == NULL) {
        errno = WSAEINVAL;
        return -1;
    }
    commstate->is_pipe = 0;

    /* Set the socket to nonblocking mode */
    if (ioctlsocket((SOCKET)fd, FIONBIO, &yes) == SOCKET_ERROR) {
        errno = WSAGetLastError();
        return -1;
    }

    /* Make the socket non-inheritable */
    if (!SetHandleInformation((HANDLE)fd, HANDLE_FLAG_INHERIT, 0)) {
        errno = WSAGetLastError();
        return -1;
    }

    /* Associate it with the I/O completion port. */
    /* Use socket as completion key. */
    if (CreateIoCompletionPort((HANDLE)fd,
                                iocph,
                                (ULONG_PTR)fd,
                                0) == NULL) {
        errno = WSAGetLastError();
        return -1;
    }
    commstate->masks = COMM_ATTACHED;
    commstate->wreqs = 0;

    return 0;
}

void aeShutdown(socket_t fd) {
    char rbuf[100];
    struct timeval timenow;
    long long waitmsecs = 50;      /* wait up to 50 millisecs */
    long long endms;
    long long nowms;

    /* wait for last item to complete up to tosecs seconds*/
    gettimeofday(&timenow, NULL);
    endms = ((long long)timenow.tv_sec * 1000) +
                    ((long long)timenow.tv_usec / 1000) + waitmsecs;

    if (shutdown(fd, SD_SEND) != SOCKET_ERROR) {
        /* read data until no more or error to ensure shutdown completed */
        while (1) {
            int rc = recv(fd, rbuf, 100, 0);
            if (rc == 0 || rc == SOCKET_ERROR)
                break;
            else {
                gettimeofday(&timenow, NULL);
                nowms = ((long long)timenow.tv_sec * 1000) +
                            ((long long)timenow.tv_usec / 1000);
                if (nowms > endms)
                    break;
            }
        }
    }
}

/* when closing comm, need to unassociate completion port */
int aeWinCloseComm(socket_t fd) {
	aeCommState *commstate;

	if ((commstate = aeGetCommState(iocpState, fd)) == NULL) {
        if (commstate->is_pipe) {
            CloseHandle(commstate->fd);
        }
        else {
            closesocket((SOCKET)fd);
        }
        return 0;
	}

	aeShutdown(fd);
	commstate->masks &= ~(COMM_ATTACHED | AE_WRITABLE | AE_READABLE);

	if (commstate->wreqs == 0 &&
		(commstate->masks & (CONNECT_PENDING | COMM_ATTACHED)) == 0) {
        if (commstate->is_pipe) {
            CloseHandle(commstate->fd);
        }
        else {
            closesocket((SOCKET)commstate->fd);
        }
	}
	else {
		commstate->masks |= CLOSE_PENDING;
	}
	aeDelCommState(iocpState, commstate);

	return 0;
}

void aeWinInit(void *state, HANDLE iocp, fnGetCommState *getCommState,
                                        fnDelCommState *delCommState) {
    iocpState = state;
    iocph = iocp;
}

void aeWinCleanup() {
    iocpState = NULL;
}

HANDLE aeWinGetIOCP() {
        return iocph;
}

void aeWinSetIOCP(HANDLE h) {
	iocph = h;
}

void *aeWinGetIOCPState() {
        return iocpState;
}

void aeWinSetIOCPState(void *iocp_state) {
	iocpState = iocp_state;
}
