/*
 * Copyright 2015 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.navercorp.nbasearc.gcp;

import java.util.ArrayDeque;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * @author seongjoon.ahn@navercorp.com
 *
 */
class Pipeline {

    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);

    private final Queue<Request> requests;
    private final Queue<Request> sent;
    private final SingleThreadEventLoop eventLoop;

    private final int SOCKET_BUFFER_SIZE = 65535;
    private final int INITIAL_CAPACITY = 65535 / 2;

    Pipeline(int initialCapacity, SingleThreadEventLoop eventLoop) {
        this.requests = new ArrayDeque<Request>(initialCapacity);
        this.sent = new ArrayDeque<Request>(initialCapacity);
        this.eventLoop = eventLoop;
    }

    boolean isEmpty() {
        return requests.isEmpty() && sent.isEmpty();
    }

    void clear(ErrorCode errCode) {
        clearQueue(errCode, sent);
        clearQueue(errCode, requests);
    }
    
    private void clearQueue(ErrorCode errCode, Queue<Request> q) {
        while (q.isEmpty() == false) {
            Request rqst = q.remove();
            if (rqst.getType() == Request.Type.SYSTEM) {
                continue;
            }
            eventLoop.delTimer(rqst);
            if (rqst.isTimeout() == false) {
                rqst.getVirtualConnection().onResponse(rqst, null, errCode);
            }
        }
    }

    void put(Request rqst) {
        assert rqst.getState() == Request.State.QUEUING : 
            rqst.getState() + " of request cannot be put into pipeline.";
        requests.add(rqst);
    }

    void remove(Request rqst) {
        assert rqst.getState() != Request.State.SENT :
            rqst.getState() + " of request cannot be removed from pipeline.";
        requests.remove(rqst);
    }

    Request pollSent() {
        return sent.poll();
    }
    
    Request peekFirst() {
        final Request rqst = sent.peek();
        if (rqst != null) {
            return rqst;
        }
        
        return requests.peek();
    }

    ByteBuf aggregate(ByteBufAllocator allocator) {

        if (requests.isEmpty()) {
            return null;
        }

        /* aggregate */
        ByteBuf buf = allocator.ioBuffer(INITIAL_CAPACITY);

        while (true) {
            Request rqst = requests.poll();
            if (rqst == null) {
                break;
            }

            rqst.setState(Request.State.SENT);
            buf.writeBytes(rqst.getCommand());
            sent.add(rqst);

            if (SOCKET_BUFFER_SIZE <= buf.readableBytes()) {
                break;
            }
        }

        return buf;
    }

}
/* end of class Pipeline */
