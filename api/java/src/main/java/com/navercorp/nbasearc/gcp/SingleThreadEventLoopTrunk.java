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

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

class SingleThreadEventLoopTrunk {
    private final SingleThreadEventLoop[] eventLoops;
    private final AtomicInteger roundrobin = new AtomicInteger();

    SingleThreadEventLoopTrunk(int threadPoolSize) {
        eventLoops = new SingleThreadEventLoop[threadPoolSize];
        for (int i = 0; i < threadPoolSize; i++) {
            eventLoops[i] = new SingleThreadEventLoop();
            eventLoops[i].init();
        }
    }

    ListenableFuture<?> close() {
        final SettableFuture<?> sf = SettableFuture.create();
        final AtomicInteger counter = new AtomicInteger(eventLoops.length);

        for (final SingleThreadEventLoop eventLoop : eventLoops) {
            eventLoop.close().addListener(new Runnable() {
                @Override
                public void run() {
                    if (counter.decrementAndGet() == 0) {
                        sf.set(null);
                    }
                }
            }, MoreExecutors.directExecutor());
        }

        return sf;
    }

    SingleThreadEventLoop roundrobinEventLoop() {
        return eventLoops[Math.abs(roundrobin.incrementAndGet()) % eventLoops.length];
    }
}
