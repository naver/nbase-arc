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

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

class SingleThreadEventLoop {
    private final EventLoopGroup eventLoop;
    private final HashedTimingWheel timer;

    SingleThreadEventLoop() {
        this.eventLoop = new NioEventLoopGroup(1);
        this.timer = new HashedTimingWheel(500, 256);
    }

    void init() {
        eventLoop.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    beCron();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 1, 1000, TimeUnit.MILLISECONDS);
    }

    private void beCron() {
        final long currTimestamp = System.currentTimeMillis();
        timer.processLines(currTimestamp);
    }

    ListenableFuture<?> close() {
        final SettableFuture<?> sf = SettableFuture.create();

        eventLoop.execute(new Runnable() {
            @Override
            public void run() {
                eventLoop.shutdownGracefully();
                sf.set(null);
            }
        });

        return sf;
    }

    void addTimer(TimerCallback tc) {
        timer.add(tc);
    }

    void delTimer(TimerCallback tc) {
        timer.del(tc);
    }

    EventLoopGroup getEventLoopGroup() {
        return eventLoop;
    }
}
