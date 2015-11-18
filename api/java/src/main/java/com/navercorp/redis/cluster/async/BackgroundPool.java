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

package com.navercorp.redis.cluster.async;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jaehong.kim
 */
public class BackgroundPool {
    private static final String THREAD_PREFIX = "triples-redis-client-background-";

    private ExecutorService pool;
    private int poolSize = 32;

    public BackgroundPool(final int poolSize) {
        this.poolSize = poolSize;
    }

    public ExecutorService getPool() {
        if (this.pool != null) {
            return this.pool;
        }
        synchronized (this) {
            if (this.pool != null) {
                return this.pool;
            }
            this.pool = Executors.newFixedThreadPool(this.poolSize, new BackgroundPoolFactory());
        }
        return this.pool;
    }

    public void shutdown() {
        if (pool != null) {
            pool.shutdownNow();
        }
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("prefix=").append(THREAD_PREFIX).append(",");
        sb.append("size=");
        if (pool != null) {
            sb.append(this.poolSize);
        } else {
            sb.append("0");
        }
        sb.append("}");
        return sb.toString();
    }

    class BackgroundPoolFactory implements ThreadFactory {
        private AtomicInteger count = new AtomicInteger(0);

        public Thread newThread(Runnable runnable) {
            final Thread t = new Thread(runnable, THREAD_PREFIX + count.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }
}