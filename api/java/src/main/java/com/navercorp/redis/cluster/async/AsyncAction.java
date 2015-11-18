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

import com.navercorp.redis.cluster.gateway.GatewayClient;

/**
 *
 * @author jaehong.kim
 */
public abstract class AsyncAction<T> {
    private final GatewayClient client;
    private final AsyncResultHandler<T> handler;

    public AsyncAction(final GatewayClient client) {
        this(client, null);
    }

    public AsyncAction(final GatewayClient client, final AsyncResultHandler<T> handler) {
        this.client = client;
        this.handler = handler;
    }

    public void run() {
        final Runnable runnable = new Runnable() {
            public void run() {
                AsyncResult<T> res = new AsyncResult<T>();
                try {
                    final T result = action();
                    res.setResult(result);
                } catch (Exception e) {
                    res.SetCause(e);
                }

                if (handler != null) {
                    try {
                        handler.handle(res);
                    } catch (Throwable ignored) {
                    }
                }
            }
        };

        client.executeBackgroundPool(runnable);
    }

    public abstract T action() throws Exception;
}