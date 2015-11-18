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

package com.navercorp.redis.cluster.pipeline;

import redis.clients.jedis.exceptions.JedisDataException;

public class Response<T> {
    protected T response = null;
    private boolean built = false;
    private boolean set = false;
    private Builder<T> builder;
    private Object data;

    public Response(Builder<T> b) {
        this.builder = b;
    }

    public void set(Object data) {
        this.data = data;
        set = true;
    }

    public T get() {
        if (!set) {
            throw new JedisDataException(
                    "Please close pipeline or multi block before calling this method.");
        }
        if (!built) {
            if (data != null) {
                if (data instanceof JedisDataException) {
                    throw new JedisDataException((JedisDataException) data);
                }
                response = builder.build(data);
            }
            this.data = null;
            built = true;
        }
        return response;
    }

    public String toString() {
        return "Response " + builder.toString();
    }

}
