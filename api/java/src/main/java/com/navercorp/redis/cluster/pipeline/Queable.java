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

import java.util.LinkedList;
import java.util.Queue;

public class Queable {
    private Queue<Response<?>> pipelinedResponses = new LinkedList<Response<?>>();

    protected void clean() {
        pipelinedResponses.clear();
    }

    protected Response<?> generateResponse(Object data) {
        Response<?> response = pipelinedResponses.poll();
        if (response != null) {
            response.set(data);
        }
        return response;
    }

    protected <T> Response<T> getResponse(Builder<T> builder) {
        Response<T> lr = new Response<T>(builder);
        pipelinedResponses.add(lr);
        return lr;
    }

}
