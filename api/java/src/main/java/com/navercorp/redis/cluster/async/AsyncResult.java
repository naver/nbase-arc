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

/**
 *
 * @author jaehong.kim
 */
public class AsyncResult<T> {
    private boolean failed;
    private boolean succeeded;
    private T result;
    private Throwable throwable;

    public T getResult() {
        return result;
    }

    public Throwable getCause() {
        return throwable;
    }

    public boolean isSucceeded() {
        return succeeded;
    }

    public boolean isFailed() {
        return failed;
    }

    public void setResult(T result) {
        this.result = result;
        this.succeeded = true;
    }

    public void SetCause(Throwable throwable) {
        this.throwable = throwable;
        this.failed = true;
    }
}
