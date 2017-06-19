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
package com.navercorp.redis.cluster.gateway;

/**
 * The Class GatewayException.
 *
 * @author jaehong.kim
 */
public class GatewayException extends RuntimeException {

    /**
     * The Constant serialVersionUID.
     */
    private static final long serialVersionUID = -8920726312846580133L;

    private boolean retryable = false;

    /**
     * Instantiates a new gateway exception.
     *
     * @param message the message
     */
    public GatewayException(String message) {
        super(message);
    }

    /**
     * Instantiates a new gateway exception.
     *
     * @param message the message
     * @param ex      the ex
     */
    public GatewayException(String message, Throwable ex) {
        super(message, ex);
    }

    public GatewayException(String message, Throwable ex, boolean retryable) {
        super(message, ex);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}