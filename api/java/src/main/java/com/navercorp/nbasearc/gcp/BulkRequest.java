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

import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BulkRequest implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(BulkRequest.class);

    private final Queue<Request> requests;
    private final PhysicalConnection physicalConnection;

    BulkRequest(Queue<Request> requests, PhysicalConnection physicalConnection) {
        this.requests = requests;
        this.physicalConnection = physicalConnection;
    }

    @Override
    public void run() {
        while (requests.isEmpty() == false) {
            physicalConnection.request(requests.poll());
        }
    }

}
