/*
 * Copyright 2015 Naver Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.nbasearc.confmaster.server.lock;

import java.util.*;
import java.util.concurrent.locks.Lock;

public class LockHelper {
    
    private final Deque<Lock> lockList = new ArrayDeque<Lock>();

    public void acquireLock(Lock lock) {
        lock.lock();
        lockList.push(lock);
    }

    public void releaseAllLock() {
        while (!lockList.isEmpty()) {
            lockList.pop().unlock();
        }
    }
    
}
