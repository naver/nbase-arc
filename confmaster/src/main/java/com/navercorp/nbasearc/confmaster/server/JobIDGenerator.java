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

package com.navercorp.nbasearc.confmaster.server;


public class JobIDGenerator {
    
    private static class SingletonHolder {
        public static final JobIDGenerator INSTANCE = new JobIDGenerator();
    }

    public static JobIDGenerator getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private long seed;

    /*
     * Unique ID generation rule is as follows
     *   + 1 bit : sign bit
     *   + 43 bit : time in milliseconds (since 1375433132512L)
     *   + 20 bit : sequence number to avoid collision
     */
    public void initialize() {
        seed = System.currentTimeMillis() - 1375433132512L;
        seed = seed << 20;
    }
    
    public synchronized long getID() {
        return seed ++;
    }

}
