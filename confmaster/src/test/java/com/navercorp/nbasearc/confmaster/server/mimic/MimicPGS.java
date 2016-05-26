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

package com.navercorp.nbasearc.confmaster.server.mimic;

import java.io.IOException;

public class MimicPGS {
    public MimicSMR mSmr;
    public MimicRedis mRedis;

    public MimicRemoteServer sSmr, sRedis;
    public Thread tSmr, tRedis;
    
    public MimicPGS(int smrPort, int redisPort)
            throws IOException, InterruptedException {
        mSmr = new MimicSMR();
        mSmr.init();
        
        mRedis = new MimicRedis();
        mRedis.init();
        
        sSmr = new MimicRemoteServer(smrPort, mSmr);
        tSmr = new Thread(sSmr);
        
        sRedis = new MimicRemoteServer(redisPort, mRedis);
        tRedis = new Thread(sRedis);
    }
    
    public void start() {
        tSmr.start();
        tRedis.start();
    }
    
    public void stop() throws InterruptedException, IOException {
        sSmr.cancel();
        sRedis.cancel();
        tSmr.join();
        tRedis.join();
        sSmr.release();
        sRedis.release();
    }
}