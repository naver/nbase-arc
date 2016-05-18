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