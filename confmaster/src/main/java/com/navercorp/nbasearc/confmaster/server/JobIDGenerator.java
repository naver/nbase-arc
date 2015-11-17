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
