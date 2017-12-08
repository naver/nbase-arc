package com.navercorp.nbasearc.gcp;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;


import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.nbasearc.gcp.ErrorCode;
import com.navercorp.nbasearc.gcp.GatewayConnectionPool;
import com.navercorp.nbasearc.gcp.RequestCallback;
import com.navercorp.nbasearc.gcp.VirtualConnection;
import com.navercorp.redis.cluster.gateway.AffinityState;

public class PerformanceTest {

    private static final Logger log = LoggerFactory.getLogger(VirtualConnection.class);
    
    GatewayConnectionPool gcp;

    @Before
    public void before() {
        gcp = new GatewayConnectionPool(2, true);
        gwAdd(1, "127.0.0.1", 6000, 2);
        gwAdd(2, "127.0.0.1", 6010, 2);
        gwAdd(3, "127.0.0.1", 6020, 2);
    }

    @After
    public void after() throws InterruptedException, ExecutionException {
        gcp.close();
    }

    private void gwAdd(final Integer id, final String ip, final int port, final int concnt) {
        try {
            gcp.addGw(id, ip, port, concnt, 1000).get(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
    
    final Map<ErrorCode, AtomicInteger> ops = new ConcurrentHashMap<ErrorCode, AtomicInteger>();
    
    class MRC implements RequestCallback {
        AtomicInteger expectedNo;
        int no;
        public MRC(AtomicInteger expectedNo, int no) {
            this.expectedNo = expectedNo;
            this.no = no;
        }
        @Override
        public void onResponse(byte[] response, ErrorCode err) {
            /*
            if (expectedNo.compareAndSet(no, no+1) == false) {
                if (err != ErrorCode.TIMEOUT) {
                    System.out.println("concurrent callback"); 
                }
            }
            
            if (err != ErrorCode.OK && err != ErrorCode.TIMEOUT) {
                //System.out.print("E");
            }
            if (err == ErrorCode.TIMEOUT) {
                //System.out.println("abc");
            }
            //*/
            ops.get(err).incrementAndGet();
        }
    };
    
    @Ignore
    @Test
    public void performance() throws InterruptedException {
        for (ErrorCode ec : ErrorCode.values()) {
            ops.put(ec, new AtomicInteger(0));
        }
        
        final int MAX_THREAD = 8;
        final ExecutorService executor = Executors.newFixedThreadPool(MAX_THREAD + 2, new ThreadFactory() {
            AtomicInteger no = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "TestThread-" + no.getAndIncrement());
            }
        });
        
        for (int t = 0; t < MAX_THREAD; t++) {
            //final byte[] CMD = String.format("*3\r\n$3\r\nset\r\n$6\r\nhaha%02d\r\n$4\r\nhoho\r\n", t).getBytes();
            final byte[] CMD = String.format("*2\r\n$3\r\nget\r\n$6\r\nhaha%02d\r\n", t).getBytes();

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            AtomicInteger expectedNo = new AtomicInteger(0); 
                            int callbackNo = 0;

                            final VirtualConnection vc = gcp.newVc(512);
                            vc.allocPc(0, AffinityState.READ, true);
                            final int MAX_USE = 100000;
                            final CountDownLatch latch = new CountDownLatch(MAX_USE);
                            for (int use = 0; use < MAX_USE; use++) {
                                vc.request(CMD, 1000, new MRC(expectedNo, callbackNo++));
                                // vc.request(CMD, 5000, callabck);
                            }
                            vc.close().get();
                        } catch (Exception e) {
                            e.printStackTrace();
                            fail(e.getMessage() + "\n" + e.getStackTrace());
                        }
                    }
                }
            });
        }
        
        // Print stat
        executor.execute(new Runnable() {
            @Override
            public void run() {
                StringBuilder sb = new StringBuilder();
                while (true) {
                    sb.setLength(0);
                    for (Map.Entry<ErrorCode, AtomicInteger> e : ops.entrySet()) {
                        sb.append(e.getKey()).append(":").append(e.getValue()).append(" ");
                        e.getValue().set(0);
                    }
                    System.out.println(sb.toString());
                    
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }}
        );
        
        // Modify Gateway
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(3000);
                        System.out.println("Delete gateway 3");
                        gcp.delGw(3, "127.0.0.1", 6020);

                        Thread.sleep(3000);
                        System.out.println("Add gateway 3");
                        gwAdd(3, "127.0.0.1", 6020, 2);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }

                }
            }
        });

        Thread.sleep(86400 * 1000);
    }

}
