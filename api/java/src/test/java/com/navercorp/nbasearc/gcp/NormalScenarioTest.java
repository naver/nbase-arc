package com.navercorp.nbasearc.gcp;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jayway.awaitility.Awaitility.await;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.SettableFuture;
import com.jayway.awaitility.Duration;
import com.navercorp.nbasearc.gcp.ErrorCode;
import com.navercorp.nbasearc.gcp.Gateway;
import com.navercorp.nbasearc.gcp.GatewayConnectionPool;
import com.navercorp.nbasearc.gcp.RequestCallback;
import com.navercorp.nbasearc.gcp.VirtualConnection;
import com.navercorp.redis.cluster.gateway.AffinityState;

public class NormalScenarioTest {

    final Integer GW_ID = 3;
    final String GW_IP = "127.0.0.1";
    final int GW_PORT = 6000;
    final int CON_CNT = 2; 
    final byte[] CMD_SET = "*3\r\n$3\r\nset\r\n$4\r\nhaha\r\n$4\r\nhoho\r\n".getBytes();

    GatewayConnectionPool gcp;
    
    @Before
    public void before()
            throws SecurityException, IllegalArgumentException, NoSuchFieldException, IllegalAccessException {
        gcp = new GatewayConnectionPool(1, true);
        gwAdd(GW_ID, GW_IP, GW_PORT, CON_CNT);
    }
    
    @After
    public void after() throws InterruptedException, ExecutionException {
        try {
            gcp.close().get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void gwAdd(final Integer id, final String ip, final int port, final int concnt)
            throws SecurityException, IllegalArgumentException, NoSuchFieldException, IllegalAccessException {
        gcp.addGw(id, ip, port, concnt, 1000);
        final Gateway gw = getGwMap(gcp).get(id);
        await().atMost(Duration.TEN_SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return gw.getActive() == concnt;
            }
        });
    }
    
    private void gwDel(final Integer id)
            throws SecurityException, IllegalArgumentException, NoSuchFieldException, IllegalAccessException {
        final Gateway gw = getGwMap(gcp).get(id);
        gcp.delGw(id);
        await().atMost(Duration.ONE_MINUTE).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return gw.getState() == Gateway.State.UNUSED && gw.getActive() == 0;
            }
        });
    }
    
    @Test
    public void gwDel()
            throws SecurityException, IllegalArgumentException, NoSuchFieldException, IllegalAccessException {
        gwDel(GW_ID);
    }
    
    @Test
    public void termination() throws InterruptedException {
        final int MAX_THREAD = 1;
        final ExecutorService executor = Executors.newFixedThreadPool(MAX_THREAD + 2, new ThreadFactory() {
            AtomicInteger no = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "TestThread-" + no.getAndIncrement());
            }
        });
        
        for (int t = 0; t < MAX_THREAD; t++) {
            final byte[] CMD_SET = String.format("*3\r\n$3\r\nset\r\n$6\r\nhaha%02d\r\n$4\r\nhoho\r\n", t).getBytes();
            final byte[] CMD_GET = String.format("*2\r\n$3\r\nget\r\n$6\r\nhaha%02d\r\n", t).getBytes();

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    final VirtualConnection vc = gcp.newVc(512);
                    vc.allocPc(0, AffinityState.READ, true);
                    for (int i = 0; i < 10000; i++) {
                        vc.request(CMD_SET, 3000, new RequestCallback() {
                            @Override
                            public void onResponse(byte[] response, ErrorCode err) {
                            }
                        });

                        vc.request(CMD_GET, 3000, new RequestCallback() {
                            @Override
                            public void onResponse(byte[] response, ErrorCode err) {
                            }
                        });
                    }
                }
            });
        }
        
        Thread.sleep(100);
    }
    
    @SuppressWarnings("unchecked")
    private Map<String, Gateway> getGwMap(GatewayConnectionPool gcp)
            throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field f = gcp.getClass().getDeclaredField("gwMap");
        f.setAccessible(true);
        return (Map<String, Gateway>) f.get(gcp);
    }
    
    @Test
    public void request() throws InterruptedException, ExecutionException, TimeoutException {
        final SettableFuture<Boolean> result = SettableFuture.create();  
        VirtualConnection vc = gcp.newVc(512);
        vc.allocPc(0, AffinityState.READ, true);
        vc.request(CMD_SET, 1000, new RequestCallback() {
            @Override
            public void onResponse(byte[] response, ErrorCode err) {
                String s = new String(response, Charsets.UTF_8);
                result.set(s.equals("+OK\r\n") && err == ErrorCode.OK);
            }
        });
        assertTrue(result.get(10000, TimeUnit.MILLISECONDS));
    }
    
    @Test
    public void requestWithChangingPc() throws InterruptedException, ExecutionException, TimeoutException,
            SecurityException, IllegalArgumentException, NoSuchFieldException, IllegalAccessException {
        final SettableFuture<Boolean> result = SettableFuture.create();  
        VirtualConnection vc = gcp.newVc(512);
        vc.allocPc(0, AffinityState.READ, true);
        vc.request(CMD_SET, 1000, new RequestCallback() {
            @Override
            public void onResponse(byte[] response, ErrorCode err) {
                String s = new String(response, Charsets.UTF_8);
                result.set(s.equals("+OK\r\n") && err == ErrorCode.OK);
            }
        });
        assertTrue(result.get(10000, TimeUnit.MILLISECONDS));
        
        gwDel(GW_ID);
        
        gwAdd(2, "127.0.0.1", 6010, 2);

        vc.request(CMD_SET, 1000, new RequestCallback() {
            @Override
            public void onResponse(byte[] response, ErrorCode err) {
                String s = new String(response, Charsets.UTF_8);
                result.set(s.equals("OK") && err == ErrorCode.OK);
            }
        });
        assertTrue(result.get(10000, TimeUnit.MILLISECONDS));
    }

}
