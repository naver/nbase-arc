package com.navercorp.nbasearc.gcp;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.awaitility.Duration;
import com.navercorp.nbasearc.gcp.StatusCode;
import com.navercorp.nbasearc.gcp.GatewayConnectionPool;
import com.navercorp.nbasearc.gcp.RequestCallback;
import com.navercorp.nbasearc.gcp.VirtualConnection;
import com.navercorp.redis.cluster.gateway.AffinityState;

public class PhysicalConnectionTest {

    private static final Logger log = LoggerFactory.getLogger(VirtualConnection.class);
    
    final Integer ID = 3;
    final String IP = "127.0.0.1";
    final int PORT = 6000;
    
    @Test
    public void unuse() throws InterruptedException, ExecutionException, TimeoutException {
        GatewayConnectionPool gcp = new GatewayConnectionPool(1, true);
        gcp.addGw(ID, IP, PORT, 1, 1000).get(1000, TimeUnit.MILLISECONDS);
        
        VirtualConnection vc = gcp.newVc(512);
        vc.allocPc(0, AffinityState.READ, true);
        
        final int MAX = 3000000;
        final byte[] CMD = String.format("*3\r\n$3\r\nset\r\n$6\r\nhaha99\r\n$4\r\nhoho\r\n").getBytes();
        final AtomicInteger remaining = new AtomicInteger(MAX);
        for (int i = 0; i < MAX; i++) {
            vc.request(CMD, 3000, new RequestCallback() {
                @Override
                public void onResponse(byte[] response, StatusCode statusCode) {
                    remaining.decrementAndGet();
                }
            });
        }
        
        await().atMost(Duration.TEN_MINUTES).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return remaining.get() == 0;
            }
        });
        
        gcp.delGw(ID, IP, PORT);
    }

}
