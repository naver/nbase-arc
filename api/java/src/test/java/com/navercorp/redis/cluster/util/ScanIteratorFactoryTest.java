package com.navercorp.redis.cluster.util;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.jedis.JedisConverters;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.redis.cluster.gateway.GatewayClient;
import com.navercorp.redis.cluster.gateway.GatewayConfig;
import com.navercorp.redis.cluster.gateway.GatewayException;
import com.navercorp.redis.cluster.util.ScanIteratorFactory.ScanIterator;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.SafeEncoder;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-gatewayclient.xml")
public class ScanIteratorFactoryTest {

    @Autowired
    GatewayClient gatewayClient;

    static final String KEY = "key";
    static final byte[] KEY_BYTES = JedisConverters.toBytes("key");
    static final String VALUE = "value";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        clear();
    }

    private void clear() {
        gatewayClient.del(KEY_BYTES);
    }
    
    @Test
    public void scanIterator() {
        long expectedPosition = 1;
        Queue<String> expectedKeys = new LinkedList<String>();
        Queue<Long> expectedCursors = new LinkedList<Long>();
        final Queue<ScanResult<String>> results = new LinkedList<ScanResult<String>>();  
        
        {   // data O, cursor O
            List<String> keys = new ArrayList<String>();
            keys.add("test1"); expectedKeys.add("test1"); expectedCursors.add(7L);
            keys.add("test2"); expectedKeys.add("test2"); expectedCursors.add(7L);
            results.add(new ScanResult<String>("7", keys));
        }

        {   // data X, cursor O
            List<String> keys = new ArrayList<String>();
            results.add(new ScanResult<String>("15", keys));
        }

        {   // data O, cursor X
            List<String> keys = new ArrayList<String>();
            keys.add("test3"); expectedKeys.add("test3"); expectedCursors.add(0L);
            keys.add("test4"); expectedKeys.add("test4"); expectedCursors.add(0L);
            results.add(new ScanResult<String>("0", keys));
        }

        {   // data X, cursor X
            List<String> keys = new ArrayList<String>();
            results.add(new ScanResult<String>("0", keys));
        }
        
        ScanIteratorFactory.Scanner<String> scanner = new ScanIteratorFactory.Scanner<String>() {
            @Override
            public ScanResult<String> scan(byte[] cursor, ScanParams params) {
                return results.poll();
            }
        };
        ScanIterator<String> iterator = 
                new ScanIterator<String>(new ScanParams(), scanner);
        assertEquals(iterator.getCursor(), 0);
        
        while (iterator.hasNext()) {
            assertEquals(expectedKeys.poll(), iterator.next());
            assertEquals(expectedCursors.poll(), Long.valueOf(iterator.getCursor()));
            assertEquals(expectedPosition++, iterator.getPosition());
        }
        
        assertEquals(0, expectedKeys.size());
        assertEquals(0, expectedCursors.size());
    }

    @Test(expected = NoSuchElementException.class)
    public void scanIteratorNoSuchElementException() {
        ScanIteratorFactory.Scanner<String> scanner = new ScanIteratorFactory.Scanner<String>() {
            @Override
            public ScanResult<String> scan(byte[] cursor, ScanParams params) {
                List<String> keys = new ArrayList<String>();
                keys.add("test3");
                keys.add("test4");
                return new ScanResult<String>("0", keys);
            }
        };
        ScanIterator<String> iterator = 
                new ScanIterator<String>(new ScanParams(), scanner);
        
        while (true) {
            iterator.next();
        }
    }
    
    @Test
    public void scanIteratorShouldNotLoopWhenNoValuesFound() {
        Queue<ScanResult<String>> scanResults = new LinkedList<ScanResult<String>>();
        scanResults.add(createScanResult(0));
        ScanIterator<?> iterator = createScanIterator(scanResults);
        assertFalse(iterator.hasNext());
    }
    
    @Test
    public void scanIteratorShouldNotLoopWhenReachingStartingPointInFirstLoop() {
        Queue<ScanResult<String>> scanResults = new LinkedList<ScanResult<String>>();
        scanResults.add(createScanResult(0, "spring", "data", "redis"));
        ScanIterator<?> iterator = createScanIterator(scanResults);

        assertEquals("spring", iterator.next());
        assertEquals(0L, iterator.getCursor());
        assertEquals(true, iterator.hasNext());

        assertEquals("data", iterator.next());
        assertEquals(0L, iterator.getCursor());
        assertEquals(true, iterator.hasNext());

        assertEquals("redis", iterator.next());
        assertEquals(0L, iterator.getCursor());
        assertEquals(false, iterator.hasNext());
    }
    
    @Test
    public void hasNextShouldCallScanUntilFinishedWhenScanResultIsAnEmptyCollection() {
        Queue<ScanResult<String>> scanResults = new LinkedList<ScanResult<String>>();
        scanResults.add(createScanResult(1, "spring"));
        for (long i = 2; i < 6; i++) {
            scanResults.add(createScanResult(i));
        }
        scanResults.add(createScanResult(0, "data"));
        
        ScanIterator<String> iterator = createScanIterator(scanResults);
        assertEquals(0, iterator.getPosition());
        
        List<String> results = new ArrayList<String>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        
        assertEquals(2, results.size());
        assertEquals("spring", results.get(0));
        assertEquals("data", results.get(1));
        assertEquals(2L, iterator.getPosition());
    }
    
    @Test
    public void hasNextShouldStopWhenScanResultIsAnEmptyCollectionAndStateIsFinished() {
        Queue<ScanResult<String>> scanResults = new LinkedList<ScanResult<String>>();
        scanResults.add(createScanResult(1, "spring"));
        for (long i = 2; i < 6; i++) {
            scanResults.add(createScanResult(i));
        }
        scanResults.add(createScanResult(6, "data"));
        scanResults.add(createScanResult(0));
        
        ScanIterator<String> iterator = createScanIterator(scanResults);
        assertEquals(0, iterator.getPosition());
        
        List<String> results = new ArrayList<String>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }

        assertEquals(2, results.size());
        assertEquals("spring", results.get(0));
        assertEquals("data", results.get(1));
        assertEquals(2L, iterator.getPosition());
    }
    
    @Test
    public void hasNextShouldStopCorrectlyWhenWholeScanIterationDoesNotReturnResultsAndStateIsFinished() {
        Queue<ScanResult<String>> scanResults = new LinkedList<ScanResult<String>>();
        for (long i = 1; i < 6; i++) {
            scanResults.add(createScanResult(i));
        }
        scanResults.add(createScanResult(0));
        
        ScanIterator<?> iterator = createScanIterator(scanResults);
        assertEquals(0, iterator.getPosition());
        
        int loops = 0;
        while (iterator.hasNext()) {
            iterator.next();
            loops++;
        }
        
        assertEquals(0, loops);
        assertEquals(0, iterator.getPosition());
    }

    @Test
    public void scanIteratorWithGatewayClient() {
        String[] keys = { "test:string:scan1", "test:string:scan2", "test:string:scan3" };
        List<String> scanResults = new ArrayList<String>();

        for (int i = 0; i < keys.length; i++) {
            gatewayClient.del(keys[i]);
        }

        for (int i = 0; i < keys.length; i++) {
            gatewayClient.setrange(keys[i], 5, keys[i]);
        }

        ScanIterator<byte[]> scanIterator = ScanIteratorFactory.createScanIterator(gatewayClient,
                new ScanParams().count(2).match("test:string:scan*"));
        while (scanIterator.hasNext()) {
            scanResults.add(SafeEncoder.encode(scanIterator.next()));
        }

        for (String key : keys) {
            assertTrue(key + " isn't scaned", scanResults.contains(key));
        }
    }

    /**
     * Ignore this test until a bug in the Gateway is fixed.
     * When the scan command contains an invalid option value,
     * Gateway's scan command replies normal array response,
     * but it is expected for the Gateway to reply an error response.
     * @throws Throwable 
     */
    @Ignore
    @Test(expected = JedisDataException.class)
    public void expectSyntaxErrorWhenInvalidCountOption() {
        ScanIterator<byte[]> scanIterator = 
                ScanIteratorFactory.createScanIterator(gatewayClient, new ScanParams().count(0));

        try {
            while (scanIterator.hasNext()) {
                scanIterator.next();
            }
        } catch (GatewayException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof JedisDataException);
            assertTrue(cause.getMessage().contains("ERR syntax error"));
            throw (JedisDataException) cause;
        }
    }

    /**
     * @see #expectSyntaxErrorWhenInvalidCountOption()
     */
    @Ignore
    @Test(expected = JedisDataException.class)
    public void expectSyntaxErrorWhenInvalidMatchOption() {
        ScanIterator<byte[]> scanIterator = 
                ScanIteratorFactory.createScanIterator(gatewayClient, new ScanParams().match(""));

        try {
            while (scanIterator.hasNext()) {
                scanIterator.next();
            }
        } catch (GatewayException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof JedisDataException);
            assertTrue(cause.getMessage().contains("ERR syntax error"));
            throw (JedisDataException) cause;
        }
    }

    protected ScanResult<String> createScanResult(long cursor, String... keys) {
        return new ScanResult<String>(String.valueOf(cursor), Arrays.asList(keys));
    }
    
    protected ScanIterator<String> createScanIterator(final Queue<ScanResult<String>> scanResults) {
        ScanIteratorFactory.Scanner<String> scanner = new ScanIteratorFactory.Scanner<String>() {
            @Override
            public ScanResult<String> scan(byte[] cursor, ScanParams params) {
                return scanResults.poll();
            }
        };
        return new ScanIterator<String>(new ScanParams(), scanner);
    }

}
