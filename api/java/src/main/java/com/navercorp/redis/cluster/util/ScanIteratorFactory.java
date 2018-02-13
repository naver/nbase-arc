package com.navercorp.redis.cluster.util;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import com.navercorp.redis.cluster.gateway.GatewayClient;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.util.SafeEncoder;

public class ScanIteratorFactory {

    interface Scanner<T> {
        ScanResult<T> scan(byte[] cursor, ScanParams params);
    }

    public static class ScanIterator<T> {
        private final ScanParams params;
        private final Scanner<T> scanner;

        private ScanResult<T> result;
        private int resultIndex;
        private long position;
        private byte[] cursor;

        ScanIterator(ScanParams options, Scanner<T> scanner) {
            this.params = options;
            this.scanner = scanner;
            this.result = null;
            this.resultIndex = 0;
            this.position = 0;
            this.cursor = ScanParams.SCAN_POINTER_START_BINARY;
        }

        private boolean isCursorZero() {
            return Arrays.equals(this.cursor, ScanParams.SCAN_POINTER_START_BINARY);
        }

        private boolean isEnd() {
            return result != null && isCursorZero() && resultIndex == result.getResult().size();
        }

        private boolean hasLocalNext() {
            return result != null && resultIndex < result.getResult().size();
        }

        public boolean hasNext() {
            while (!hasLocalNext() && !isEnd()) {
                result = scanner.scan(cursor, params);
                cursor = result.getCursorAsBytes();
                resultIndex = 0;
            }
            return hasLocalNext();
        }

        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements available.");
            }

            position++;
            return result.getResult().get(resultIndex++);
        }

        public long getPosition() {
            return position;
        }

        public long getCursor() {
            return Long.valueOf(SafeEncoder.encode(cursor));
        }
    }

    public static ScanIterator<byte[]> createScanIterator(
            final GatewayClient client, final ScanParams options) {

        Scanner<byte[]> scanner = new Scanner<byte[]>() {
            @Override
            public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
                return client.scan(cursor, params);
            }
        };

        return new ScanIterator<byte[]>(options, scanner);
    }

    public static ScanIterator<byte[]> createSScanIterator(
            final GatewayClient client, final byte[] key, final ScanParams options) {

        Scanner<byte[]> scanner = new Scanner<byte[]>() {
            @Override
            public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
                return client.sscan(key, cursor, params);
            }
        };

        return new ScanIterator<byte[]>(options, scanner);
    }

    public static ScanIterator<Tuple> createZScanIterator(
            final GatewayClient client, final byte[] key, final ScanParams options) {

        Scanner<redis.clients.jedis.Tuple> scanner = new Scanner<redis.clients.jedis.Tuple>() {
            @Override
            public ScanResult<redis.clients.jedis.Tuple> scan(byte[] cursor, ScanParams params) {
                return client.zscan(key, cursor, params);
            }
        };

        return new ScanIterator<Tuple>(options, scanner);
    }

    public static ScanIterator<Entry<byte[], byte[]>> createHScanIterator(
            final GatewayClient client, final byte[] key, final ScanParams options) {

        Scanner<Entry<byte[], byte[]>> scanner = new Scanner<Entry<byte[], byte[]>>() {
            @Override
            public ScanResult<Entry<byte[], byte[]>> scan(byte[] cursor, ScanParams params) {
                return client.hscan(key, cursor, params);
            }
        };

        return new ScanIterator<Entry<byte[], byte[]>>(options, scanner);
    }

}
