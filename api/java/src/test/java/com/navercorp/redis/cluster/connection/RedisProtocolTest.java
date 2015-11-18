/*
 * Copyright 2015 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.navercorp.redis.cluster.connection;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.navercorp.redis.cluster.util.RedisInputStream;
import com.navercorp.redis.cluster.util.RedisOutputStream;

import redis.clients.util.SafeEncoder;

/**
 * @author jaehong.kim
 */
public class RedisProtocolTest {

    @Test
    public void buildACommand() throws IOException {
        PipedInputStream pis = new PipedInputStream();
        BufferedInputStream bis = new BufferedInputStream(pis);
        PipedOutputStream pos = new PipedOutputStream(pis);
        RedisOutputStream ros = new RedisOutputStream(pos);

        RedisProtocol.sendCommand(ros, RedisProtocol.Command.GET, "SOMEKEY".getBytes(RedisProtocol.CHARSET));
        ros.flush();
        pos.close();
        String expectedCommand = "*2\r\n$3\r\nGET\r\n$7\r\nSOMEKEY\r\n";

        int b;
        StringBuilder sb = new StringBuilder();
        while ((b = bis.read()) != -1) {
            sb.append((char) b);
        }

        assertEquals(expectedCommand, sb.toString());
    }

    @Test
    public void bulkReply() {
        InputStream is = new ByteArrayInputStream("$6\r\nfoobar\r\n".getBytes());
        byte[] response = (byte[]) RedisProtocol.read(new RedisInputStream(is));
        assertArrayEquals(SafeEncoder.encode("foobar"), response);
    }

    @Test
    public void fragmentedBulkReply() {
        FragmentedByteArrayInputStream fis = new FragmentedByteArrayInputStream(
                "$30\r\n012345678901234567890123456789\r\n".getBytes());
        byte[] response = (byte[]) RedisProtocol.read(new RedisInputStream(fis));
        assertArrayEquals(SafeEncoder.encode("012345678901234567890123456789"), response);
    }

    @Test
    public void nullBulkReply() {
        InputStream is = new ByteArrayInputStream("$-1\r\n".getBytes());
        String response = (String) RedisProtocol.read(new RedisInputStream(is));
        assertEquals(null, response);
    }

    @Test
    public void singleLineReply() {
        InputStream is = new ByteArrayInputStream("+OK\r\n".getBytes());
        byte[] response = (byte[]) RedisProtocol.read(new RedisInputStream(is));
        assertArrayEquals(SafeEncoder.encode("OK"), response);
    }

    @Test
    public void integerReply() {
        InputStream is = new ByteArrayInputStream(":123\r\n".getBytes());
        long response = (Long) RedisProtocol.read(new RedisInputStream(is));
        assertEquals(123, response);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void multiBulkReply() {
        InputStream is = new ByteArrayInputStream(
                "*4\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$5\r\nHello\r\n$5\r\nWorld\r\n".getBytes());
        List<byte[]> response = (List<byte[]>) RedisProtocol.read(new RedisInputStream(is));
        List<byte[]> expected = new ArrayList<byte[]>();
        expected.add(SafeEncoder.encode("foo"));
        expected.add(SafeEncoder.encode("bar"));
        expected.add(SafeEncoder.encode("Hello"));
        expected.add(SafeEncoder.encode("World"));

        assertEquals(expected.size(), response.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void nullMultiBulkReply() {
        InputStream is = new ByteArrayInputStream("*-1\r\n".getBytes());
        List<String> response = (List<String>) RedisProtocol.read(new RedisInputStream(is));
        assertNull(response);
    }

    public class FragmentedByteArrayInputStream extends ByteArrayInputStream {
        private int readMethodCallCount = 0;

        public FragmentedByteArrayInputStream(final byte[] buf) {
            super(buf);
        }

        public synchronized int read(final byte[] b, final int off, final int len) {
            readMethodCallCount++;
            if (len <= 10) {
                // if the len <= 10, return as usual ..
                return super.read(b, off, len);
            } else {
                // else return the first half ..
                return super.read(b, off, len / 2);
            }
        }

        public int getReadMethodCallCount() {
            return readMethodCallCount;
        }
    }
}
