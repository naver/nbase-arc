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

package com.navercorp.redis.cluster;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.util.SafeEncoder;

/**
 * @author jaehong.kim
 */
public class SortedSetCommandsTest extends RedisClusterTestBase {
    final byte[] ba = {0x0A};
    final byte[] bb = {0x0B};
    final byte[] bc = {0x0C};
    final byte[] bd = {0x0D};

    @Override
    public void clear() {
        redis.del(REDIS_KEY_0);
        redis.del(REDIS_KEY_1);
        redis.del(REDIS_KEY_2);
        redis.del(REDIS_BKEY_0);
        redis.del(REDIS_BKEY_1);
        redis.del(REDIS_BKEY_2);
    }

    @Test
    public void zadd() {
        long status = redis.zadd(REDIS_KEY_0, 1d, "a");
        assertEquals(1, status);

        status = redis.zadd(REDIS_KEY_0, 10d, "b");
        assertEquals(1, status);

        status = redis.zadd(REDIS_KEY_0, 0.1d, "c");
        assertEquals(1, status);

        status = redis.zadd(REDIS_KEY_0, 2d, "a");
        assertEquals(0, status);

        status = redis.zadd(REDIS_KEY_0, 100d, "a", ZAddParams.zAddParams().xx());
        assertEquals(0, status);
        status = redis.zadd(REDIS_KEY_0, 101d, "a", ZAddParams.zAddParams().xx().ch());
        assertEquals(1, status);

        status = redis.zadd(REDIS_KEY_0, 100d, "d", ZAddParams.zAddParams().nx());
        assertEquals(1, status);
        status = redis.zadd(REDIS_KEY_0, 101d, "d", ZAddParams.zAddParams().nx().ch());
        assertEquals(0, status);

        Map<String, Double> elements = new HashMap<String, Double>();
        elements.put("a", 0d);
        elements.put("b", 1d);
        elements.put("c", 2d);
        status = redis.zadd(REDIS_KEY_1, elements, ZAddParams.zAddParams().nx());
        assertEquals(3, status);
        
        Set<Tuple> key1Elements = redis.zrangeWithScores(REDIS_KEY_1, 0, 3);
        assertTrue(key1Elements.contains(new Tuple("a", 0d)));
        assertTrue(key1Elements.contains(new Tuple("b", 1d)));
        assertTrue(key1Elements.contains(new Tuple("c", 2d)));

        // Binary
        long bstatus = redis.zadd(REDIS_BKEY_0, 1d, ba);
        assertEquals(1, bstatus);

        bstatus = redis.zadd(REDIS_BKEY_0, 10d, bb);
        assertEquals(1, bstatus);

        bstatus = redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        assertEquals(1, bstatus);

        bstatus = redis.zadd(REDIS_BKEY_0, 2d, ba);
        assertEquals(0, bstatus);

        bstatus = redis.zadd(REDIS_BKEY_0, 100d, ba, ZAddParams.zAddParams().xx());
        assertEquals(0, bstatus);
        bstatus = redis.zadd(REDIS_BKEY_0, 101d, ba, ZAddParams.zAddParams().xx().ch());
        assertEquals(1, bstatus);

        bstatus = redis.zadd(REDIS_BKEY_0, 100d, bd, ZAddParams.zAddParams().nx());
        assertEquals(1, bstatus);
        bstatus = redis.zadd(REDIS_BKEY_0, 101d, bd, ZAddParams.zAddParams().nx().ch());
        assertEquals(0, bstatus);

        Map<byte[], Double> belements = new HashMap<byte[], Double>();
        belements.put(ba, 0d);
        belements.put(bb, 1d);
        belements.put(bc, 2d);
        bstatus = redis.zadd(REDIS_BKEY_1, belements, ZAddParams.zAddParams().nx());
        assertEquals(3, bstatus);

        Set<Tuple> bkey1Elements = redis.zrangeWithScores(REDIS_BKEY_1, 0, 3);
        assertTrue(bkey1Elements.contains(new Tuple(ba, 0d)));
        assertTrue(bkey1Elements.contains(new Tuple(bb, 1d)));
        assertTrue(bkey1Elements.contains(new Tuple(bc, 2d)));
    }

    @Test
    public void zrange() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 10d, "b");
        redis.zadd(REDIS_KEY_0, 0.1d, "c");
        redis.zadd(REDIS_KEY_0, 2d, "a");

        Set<String> expected = new LinkedHashSet<String>();
        expected.add("c");
        expected.add("a");

        Set<String> range = redis.zrange(REDIS_KEY_0, 0, 1);
        assertEquals(expected, range);

        expected.add("b");
        range = redis.zrange(REDIS_KEY_0, 0, 100);
        assertEquals(expected, range);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(bc);
        bexpected.add(ba);

        Set<byte[]> brange = redis.zrange(REDIS_BKEY_0, 0, 1);
        assertEquals(bexpected, brange);

        bexpected.add(bb);
        brange = redis.zrange(REDIS_BKEY_0, 0, 100);
        assertEquals(bexpected, brange);

    }

    @Test
    public void zrevrange() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 10d, "b");
        redis.zadd(REDIS_KEY_0, 0.1d, "c");
        redis.zadd(REDIS_KEY_0, 2d, "a");

        Set<String> expected = new LinkedHashSet<String>();
        expected.add("b");
        expected.add("a");

        Set<String> range = redis.zrevrange(REDIS_KEY_0, 0, 1);
        assertEquals(expected, range);

        expected.add("c");
        range = redis.zrevrange(REDIS_KEY_0, 0, 100);
        assertEquals(expected, range);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(bb);
        bexpected.add(ba);

        Set<byte[]> brange = redis.zrevrange(REDIS_BKEY_0, 0, 1);
        assertEquals(bexpected, brange);

        bexpected.add(bc);
        brange = redis.zrevrange(REDIS_BKEY_0, 0, 100);
        assertEquals(bexpected, brange);

    }

    @Test
    public void zrem() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 2d, "b");

        long status = redis.zrem(REDIS_KEY_0, "a");

        Set<String> expected = new LinkedHashSet<String>();
        expected.add("b");

        assertEquals(1, status);
        assertEquals(expected, redis.zrange(REDIS_KEY_0, 0, 100));

        status = redis.zrem(REDIS_KEY_0, REDIS_KEY_1);

        assertEquals(0, status);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 2d, bb);

        long bstatus = redis.zrem(REDIS_BKEY_0, ba);

        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(bb);

        assertEquals(1, bstatus);
        assertEquals(bexpected, redis.zrange(REDIS_BKEY_0, 0, 100));

        bstatus = redis.zrem(REDIS_BKEY_0, REDIS_BKEY_1);

        assertEquals(0, bstatus);

    }

    @Test
    public void zincrby() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 2d, "b");

        double score = redis.zincrby(REDIS_KEY_0, 2d, "a");

        Set<String> expected = new LinkedHashSet<String>();
        expected.add("a");
        expected.add("b");

        assertEquals(3d, score, 0);
        assertEquals(expected, redis.zrange(REDIS_KEY_0, 0, 100));

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 2d, bb);

        double bscore = redis.zincrby(REDIS_BKEY_0, 2d, ba);

        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(bb);
        bexpected.add(ba);

        assertEquals(3d, bscore, 0);
        assertEquals(bexpected, redis.zrange(REDIS_BKEY_0, 0, 100));

    }

    @Test
    public void zrank() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 2d, "b");

        long rank = redis.zrank(REDIS_KEY_0, "a");
        assertEquals(0, rank);

        rank = redis.zrank(REDIS_KEY_0, "b");
        assertEquals(1, rank);

        assertNull(redis.zrank(REDIS_KEY_2, "b"));

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 2d, bb);

        long brank = redis.zrank(REDIS_BKEY_0, ba);
        assertEquals(0, brank);

        brank = redis.zrank(REDIS_BKEY_0, bb);
        assertEquals(1, brank);

        assertNull(redis.zrank(REDIS_BKEY_2, bb));

    }

    @Test
    public void zrevrank() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 2d, "b");

        long rank = redis.zrevrank(REDIS_KEY_0, "a");
        assertEquals(1, rank);

        rank = redis.zrevrank(REDIS_KEY_0, "b");
        assertEquals(0, rank);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 2d, bb);

        long brank = redis.zrevrank(REDIS_BKEY_0, ba);
        assertEquals(1, brank);

        brank = redis.zrevrank(REDIS_BKEY_0, bb);
        assertEquals(0, brank);

    }

    @Test
    public void zrangeWithScores() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 10d, "b");
        redis.zadd(REDIS_KEY_0, 0.1d, "c");
        redis.zadd(REDIS_KEY_0, 2d, "a");

        Set<Tuple> expected = new LinkedHashSet<Tuple>();
        expected.add(new Tuple("c", 0.1d));
        expected.add(new Tuple("a", 2d));

        Set<Tuple> range = redis.zrangeWithScores(REDIS_KEY_0, 0, 1);
        assertEquals(expected, range);

        expected.add(new Tuple("b", 10d));
        range = redis.zrangeWithScores(REDIS_KEY_0, 0, 100);
        assertEquals(expected, range);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
        bexpected.add(new Tuple(bc, 0.1d));
        bexpected.add(new Tuple(ba, 2d));

        Set<Tuple> brange = redis.zrangeWithScores(REDIS_BKEY_0, 0, 1);
        assertEquals(bexpected, brange);

        bexpected.add(new Tuple(bb, 10d));
        brange = redis.zrangeWithScores(REDIS_BKEY_0, 0, 100);
        assertEquals(bexpected, brange);

    }

    @Test
    public void zrevrangeWithScores() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 10d, "b");
        redis.zadd(REDIS_KEY_0, 0.1d, "c");
        redis.zadd(REDIS_KEY_0, 2d, "a");

        Set<Tuple> expected = new LinkedHashSet<Tuple>();
        expected.add(new Tuple("b", 10d));
        expected.add(new Tuple("a", 2d));

        Set<Tuple> range = redis.zrevrangeWithScores(REDIS_KEY_0, 0, 1);
        assertEquals(expected, range);

        expected.add(new Tuple("c", 0.1d));
        range = redis.zrevrangeWithScores(REDIS_KEY_0, 0, 100);
        assertEquals(expected, range);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
        bexpected.add(new Tuple(bb, 10d));
        bexpected.add(new Tuple(ba, 2d));

        Set<Tuple> brange = redis.zrevrangeWithScores(REDIS_BKEY_0, 0, 1);
        assertEquals(bexpected, brange);

        bexpected.add(new Tuple(bc, 0.1d));
        brange = redis.zrevrangeWithScores(REDIS_BKEY_0, 0, 100);
        assertEquals(bexpected, brange);

    }

    @Test
    public void zcard() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 10d, "b");
        redis.zadd(REDIS_KEY_0, 0.1d, "c");
        redis.zadd(REDIS_KEY_0, 2d, "a");

        long size = redis.zcard(REDIS_KEY_0);
        assertEquals(3, size);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        long bsize = redis.zcard(REDIS_BKEY_0);
        assertEquals(3, bsize);

    }

    @Test
    public void zscore() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 10d, "b");
        redis.zadd(REDIS_KEY_0, 0.1d, "c");
        redis.zadd(REDIS_KEY_0, 2d, "a");

        Double score = redis.zscore(REDIS_KEY_0, "b");
        assertEquals((Double) 10d, score);

        score = redis.zscore(REDIS_KEY_0, "c");
        assertEquals((Double) 0.1d, score);

        score = redis.zscore(REDIS_KEY_0, "s");
        assertNull(score);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        Double bscore = redis.zscore(REDIS_BKEY_0, bb);
        assertEquals((Double) 10d, bscore);

        bscore = redis.zscore(REDIS_BKEY_0, bc);
        assertEquals((Double) 0.1d, bscore);

        bscore = redis.zscore(REDIS_BKEY_0, SafeEncoder.encode("s"));
        assertNull(bscore);

    }

    @Test
    public void zcount() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 10d, "b");
        redis.zadd(REDIS_KEY_0, 0.1d, "c");
        redis.zadd(REDIS_KEY_0, 2d, "a");

        long result = redis.zcount(REDIS_KEY_0, 0.01d, 2.1d);

        assertEquals(2, result);

        result = redis.zcount(REDIS_KEY_0, "(0.01", "+inf");

        assertEquals(3, result);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        long bresult = redis.zcount(REDIS_BKEY_0, 0.01d, 2.1d);

        assertEquals(2, bresult);

        bresult = redis.zcount(REDIS_BKEY_0, SafeEncoder.encode("(0.01"), SafeEncoder.encode("+inf"));

        assertEquals(3, bresult);
    }

    @Test
    public void zrangebyscore() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 10d, "b");
        redis.zadd(REDIS_KEY_0, 0.1d, "c");
        redis.zadd(REDIS_KEY_0, 2d, "a");

        Set<String> range = redis.zrangeByScore(REDIS_KEY_0, 0d, 2d);

        Set<String> expected = new LinkedHashSet<String>();
        expected.add("c");
        expected.add("a");

        assertEquals(expected, range);

        range = redis.zrangeByScore(REDIS_KEY_0, 0d, 2d, 0, 1);

        expected = new LinkedHashSet<String>();
        expected.add("c");

        assertEquals(expected, range);

        range = redis.zrangeByScore(REDIS_KEY_0, 0d, 2d, 1, 1);
        Set<String> range2 = redis.zrangeByScore(REDIS_KEY_0, "-inf", "(2");
        assertEquals(expected, range2);

        expected = new LinkedHashSet<String>();
        expected.add("a");

        assertEquals(expected, range);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        Set<byte[]> brange = redis.zrangeByScore(REDIS_BKEY_0, 0d, 2d);

        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(bc);
        bexpected.add(ba);

        assertEquals(bexpected, brange);

        brange = redis.zrangeByScore(REDIS_BKEY_0, 0d, 2d, 0, 1);

        bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(bc);

        assertEquals(bexpected, brange);

        brange = redis.zrangeByScore(REDIS_BKEY_0, 0d, 2d, 1, 1);
        Set<byte[]> brange2 = redis.zrangeByScore(REDIS_BKEY_0, SafeEncoder.encode("-inf"), SafeEncoder.encode("(2"));
        assertEquals(bexpected, brange2);

        bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(ba);

        assertEquals(bexpected, brange);

    }

    @Test
    public void zrevrangebyscore() {
        redis.zadd(REDIS_KEY_0, 1.0d, "a");
        redis.zadd(REDIS_KEY_0, 2.0d, "b");
        redis.zadd(REDIS_KEY_0, 3.0d, "c");
        redis.zadd(REDIS_KEY_0, 4.0d, "d");
        redis.zadd(REDIS_KEY_0, 5.0d, "e");

        Set<String> range = redis.zrevrangeByScore(REDIS_KEY_0, 3d, Double.NEGATIVE_INFINITY, 0, 1);
        Set<String> expected = new LinkedHashSet<String>();
        expected.add("c");

        assertEquals(expected, range);

        range = redis.zrevrangeByScore(REDIS_KEY_0, 3.5d, Double.NEGATIVE_INFINITY, 0, 2);
        expected = new LinkedHashSet<String>();
        expected.add("c");
        expected.add("b");

        assertEquals(expected, range);

        range = redis.zrevrangeByScore(REDIS_KEY_0, 3.5d, Double.NEGATIVE_INFINITY, 1, 1);
        expected = new LinkedHashSet<String>();
        expected.add("b");

        assertEquals(expected, range);

        range = redis.zrevrangeByScore(REDIS_KEY_0, 4d, 2d);
        expected = new LinkedHashSet<String>();
        expected.add("d");
        expected.add("c");
        expected.add("b");

        assertEquals(expected, range);

        range = redis.zrevrangeByScore(REDIS_KEY_0, "+inf", "(4");
        expected = new LinkedHashSet<String>();
        expected.add("e");

        assertEquals(expected, range);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        Set<byte[]> brange = redis.zrevrangeByScore(REDIS_BKEY_0, 2d, 0d);

        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(bc);
        bexpected.add(ba);

        assertEquals(bexpected, brange);

        brange = redis.zrevrangeByScore(REDIS_BKEY_0, 2d, 0d, 0, 1);

        bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(ba);

        assertEquals(bexpected, brange);

        Set<byte[]> brange2 = redis.zrevrangeByScore(REDIS_BKEY_0, SafeEncoder.encode("+inf"), SafeEncoder.encode("(2"));

        bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(bb);

        assertEquals(bexpected, brange2);

        brange = redis.zrevrangeByScore(REDIS_BKEY_0, 2d, 0d, 1, 1);
        bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(bc);

        assertEquals(bexpected, brange);
    }

    @Test
    public void zrangebyscoreWithScores() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 10d, "b");
        redis.zadd(REDIS_KEY_0, 0.1d, "c");
        redis.zadd(REDIS_KEY_0, 2d, "a");

        Set<Tuple> range = redis.zrangeByScoreWithScores(REDIS_KEY_0, 0d, 2d);

        Set<Tuple> expected = new LinkedHashSet<Tuple>();
        expected.add(new Tuple("c", 0.1d));
        expected.add(new Tuple("a", 2d));

        assertEquals(expected, range);

        range = redis.zrangeByScoreWithScores(REDIS_KEY_0, 0d, 2d, 0, 1);

        expected = new LinkedHashSet<Tuple>();
        expected.add(new Tuple("c", 0.1d));

        assertEquals(expected, range);

        range = redis.zrangeByScoreWithScores(REDIS_KEY_0, 0d, 2d, 1, 1);

        expected = new LinkedHashSet<Tuple>();
        expected.add(new Tuple("a", 2d));

        assertEquals(expected, range);

        // Binary

        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        Set<Tuple> brange = redis.zrangeByScoreWithScores(REDIS_BKEY_0, 0d, 2d);

        Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
        bexpected.add(new Tuple(bc, 0.1d));
        bexpected.add(new Tuple(ba, 2d));

        assertEquals(bexpected, brange);

        brange = redis.zrangeByScoreWithScores(REDIS_BKEY_0, 0d, 2d, 0, 1);

        bexpected = new LinkedHashSet<Tuple>();
        bexpected.add(new Tuple(bc, 0.1d));

        assertEquals(bexpected, brange);

        brange = redis.zrangeByScoreWithScores(REDIS_BKEY_0, 0d, 2d, 1, 1);

        bexpected = new LinkedHashSet<Tuple>();
        bexpected.add(new Tuple(ba, 2d));

        assertEquals(bexpected, brange);

    }

    @Test
    public void zrevrangebyscoreWithScores() {
        redis.zadd(REDIS_KEY_0, 1.0d, "a");
        redis.zadd(REDIS_KEY_0, 2.0d, "b");
        redis.zadd(REDIS_KEY_0, 3.0d, "c");
        redis.zadd(REDIS_KEY_0, 4.0d, "d");
        redis.zadd(REDIS_KEY_0, 5.0d, "e");

        Set<Tuple> range = redis.zrevrangeByScoreWithScores(REDIS_KEY_0, 3d, Double.NEGATIVE_INFINITY, 0, 1);
        Set<Tuple> expected = new LinkedHashSet<Tuple>();
        expected.add(new Tuple("c", 3.0d));

        assertEquals(expected, range);

        range = redis.zrevrangeByScoreWithScores(REDIS_KEY_0, 3.5d, Double.NEGATIVE_INFINITY, 0, 2);
        expected = new LinkedHashSet<Tuple>();
        expected.add(new Tuple("c", 3.0d));
        expected.add(new Tuple("b", 2.0d));

        assertEquals(expected, range);

        range = redis.zrevrangeByScoreWithScores(REDIS_KEY_0, 3.5d, Double.NEGATIVE_INFINITY, 1, 1);
        expected = new LinkedHashSet<Tuple>();
        expected.add(new Tuple("b", 2.0d));

        assertEquals(expected, range);

        range = redis.zrevrangeByScoreWithScores(REDIS_KEY_0, 4d, 2d);
        expected = new LinkedHashSet<Tuple>();
        expected.add(new Tuple("d", 4.0d));
        expected.add(new Tuple("c", 3.0d));
        expected.add(new Tuple("b", 2.0d));

        assertEquals(expected, range);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        Set<Tuple> brange = redis.zrevrangeByScoreWithScores(REDIS_BKEY_0, 2d, 0d);

        Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
        bexpected.add(new Tuple(bc, 0.1d));
        bexpected.add(new Tuple(ba, 2d));

        assertEquals(bexpected, brange);

        brange = redis.zrevrangeByScoreWithScores(REDIS_BKEY_0, 2d, 0d, 0, 1);

        bexpected = new LinkedHashSet<Tuple>();
        bexpected.add(new Tuple(ba, 2d));

        assertEquals(bexpected, brange);

        brange = redis.zrevrangeByScoreWithScores(REDIS_BKEY_0, 2d, 0d, 1, 1);

        bexpected = new LinkedHashSet<Tuple>();
        bexpected.add(new Tuple(bc, 0.1d));

        assertEquals(bexpected, brange);
    }

    @Test
    public void zremrangeByRank() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 10d, "b");
        redis.zadd(REDIS_KEY_0, 0.1d, "c");
        redis.zadd(REDIS_KEY_0, 2d, "a");

        long result = redis.zremrangeByRank(REDIS_KEY_0, 0, 0);

        assertEquals(1, result);

        Set<String> expected = new LinkedHashSet<String>();
        expected.add("a");
        expected.add("b");

        assertEquals(expected, redis.zrange(REDIS_KEY_0, 0, 100));

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        long bresult = redis.zremrangeByRank(REDIS_BKEY_0, 0, 0);

        assertEquals(1, bresult);

        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(ba);
        bexpected.add(bb);

        assertEquals(bexpected, redis.zrange(REDIS_BKEY_0, 0, 100));

    }

    @Test
    public void zremrangeByScore() {
        redis.zadd(REDIS_KEY_0, 1d, "a");
        redis.zadd(REDIS_KEY_0, 10d, "b");
        redis.zadd(REDIS_KEY_0, 0.1d, "c");
        redis.zadd(REDIS_KEY_0, 2d, "a");

        long result = redis.zremrangeByScore(REDIS_KEY_0, 0, 2);

        assertEquals(2, result);

        Set<String> expected = new LinkedHashSet<String>();
        expected.add("b");

        assertEquals(expected, redis.zrange(REDIS_KEY_0, 0, 100));

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, ba);
        redis.zadd(REDIS_BKEY_0, 10d, bb);
        redis.zadd(REDIS_BKEY_0, 0.1d, bc);
        redis.zadd(REDIS_BKEY_0, 2d, ba);

        long bresult = redis.zremrangeByScore(REDIS_BKEY_0, 0, 2);

        assertEquals(2, bresult);

        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(bb);

        assertEquals(bexpected, redis.zrange(REDIS_BKEY_0, 0, 100));
    }

    @Test
    public void tupleCompare() {
        Tuple t1 = new Tuple(REDIS_KEY_0, 1d);
        Tuple t2 = new Tuple(REDIS_KEY_1, 2d);

        assertEquals(-1, t1.compareTo(t2));
        assertEquals(1, t2.compareTo(t1));
        assertEquals(0, t2.compareTo(t2));
    }
}