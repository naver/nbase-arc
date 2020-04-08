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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Charsets;
import com.navercorp.redis.cluster.gateway.GatewayAddress;
import com.navercorp.redis.cluster.gateway.GatewayServer;
import com.navercorp.redis.cluster.util.TestEnvUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ComparisonFailure;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.core.Cursor;

/**
 * @author jaehong.kim
 */
public abstract class RedisClusterTestBase extends Assert {
	public final String OK = "OK";
	public final String ZERO = "0";
	
    public final String REDIS_KEY_0 = "redis_key_0";
    public final String REDIS_KEY_1 = "redis_key_1";
    public final String REDIS_KEY_2 = "redis_key_2";
    public final String REDIS_KEY_3 = "redis_key_3";
    public final String REDIS_FIELD_0 = "redis_field_0";
    public final String REDIS_FIELD_1 = "redis_field_1";
    public final String REDIS_FIELD_2 = "redis_field_2";
    public final String REDIS_FIELD_3 = "redis_field_3";
    public final String REDIS_VALUE_0 = "redis_value_0";
    public final String REDIS_VALUE_1 = "redis_value_1";
    public final String REDIS_VALUE_2 = "redis_value_2";
    public final String REDIS_VALUE_3 = "redis_value_3";
    public final String REDIS_XX = "XX";
    public final String REDIS_NX = "NX";
    public final String REDIS_EX = "EX";
    public final String REDIS_PX = "PX";

	public final String K0 = "K0";
	public final String K1 = "K1";
	public final String K2 = "K2";
	public final byte[] BK0 = K0.getBytes(Charsets.UTF_8);
	public final byte[] BK1 = K1.getBytes(Charsets.UTF_8);
	public final byte[] BK2 = K2.getBytes(Charsets.UTF_8);
	public final String F0 = "F0";
	public final String F1 = "F1";
	public final String F2 = "F2";
	public final String N0 = "N0";
	public final String N1 = "N1";
	public final String N2 = "N2";
	public final String V0 = "V0";
	public final String V1 = "V1";
	public final String V2 = "V2";
	public final byte[] BV0 = V0.getBytes(Charsets.UTF_8);
	public final byte[] BV1 = V1.getBytes(Charsets.UTF_8);
	public final byte[] BV2 = V2.getBytes(Charsets.UTF_8);
    
    public final byte[] REDIS_BKEY_0 = {0x01, 0x02, 0x03, 0x04};
    public final byte[] REDIS_BKEY_1 = {0x01, 0x02, 0x03, 0x04, 0x0A};
    public final byte[] REDIS_BKEY_2 = {0x01, 0x02, 0x03, 0x04, 0x0B};
    public final byte[] REDIS_BKEY_3 = {0x01, 0x02, 0x03, 0x04, 0x0C};
    public final byte[] REDIS_BFIELD_0 = {0x02, 0x02, 0x03, 0x04};
    public final byte[] REDIS_BFIELD_1 = {0x02, 0x02, 0x03, 0x04, 0x0A};
    public final byte[] REDIS_BFIELD_2 = {0x02, 0x02, 0x03, 0x04, 0x0B};
    public final byte[] REDIS_BFIELD_3 = {0x02, 0x02, 0x03, 0x04, 0x0C};
    public final byte[] REDIS_BVALUE_0 = {0x05, 0x06, 0x07, 0x08};
    public final byte[] REDIS_BVALUE_1 = {0x05, 0x06, 0x07, 0x08, 0x0A};
    public final byte[] REDIS_BVALUE_2 = {0x05, 0x06, 0x07, 0x08, 0x0B};
    public final byte[] REDIS_BVALUE_3 = {0x05, 0x06, 0x07, 0x08, 0x0C};
    public final byte[] REDIS_BXX = {0x78, 0x78};
    public final byte[] REDIS_BNX = {0x6E, 0x78};
    public final byte[] REDIS_BEX = {0x65, 0x78};
    public final byte[] REDIS_BPX = {0x70, 0x78};
    
    public final byte[] REDIS_BKEY_VALUE_0 = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    public final byte[] REDIS_BKEY_STAR = {0x01, 0x02, 0x03, 0x04, '*'};
    public final byte[] REDIS_BVALUE_STAR = {0x05, 0x06, 0x07, 0x08, '*'};

    public static final String TRIPLES_KEY_0 = "TRIPLES_KEY_0";
    public static final String TRIPLES_KEY_1 = "TRIPLES_KEY_1";
    public static final String TRIPLES_KEY_2 = "TRIPLES_KEY_2";
    public static final String TRIPLES_FIELD_0 = "TRIPLES_FIELD_0";
    public static final String TRIPLES_FIELD_1 = "TRIPLES_FIELD_1";
    public static final String TRIPLES_FIELD_2 = "TRIPLES_FIELD_2";
    public static final String TRIPLES_NAME_0 = "TRIPLES_NAME_0";
    public static final String TRIPLES_NAME_1 = "TRIPLES_NAME_1";
    public static final String TRIPLES_VALUE_0 = "TRIPLES_VALUE_0";
    public static final String TRIPLES_VALUE_1 = "TRIPLES_VALUE_1";
    public static final String TRIPLES_VALUE_2 = "TRIPLES_VALUE_2";

    public static final byte[] TRIPLES_BKEY_0 = {0x04, 0x03, 0x02, 0x01};
    public static final byte[] TRIPLES_BKEY_1 = {0x04, 0x03, 0x02, 0x01, 0x0A};
    public static final byte[] TRIPLES_BKEY_2 = {0x04, 0x03, 0x02, 0x01, 0x0B};
    public static final byte[] TRIPLES_BFIELD_0 = {0x05, 0x04, 0x03, 0x02, 0x0A};
    public static final byte[] TRIPLES_BFIELD_1 = {0x05, 0x04, 0x03, 0x02, 0x0B};
    public static final byte[] TRIPLES_BFIELD_2 = {0x05, 0x04, 0x03, 0x02, 0x0C};
    public static final byte[] TRIPLES_BNAME_0 = {0x06, 0x05, 0x04, 0x03};
    public static final byte[] TRIPLES_BNAME_1 = {0x06, 0x05, 0x04, 0x03, 0x0A};
    public static final byte[] TRIPLES_BVALUE_0 = {0x07, 0x06, 0x05, 0x04};
    public static final byte[] TRIPLES_BVALUE_1 = {0x07, 0x06, 0x05, 0x04, 0x0A};
    public static final byte[] TRIPLES_BVALUE_2 = {0x07, 0x06, 0x05, 0x04, 0x0B};
    
    public static final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
    public static final byte[] bcar = { 0x09, 0x0A, 0x0B, 0x0C };
    
    public static final byte[] ba = { 0x0A };
    public static final byte[] bb = { 0x0B };
    public static final byte[] bc = { 0x0C };

    public static final byte[] bInclusiveB = { 0x5B, 0x0B };
    public static final byte[] bExclusiveC = { 0x28, 0x0C };
    public static final byte[] bLexMinusInf = { 0x2D };
    public static final byte[] bLexPlusInf = { 0x2B };

    public static final byte[] bbar1 = { 0x05, 0x06, 0x07, 0x08, 0x0A };
    public static final byte[] bbar2 = { 0x05, 0x06, 0x07, 0x08, 0x0B };
    public static final byte[] bbar3 = { 0x05, 0x06, 0x07, 0x08, 0x0C };
    public static final byte[] bbarstar = { 0x05, 0x06, 0x07, 0x08, '*' };

    public static RedisCluster redis;
    public static GatewayServer server;


    @Before
    public void before() {
        redis = new RedisCluster(TestEnvUtils.getHost(), TestEnvUtils.getPort(), 1000);
        server = new GatewayServer(new GatewayAddress(1, TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort()));
        clear();
    }

    @After
    public void after() {
        redis.disconnect();
    }

    public abstract void clear();

    protected void assertEquals(List<byte[]> expected, List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        for (int n = 0; n < expected.size(); n++) {
            assertArrayEquals(expected.get(n), actual.get(n));
        }
    }

    protected void assertEquals(Set<byte[]> expected, Set<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        Iterator<byte[]> e = expected.iterator();
        while (e.hasNext()) {
            byte[] next = e.next();
            boolean contained = false;
            for (byte[] element : expected) {
                if (Arrays.equals(next, element)) {
                    contained = true;
                }
            }
            if (!contained) {
                throw new ComparisonFailure("element is missing", Arrays.toString(next), actual.toString());
            }
        }
    }

    protected boolean arrayContains(List<byte[]> array, byte[] expected) {
        for (byte[] a : array) {
            try {
                assertArrayEquals(a, expected);
                return true;
            } catch (AssertionError e) {

            }
        }
        return false;
    }

    protected boolean setContains(Set<byte[]> set, byte[] expected) {
        for (byte[] a : set) {
            try {
                assertArrayEquals(a, expected);
                return true;
            } catch (AssertionError e) {

            }
        }
        return false;
    }
    
    public static void assertByteArraySetEquals(Set<byte[]> expected, Set<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        Iterator<byte[]> e = expected.iterator();
        while (e.hasNext()) {
            byte[] next = e.next();
            boolean contained = false;
            for (byte[] element : expected) {
                if (Arrays.equals(next, element)) {
                    contained = true;
                }
            }
            if (!contained) {
                throw new ComparisonFailure("element is missing", Arrays.toString(next), actual.toString());
            }
        }
    }

    public static interface ResultChecker {
		void check(String name, Object expectedResult, Object result);
	}
	
    public static class Equal implements ResultChecker {
		double THRESHOLD = .0001;
		
		@Override
		public void check(String name, Object expectedResult, Object result) {
			if (expectedResult instanceof Map) {
				Map<?, ?> c1 = (Map<?, ?>) expectedResult;
				Map<?, ?> c2 = (Map<?, ?>) result;

				for (Object k : c1.keySet()) {
					check(name + ", element " + k, c1.get(k), c2.get(k));
				}
			} else if (expectedResult instanceof Set) {
				Set<?> s1 = (Set<?>) expectedResult;
				Collection<?> s2 = (Collection<?>) result;
				
				for (Object o1 : s1) {
					boolean find = false;
					for (Object o2 : s2) {
						if (o1.getClass().isArray()) {
							assertEquals(name + " length of array", Array.getLength(o1), Array.getLength(o2));
							for (int i = 0; i < Array.getLength(o1); i++) {
								if (Array.get(o1, i).equals(Array.get(o2, i)) == false) {
									break;
								}
							}
							find = true;
						} else {
							find = o1.equals(o2);
						}
						
						if (find) {
							break;
						}
					}
					
					assertTrue(name, find);
				}
			} else if (expectedResult instanceof Double) {
				Double d1 = (Double) expectedResult;
				Double d2 = (Double) result;
				double diff = Math.abs(d1 - d2);
				assertTrue(name + " compare " + d1 + " with " + d2, diff < THRESHOLD);
			} else if (expectedResult instanceof byte[]) {
				assertArrayEquals(name, (byte[]) expectedResult, (byte[]) result);
			} else if (expectedResult instanceof Iterable) {
				Iterable<?> c1 = (Iterable<?>) expectedResult;
				Iterable<?> c2 = (Iterable<?>) result;

				Iterator<?> i1 = c1.iterator();
				Iterator<?> i2 = c2.iterator();
				int i = 0;
				while (i1.hasNext()) {
					check(name + ", element " + i, i1.next(), i2.next());
					i++;
				}
			} else if (expectedResult instanceof Point) {
				Point p1 = (Point) expectedResult;
				Point p2 = (Point) result;
				double diff = Math.abs(p1.getX() - p2.getX()) + Math.abs(p1.getY() - p2.getY());
				assertTrue(name + " compare " + p1 + " with " + p2, diff < THRESHOLD);
			} else if (expectedResult instanceof GeoResult) {
				@SuppressWarnings("unchecked")
				GeoResult<GeoLocation<byte[]>> g1 = (GeoResult<GeoLocation<byte[]>>) expectedResult;
				@SuppressWarnings("unchecked")
				GeoResult<GeoLocation<byte[]>> g2 = (GeoResult<GeoLocation<byte[]>>) result;
				check(name, g1.getContent().getPoint(), g2.getContent().getPoint());
				assertArrayEquals(name, g1.getContent().getName(), g2.getContent().getName());
				double diff = Math.abs(g1.getDistance().getValue() - g2.getDistance().getValue());
				assertTrue(name + " compare " + g1 + " with " + g2, diff < THRESHOLD);
			} else {
				assertEquals(name, expectedResult, result);
			}
		}
	}

	public static class ScanEqual<T> implements ResultChecker {
		@Override
		public void check(String name, Object expectedResult, Object result) {
			@SuppressWarnings("unchecked")
			Collection<T> c1 = (Collection<T>) expectedResult;
			Collection<T> c2 = new ArrayList<T>();
			@SuppressWarnings("unchecked")
			Cursor<T> cursor = (Cursor<T>)result;
			
			while (cursor.hasNext()) {
				c2.add(cursor.next());
			}

			Iterator<?> i1 = c1.iterator();
			Iterator<?> i2 = c1.iterator();
			for (int i = 0; i < c1.size(); i++) {
				Object o1 = (Object) i1.next();
				if (o1 instanceof byte[]) {
					assertArrayEquals(name, (byte[]) o1, (byte[]) i2.next());
				} else {
					assertEquals(name, o1, i2.next());
				}
			}
		}
	}
	
	public static class In implements ResultChecker {
		@Override
		public void check(String name, Object expectedResult, Object result) {
			Collection<?> c = (Collection<?>) expectedResult;
			Iterator<?> i = c.iterator();
			boolean find = false;
			while (i.hasNext()) {
				Object o1 = i.next();
				if (result instanceof byte[]) {
					if (Arrays.equals((byte[]) result, (byte[]) o1)) {
						find = true;
						break;
					}
				} else if (result instanceof Collection) {
					Collection<?> c2 = (Collection<?>) result;
					if (c2.contains(o1)) {
						find = true;
						break;
					}
				} else {
					if (result.equals(o1)) {
						find = true;
						break;
					}
				}
			}
			assertTrue(name + " <" + result + "> not found", find);
		}
	}
	
	public static class NotNull implements ResultChecker {
		@Override
		public void check(String name, Object expectedResult, Object result) {
			assertNotNull(name, result);
		}
	}
	
	public static class Less implements ResultChecker {
		@Override
		public void check(String name, Object expectedResult, Object result) {
			assertEquals(name, result.getClass(), Long.class);
			assertTrue(name, (Long) expectedResult < (Long) result);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static Class[] p(Class... params) {
		return params;
	}

	public static Object[] a(Object... args) {
		return args;
	}
	
	public static ResultChecker EQ;
	public static ResultChecker LT;
	public static ResultChecker IN;
	public static ResultChecker NN;
	static {
		EQ = new Equal();
		LT = new Less();
		IN = new In();
		NN = new NotNull();
	}
}
