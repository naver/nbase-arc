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

package com.navercorp.redis.cluster.pipeline;

import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.util.JedisByteHashMap;
import redis.clients.util.SafeEncoder;

import java.util.*;

import com.navercorp.redis.cluster.util.ByteHashMap;

/**
 * @author jaehong.kim
 */
public class BuilderFactory {
    public static final Builder<Double> DOUBLE = new Builder<Double>() {
        public Double build(Object data) {
            String asString = STRING.build(data);
            return asString == null ? null : Double.valueOf(asString);
        }

        public String toString() {
            return "double";
        }
    };

    public static final Builder<Boolean> BOOLEAN = new Builder<Boolean>() {
        public Boolean build(Object data) {
            return ((Long) data) == 1;
        }

        public String toString() {
            return "boolean";
        }
    };

    public static final Builder<Long> LONG = new Builder<Long>() {
        public Long build(Object data) {
            return (Long) data;
        }

        public String toString() {
            return "long";
        }

    };
    public static final Builder<String> STRING = new Builder<String>() {
        public String build(Object data) {
            return data == null ? null : SafeEncoder.encode((byte[]) data);
        }

        public String toString() {
            return "string";
        }

    };
    public static final Builder<List<String>> STRING_LIST = new Builder<List<String>>() {
        @SuppressWarnings("unchecked")
        public List<String> build(Object data) {
            if (null == data) {
                return null;
            }
            List<byte[]> l = (List<byte[]>) data;
            final ArrayList<String> result = new ArrayList<String>(l.size());
            for (final byte[] barray : l) {
                if (barray == null) {
                    result.add(null);
                } else {
                    result.add(SafeEncoder.encode(barray));
                }
            }
            return result;
        }

        public String toString() {
            return "List<String>";
        }

    };

    public static final Builder<List<String>> STRING_LIST_NOTNULL = new Builder<List<String>>() {
        public List<String> build(Object data) {
            final List<String> result = STRING_LIST.build(data);
            if (result == null) {
                return new ArrayList<String>();
            }
            return result;
        }

        public String toString() {
            return "List<String>";
        }
    };

    public static final Builder<Map<String, String>> STRING_MAP = new Builder<Map<String, String>>() {
        @SuppressWarnings("unchecked")
        public Map<String, String> build(Object data) {
            final List<byte[]> flatHash = (List<byte[]>) data;
            final Map<String, String> hash = new HashMap<String, String>();
            final Iterator<byte[]> iterator = flatHash.iterator();
            while (iterator.hasNext()) {
                hash.put(SafeEncoder.encode(iterator.next()), SafeEncoder.encode(iterator.next()));
            }

            return hash;
        }

        public String toString() {
            return "Map<String, String>";
        }

    };
    public static final Builder<Set<String>> STRING_SET = new Builder<Set<String>>() {
        @SuppressWarnings("unchecked")
        public Set<String> build(Object data) {
            if (null == data) {
                return null;
            }
            List<byte[]> l = (List<byte[]>) data;
            final Set<String> result = new HashSet<String>(l.size());
            for (final byte[] barray : l) {
                if (barray == null) {
                    result.add(null);
                } else {
                    result.add(SafeEncoder.encode(barray));
                }
            }
            return result;
        }

        public String toString() {
            return "Set<String>";
        }

    };

    public static final Builder<Set<String>> STRING_SET_NOTNULL = new Builder<Set<String>>() {
        public Set<String> build(Object data) {
            final Set<String> result = STRING_SET.build(data);
            if (null == result) {
                return new HashSet<String>();
            }
            return result;
        }

        public String toString() {
            return "Set<String>";
        }

    };

    public static final Builder<Set<String>> STRING_ZSET = new Builder<Set<String>>() {
        @SuppressWarnings("unchecked")
        public Set<String> build(Object data) {
            if (null == data) {
                return null;
            }
            List<byte[]> l = (List<byte[]>) data;
            final Set<String> result = new LinkedHashSet<String>(l.size());
            for (final byte[] barray : l) {
                if (barray == null) {
                    result.add(null);
                } else {
                    result.add(SafeEncoder.encode(barray));
                }
            }
            return result;
        }

        public String toString() {
            return "ZSet<String>";
        }

    };

    @SuppressWarnings("unchecked")
    public static final Builder<Map<String, List<String>>> STRING_LIST_MAP = new Builder<Map<String, List<String>>>() {
        public Map<String, List<String>> build(Object data) {
            List<byte[]> values = null;
            if (data == null) {
                values = new ArrayList<byte[]>();
            } else {
                values = (List<byte[]>) data;
            }

            final Map<String, List<String>> result = new HashMap<String, List<String>>();
            final Iterator<byte[]> iterator = values.iterator();
            if (iterator == null) {
                return result;
            }

            while (iterator.hasNext()) {
                final String name = SafeEncoder.encode(iterator.next());
                List<String> list = result.get(name);
                if (list == null) {
                    list = new ArrayList<String>();
                }

                if (iterator.hasNext()) {
                    final byte[] rawValue = iterator.next();
                    if (rawValue != null) {
                        final String value = SafeEncoder.encode(rawValue);
                        list.add(value);
                    }
                }
                result.put(name, list);
            }

            return result;
        }

        public String toString() {
            return "Map<String, List<String>>";
        }
    };

    @SuppressWarnings("unchecked")
    public static final Builder<Map<String, Set<String>>> STRING_SET_MAP = new Builder<Map<String, Set<String>>>() {
        public Map<String, Set<String>> build(Object data) {
            List<byte[]> values = null;
            if (data == null) {
                values = new ArrayList<byte[]>();
            } else {
                values = (List<byte[]>) data;
            }

            final Map<String, Set<String>> result = new HashMap<String, Set<String>>();
            final Iterator<byte[]> iterator = values.iterator();
            if (iterator == null) {
                return result;
            }

            while (iterator.hasNext()) {
                final String name = SafeEncoder.encode(iterator.next());
                Set<String> list = result.get(name);
                if (list == null) {
                    list = new HashSet<String>();
                }

                if (iterator.hasNext()) {
                    final byte[] rawValue = iterator.next();
                    if (rawValue != null) {
                        final String value = SafeEncoder.encode(rawValue);
                        list.add(value);
                    }
                }
                result.put(name, list);
            }

            return result;
        }

        public String toString() {
            return "Map<String, Set<String>>";
        }
    };

    public static final Builder<byte[]> BYTE_ARRAY = new Builder<byte[]>() {
        public byte[] build(Object data) {
            return ((byte[]) data); // deleted == 1
        }

        public String toString() {
            return "byte[]";
        }
    };

    public static final Builder<List<byte[]>> BYTE_ARRAY_LIST = new Builder<List<byte[]>>() {
        @SuppressWarnings("unchecked")
        public List<byte[]> build(Object data) {
            if (null == data) {
                return null;
            }
            List<byte[]> l = (List<byte[]>) data;

            return l;
        }

        public String toString() {
            return "List<byte[]>";
        }
    };

    public static final Builder<List<byte[]>> BYTE_ARRAY_LIST_NOTNULL = new Builder<List<byte[]>>() {
        public List<byte[]> build(Object data) {
            final List<byte[]> result = BYTE_ARRAY_LIST.build(data);
            if (null == result) {
                return new ArrayList<byte[]>();
            }

            return result;
        }

        public String toString() {
            return "List<byte[]>";
        }
    };

    @SuppressWarnings("unchecked")
    public static final Builder<Set<byte[]>> BYTE_ARRAY_ZSET = new Builder<Set<byte[]>>() {
        public Set<byte[]> build(Object data) {
            if (null == data) {
                return null;
            }
            List<byte[]> l = (List<byte[]>) data;
            final Set<byte[]> result = new LinkedHashSet<byte[]>(l);
            return result;
        }

        public String toString() {
            return "ZSet<byte[]>";
        }
    };

    public static final Builder<Set<byte[]>> BYTE_ARRAY_ZSET_NOTNULL = new Builder<Set<byte[]>>() {
        public Set<byte[]> build(Object data) {
            final Set<byte[]> result = BYTE_ARRAY_ZSET.build(data);
            if (null == result) {
                return new HashSet<byte[]>();
            }
            return result;
        }

        public String toString() {
            return "ZSet<byte[]>";
        }
    };

    @SuppressWarnings("unchecked")
    public static final Builder<Map<byte[], byte[]>> BYTE_ARRAY_MAP = new Builder<Map<byte[], byte[]>>() {
        public Map<byte[], byte[]> build(Object data) {
            final List<byte[]> flatHash = (List<byte[]>) data;
            final Map<byte[], byte[]> hash = new JedisByteHashMap();
            final Iterator<byte[]> iterator = flatHash.iterator();
            while (iterator.hasNext()) {
                hash.put(iterator.next(), iterator.next());
            }

            return hash;
        }

        public String toString() {
            return "Map<byte[], byte[]>";
        }
    };

    public static final Builder<Map<byte[], List<byte[]>>> BYTE_LIST_MAP = new Builder<Map<byte[], List<byte[]>>>() {
        public Map<byte[], List<byte[]>> build(Object data) {
            List<byte[]> values = null;
            if (data == null) {
                values = new ArrayList<byte[]>();
            } else {
                values = (List<byte[]>) data;
            }

            final Map<byte[], List<byte[]>> result = new ByteHashMap<List<byte[]>>();
            final Iterator<byte[]> iterator = values.iterator();
            if (iterator == null) {
                return result;
            }

            while (iterator.hasNext()) {
                final byte[] name = iterator.next();
                List<byte[]> list = result.get(name);
                if (list == null) {
                    list = new ArrayList<byte[]>();
                }

                if (iterator.hasNext()) {
                    final byte[] value = iterator.next();
                    if (value != null) {
                        list.add(value);
                    }
                }
                result.put(name, list);
            }

            return result;
        }

        public String toString() {
            return "Map<byte[], List<byte[]>>";
        }
    };

    public static final Builder<Map<byte[], Set<byte[]>>> BYTE_SET_MAP = new Builder<Map<byte[], Set<byte[]>>>() {
        public Map<byte[], Set<byte[]>> build(Object data) {
            List<byte[]> values = null;
            if (data == null) {
                values = new ArrayList<byte[]>();
            } else {
                values = (List<byte[]>) data;
            }

            final Map<byte[], Set<byte[]>> result = new ByteHashMap<Set<byte[]>>();
            final Iterator<byte[]> iterator = values.iterator();
            if (iterator == null) {
                return result;
            }

            while (iterator.hasNext()) {
                final byte[] name = iterator.next();
                Set<byte[]> set = result.get(name);
                if (set == null) {
                    set = new HashSet<byte[]>();
                }
                if (iterator.hasNext()) {
                    final byte[] value = iterator.next();
                    if (value != null) {
                        set.add(value);
                    }
                }
                result.put(name, set);
            }

            return result;
        }

        public String toString() {
            return "Map<byte[], List<byte[]>>";
        }
    };

    public static final Builder<Set<Tuple>> TUPLE_ZSET = new Builder<Set<Tuple>>() {
        @SuppressWarnings("unchecked")
        public Set<Tuple> build(Object data) {
            if (null == data) {
                return null;
            }
            List<byte[]> l = (List<byte[]>) data;
            final Set<Tuple> result = new LinkedHashSet<Tuple>(l.size());
            Iterator<byte[]> iterator = l.iterator();
            while (iterator.hasNext()) {
                result.add(new Tuple(SafeEncoder.encode(iterator.next()), Double.valueOf(SafeEncoder.encode(iterator.next()))));
            }
            return result;
        }

        public String toString() {
            return "ZSet<Tuple>";
        }

    };

    public static final Builder<Set<Tuple>> TUPLE_ZSET_BINARY = new Builder<Set<Tuple>>() {
        @SuppressWarnings("unchecked")
        public Set<Tuple> build(Object data) {
            if (null == data) {
                return null;
            }
            List<byte[]> l = (List<byte[]>) data;
            final Set<Tuple> result = new LinkedHashSet<Tuple>(l.size());
            Iterator<byte[]> iterator = l.iterator();
            while (iterator.hasNext()) {
                result.add(new Tuple(iterator.next(), Double.valueOf(SafeEncoder.encode(iterator.next()))));
            }

            return result;
        }

        public String toString() {
            return "ZSet<Tuple>";
        }
    };

    public static final Builder<ScanResult<Map.Entry<String, String>>> MAP_SCAN_RESULT = new Builder<ScanResult<Map.Entry<String, String>>>() {
        public ScanResult<Map.Entry<String, String>> build(Object data) {
            if (null == data) {
                return null;
            }

            final List<Object> result = (List<Object>) data;
            final String newCursor = new String((byte[]) result.get(0));
            final List<Map.Entry<String, String>> results = new ArrayList<Map.Entry<String, String>>();
            final List<byte[]> rawResults = (List<byte[]>) result.get(1);
            final Iterator<byte[]> iterator = rawResults.iterator();
            while (iterator.hasNext()) {
                results.add(new AbstractMap.SimpleEntry<String, String>(SafeEncoder.encode(iterator.next()), SafeEncoder.encode(iterator.next())));
            }

            return new ScanResult<Map.Entry<String, String>>(newCursor, results);
        }

        public String toString() {
            return "ScanResult<Map.ENTRY<String, String>>";
        }
    };

    public static final Builder<ScanResult<Map.Entry<byte[], byte[]>>> MAP_SCAN_RESULT_BINARY = new Builder<ScanResult<Map.Entry<byte[], byte[]>>>() {
        public ScanResult<Map.Entry<byte[], byte[]>> build(Object data) {
            if (null == data) {
                return null;
            }

            final List<Object> result = (List<Object>) data;
            final String newCursor = new String((byte[]) result.get(0));
            final List<Map.Entry<byte[], byte[]>> results = new ArrayList<Map.Entry<byte[], byte[]>>();
            final List<byte[]> rawResults = (List<byte[]>) result.get(1);
            final Iterator<byte[]> iterator = rawResults.iterator();
            while (iterator.hasNext()) {
                results.add(new AbstractMap.SimpleEntry<byte[], byte[]>(iterator.next(), iterator.next()));
            }

            return new ScanResult<Map.Entry<byte[], byte[]>>(newCursor, results);
        }

        public String toString() {
            return "ScanResult<Map.Entry<byte[], byte[]>>";
        }
    };

    public static final Builder<ScanResult<String>> SCAN_RESULT = new Builder<ScanResult<String>>() {
        public ScanResult<String> build(Object data) {
            if (null == data) {
                return null;
            }
            final List<Object> result = (List<Object>) data;
            final String newCursor = new String((byte[]) result.get(0));
            final List<String> results = new ArrayList<String>();
            final List<byte[]> rawResults = (List<byte[]>) result.get(1);
            for (byte[] bs : rawResults) {
                results.add(SafeEncoder.encode(bs));
            }

            return new ScanResult<String>(newCursor, results);
        }

        public String toString() {
            return "ScanResult<String>";
        }
    };

    public static final Builder<ScanResult<byte[]>> SCAN_RESULT_BINARY = new Builder<ScanResult<byte[]>>() {
        public ScanResult<byte[]> build(Object data) {
            if (null == data) {
                return null;
            }
            final List<Object> result = (List<Object>) data;
            final String newCursor = new String((byte[]) result.get(0));
            final List<byte[]> results = new ArrayList<byte[]>();
            final List<byte[]> rawResults = (List<byte[]>) result.get(1);
            for (byte[] bs : rawResults) {
                results.add(bs);
            }

            return new ScanResult<byte[]>(newCursor, results);
        }

        public String toString() {
            return "ScanResult<byte[]>";
        }
    };

    public static final Builder<ScanResult<Tuple>> TUPLE_SCAN_RESULT = new Builder<ScanResult<Tuple>>() {
        public ScanResult<Tuple> build(Object data) {
            if (null == data) {
                return null;
            }
            final List<Object> result = (List<Object>) data;
            final String newCursor = new String((byte[]) result.get(0));
            final List<Tuple> results = new ArrayList<Tuple>();
            final List<byte[]> rawResults = (List<byte[]>) result.get(1);
            final Iterator<byte[]> iterator = rawResults.iterator();
            while (iterator.hasNext()) {
                results.add(new Tuple(SafeEncoder.encode(iterator.next()), Double.valueOf(SafeEncoder.encode(iterator.next()))));
            }

            return new ScanResult<Tuple>(newCursor, results);
        }

        public String toString() {
            return "ScanResult<Tuple>";
        }

    };

    public static final Builder<ScanResult<Tuple>> TUPLE_SCAN_RESULT_BINARY = new Builder<ScanResult<Tuple>>() {
        public ScanResult<Tuple> build(Object data) {
            if (null == data) {
                return null;
            }
            final List<Object> result = (List<Object>) data;
            final String newCursor = new String((byte[]) result.get(0));
            final List<Tuple> results = new ArrayList<Tuple>();
            final List<byte[]> rawResults = (List<byte[]>) result.get(1);
            final Iterator<byte[]> iterator = rawResults.iterator();
            while (iterator.hasNext()) {
                results.add(new Tuple(iterator.next(), Double.valueOf(SafeEncoder.encode(iterator.next()))));
            }

            return new ScanResult<Tuple>(newCursor, results);
        }

        public String toString() {
            return "ScanResult<Tuple>";
        }
    };
}