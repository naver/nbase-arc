/**
 * Copyright (c) 2011 Jonathan Leibiusky
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.navercorp.redis.cluster.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ByteHashMap<V> implements Map<byte[], V>, Cloneable, Serializable {

    private static final long serialVersionUID = -6971431362627219416L;
    private Map<ByteArrayWrapper, V> internalMap = new HashMap<ByteArrayWrapper, V>();

    @Override
    public int size() {
        return this.internalMap.size();
    }

    @Override
    public boolean isEmpty() {
        return this.internalMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        if (key instanceof byte[])
            return internalMap.containsKey(new ByteArrayWrapper((byte[]) key));
        return internalMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return internalMap.containsValue(value);
    }

    @Override
    public V get(Object key) {
        if (key instanceof byte[])
            return internalMap.get(new ByteArrayWrapper((byte[]) key));
        return internalMap.get(key);
    }

    @Override
    public V put(byte[] key, V value) {
        return internalMap.put(new ByteArrayWrapper(key), value);
    }

    @Override
    public V remove(Object key) {
        if (key instanceof byte[])
            return internalMap.remove(new ByteArrayWrapper((byte[]) key));
        return internalMap.remove(key);
    }

    @Override
    public void putAll(Map<? extends byte[], ? extends V> m) {
        Iterator<?> iterator = m.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<? extends byte[], ? extends V> next = (Entry<? extends byte[], ? extends V>) iterator.next();
            internalMap.put(new ByteArrayWrapper(next.getKey()), next.getValue());
        }
    }

    @Override
    public void clear() {
        internalMap.clear();
    }

    @Override
    public Set<byte[]> keySet() {
        Set<byte[]> keySet = new HashSet<byte[]>();
        Iterator<ByteArrayWrapper> iterator = internalMap.keySet().iterator();
        while (iterator.hasNext()) {
            keySet.add(iterator.next().data);
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        return internalMap.values();
    }

    @Override
    public Set<java.util.Map.Entry<byte[], V>> entrySet() {
        Iterator<java.util.Map.Entry<ByteArrayWrapper, V>> iterator = internalMap.entrySet().iterator();
        HashSet<Entry<byte[], V>> hashSet = new HashSet<java.util.Map.Entry<byte[], V>>();
        while (iterator.hasNext()) {
            Entry<ByteArrayWrapper, V> entry = iterator.next();
            hashSet.add(new ByteEntry(entry.getKey().data, entry.getValue()));
        }
        return hashSet;
    }

    private static final class ByteArrayWrapper {
        private final byte[] data;

        public ByteArrayWrapper(byte[] data) {
            if (data == null) {
                throw new NullPointerException();
            }
            this.data = data;
        }

        public boolean equals(Object other) {
            if (!(other instanceof ByteArrayWrapper)) {
                return false;
            }
            return Arrays.equals(data, ((ByteArrayWrapper) other).data);
        }

        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }

    private static final class ByteEntry<V> implements Entry<byte[], V> {
        private V value;
        private byte[] key;

        public ByteEntry(byte[] key, V value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() {
            return this.key;
        }

        public V getValue() {
            return this.value;
        }

        public V setValue(V value) {
            this.value = value;
            return value;
        }

    }
}