/*
 * @(#)Pool.java $version 2013. 8. 14.
 *
 * Copyright 2007 NHN Corp. All rights Reserved. 
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.navercorp.redis.cluster;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.commons.pool2.PooledObjectFactory;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public abstract class Pool<T> {
    final GenericObjectPool<T> internalPool;

    public Pool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {
        this.internalPool = new GenericObjectPool<T>(factory, poolConfig);
    }

    public T getResource() {
        try {
            return internalPool.borrowObject();
        } catch (Exception e) {
            throw new JedisConnectionException("Could not get a resource from the pool", e);
        }
    }

    public void returnResourceObject(final T resource) {
        try {
            internalPool.returnObject(resource);
        } catch (Exception e) {
            throw new JedisException("Could not return the resource to the pool", e);
        }
    }

    public void returnBrokenResource(final T resource) {
        returnBrokenResourceObject(resource);
    }

    public void returnResource(final T resource) {
        returnResourceObject(resource);
    }

    protected void returnBrokenResourceObject(final T resource) {
        try {
            internalPool.invalidateObject(resource);
        } catch (Exception e) {
            throw new JedisException("Could not return the resource to the pool", e);
        }
    }

    public void destroy() {
        try {
            internalPool.close();
        } catch (Exception e) {
            throw new JedisException("Could not destroy the pool", e);
        }
    }
}