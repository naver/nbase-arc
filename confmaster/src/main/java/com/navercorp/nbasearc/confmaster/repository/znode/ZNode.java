/*
 * Copyright 2015 Naver Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.nbasearc.confmaster.repository.znode;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandler;

public abstract class ZNode<T> implements Comparable<ZNode<T>> {

    protected Stat stat;
    protected String path;
    protected String name;
    protected WatchEventHandler watch;

    private NodeType nodeType;
    private TypeReference<T> typeRef;
    private T data;
    
    protected final MemoryObjectMapper mapper = new MemoryObjectMapper();
    protected final ApplicationContext context;    
    
    private final ReentrantReadWriteLock znodeRWLock = new ReentrantReadWriteLock();

    public ZNode(ApplicationContext context) {
        this.context = context;
        this.stat = new Stat();
    }
    
    public void setTypeRef(TypeReference<T> type) {
        this.typeRef = type;
    }
    
    public TypeReference<T> getTypeRef() {
        return this.typeRef;
    }

    public void setPath(String path) {
        this.path = path;
    }
    
    public String getPath() {
        return path;
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
    
    @SuppressWarnings("unchecked")
    public void setData(byte[] raw) {
        setData((T) mapper.readValue(raw, getTypeRef()));
    }

    public Stat getStat() {
        return stat;
    }
    
    public void setStat(Stat stat) {
        this.stat = stat;
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    public WatchEventHandler getWatch() {
        return watch;
    }

    public void setWatch(WatchEventHandler watch) {
        this.watch = watch;
    }

    public Lock readLock() {
        return znodeRWLock.readLock();
    }

    public Lock writeLock() {
        return znodeRWLock.writeLock();
    }
    
    @Override
    public int compareTo(ZNode<T> znode) {
        return name.compareTo(znode.name);
    }
    
    @Override
    public String toString() {
        return "ZNode(path=" + getPath() + ")";
    }

}
