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

package com.navercorp.nbasearc.confmaster.repository.lock;

public abstract class HierarchicalLock {

    private HierarchicalLockHelper hlh;
    private LockType lockType;

    public HierarchicalLock() {
    }

    public HierarchicalLock(HierarchicalLockHelper hlh, LockType lockType) {
        this.hlh = hlh;
        this.lockType = lockType;
    }

    protected abstract void _lock();

    public HierarchicalLockHelper getHlh() {
        return hlh;
    }

    public void setHlh(HierarchicalLockHelper hlh) {
        this.hlh = hlh;
    }

    public LockType getLockType() {
        return lockType;
    }

    public void setLockType(LockType lockType) {
        this.lockType = lockType;
    }

}
