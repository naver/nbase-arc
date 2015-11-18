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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.redis.cluster.util.TestEnvUtils;

/**
 * @author seongminwoo
 */
public class ZookeeperConTest implements Watcher {
    private final Logger log = LoggerFactory.getLogger(ZookeeperConTest.class);
    private ZooKeeper zk;
    CountDownLatch connLatcher;

    @Ignore
    @Test
    public void test() throws IOException, InterruptedException, KeeperException {
        connLatcher = new CountDownLatch(1);
        zk = new ZooKeeper(TestEnvUtils.getZkAddress(), 60 * 1000, this);
        connLatcher.await(1, TimeUnit.SECONDS);
        final byte[] value = zk.getData("/zookeeper", this, null);
        log.debug("value : " + new String(value));
        zk.close();
    }

    /**
     * @param event
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     */
    public void process(WatchedEvent event) {
        log.info("[NodeWatcher] Event {type=" + event.getType() + ", state=" + event.getState() + ", path="
                + event.getPath() + "}");

        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    log.debug("SyncConnected");
                    this.connLatcher.countDown();
                    break;
                default:

            }
        }
    }
}
