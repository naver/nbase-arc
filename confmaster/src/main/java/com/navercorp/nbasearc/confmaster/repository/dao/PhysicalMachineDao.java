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

package com.navercorp.nbasearc.confmaster.repository.dao;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.znode.PhysicalMachineData;

public interface PhysicalMachineDao {

    boolean pmExist(final String name) throws MgmtZooKeeperException;

    String createPm(final String name, final PhysicalMachineData data)
            throws MgmtZooKeeperException, NodeExistsException;

    void deletePm(final String name) throws MgmtZooKeeperException;

    byte[] loadPm(String name) throws MgmtZooKeeperException, NoNodeException;

}
