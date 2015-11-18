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

package com.navercorp.redis.cluster.spring;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author seongminwoo
 */
public class DefaultTypedTupleTest {

    /**
     * Test method for {@link DefaultTypedTuple#compareTo(java.lang.Double)}.
     */
    @Test
    public void testCompareTo() {
        DefaultTypedTuple<String> test = new DefaultTypedTuple<String>("test1", 1.0);
        assertTrue(test.compareTo(1.0) == 0);
    }

}
