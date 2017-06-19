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

package com.navercorp.nbasearc.gcp;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HashedTimingWheel {
    private static final Logger log = LoggerFactory.getLogger(HashedTimingWheel.class);

    private final long wheelSize;
    private final List<Set<TimerCallback>> wheel;

    private long tick = 0;

    HashedTimingWheel(int wheelSize, int initialCapacity) {
        this.wheelSize = wheelSize;

        wheel = new ArrayList<Set<TimerCallback>>(wheelSize);
        for (int i = 0; i < wheelSize; i++) {
            wheel.add(new HashSet<TimerCallback>(initialCapacity));
        }
    }

    void add(TimerCallback tc) {
        int idx = wheelIndex(tc.getTimerTimestamp());
        wheel.get(idx).add(tc);
    }

    void del(TimerCallback tc) {
        int idx = wheelIndex(tc.getTimerTimestamp());
        wheel.get(idx).remove(tc);
    }

    void processLines(final long currTs) {
        try {
            long currTick = tick;
            long targetTick = wheelTick(currTs);
            long numTicks = Math.min(targetTick - currTick, wheelSize);

            for (int i = 0; i < numTicks; i++, currTick++) {
                int idx = (int) (currTick % wheelSize);
                processLine(currTs, idx);
            }

            tick = targetTick;
        } catch (Exception e) {
            log.error("Process timer fail.", e);
        }
    }

    private void processLine(final long currTs, int idx) {
        final Set<TimerCallback> timeoutCallbacks = wheel.get(idx);
        Iterator<TimerCallback> iterator = timeoutCallbacks.iterator();
        while (iterator.hasNext()) {
            TimerCallback tc = iterator.next();
            if (tc.getTimerTimestamp() <= currTs) {
                iterator.remove();
                tc.onTimer();
            }
        }
    }

    private long wheelTick(long ts) {
        return ts / (1000 / wheelSize);
    }

    private int wheelIndex(long ts) {
        return (int) (wheelTick(ts) % wheelSize);
    }
}
