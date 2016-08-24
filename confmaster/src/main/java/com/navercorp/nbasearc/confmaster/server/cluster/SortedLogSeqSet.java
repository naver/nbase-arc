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

package com.navercorp.nbasearc.confmaster.server.cluster;

import java.util.Comparator;
import java.util.TreeSet;

@SuppressWarnings("serial")
public class SortedLogSeqSet extends TreeSet<LogSequence> {
    public SortedLogSeqSet() {
        super(new Comparator<LogSequence>() {
            @Override
            public int compare(LogSequence o1, LogSequence o2) {
                if (o2.getMax() > o1.getMax()) {
                    return 1;
                } else if (o2.getMax() < o1.getMax()) {
                    return -1;
                } else {
                    return compareToCommit(o1, o2);
                }
            }

            private int compareToCommit(LogSequence o1, LogSequence o2) {
                if (o2.getLogCommit() > o1.getLogCommit()) {
                    return 1;
                } else if (o2.getLogCommit() < o1.getLogCommit()) {
                    return -1;
                } else {
                    return o2.getPgs().getName()
                            .compareTo(o1.getPgs().getName());
                }
            }
        });
    }

    public int stdCompetitionRank(PartitionGroupServer pgs) {
        int rank = 0;
        int tie = 0;
        long prevScore = -1;
        for (LogSequence ls : this) {
            if (ls.getMax() == prevScore) {
                tie += 1;
            } else {
                rank += tie + 1;
                tie = 0;
            }
            prevScore = ls.getMax();

            if (ls.getPgs() == pgs) {
                return rank;
            }
        }
        return -1;
    }

    public PartitionGroupServer get(int index) {
        int i = 0;
        for (LogSequence ls : this) {
            if (i == index) {
                return ls.getPgs();
            }
            i++;
        }
        return null;
    }

    public LogSequence get(PartitionGroupServer pgs) {
        for (LogSequence ls : this) {
            if (ls.getPgs() == pgs) {
                return ls;
            }
        }
        return null;
    }
}
