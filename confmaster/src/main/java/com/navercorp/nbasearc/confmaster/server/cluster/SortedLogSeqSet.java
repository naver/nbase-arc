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

    public int index(PartitionGroupServer pgs) {
        int pos = 0;
        for (LogSequence ls : this) {
            if (ls.getPgs() == pgs) {
                return pos;
            } else {
                pos++;
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
