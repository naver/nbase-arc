package com.navercorp.nbasearc.confmaster.server.cluster;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

public class PartitionGroupTest {

    @Test
    public void logSequenceOrdering() {
        Map<String, Long> logSeqMap = new HashMap<String, Long>();
        logSeqMap.put("0", 0L);
        logSeqMap.put("1", 10L);
        logSeqMap.put("3", 30L);
        logSeqMap.put("2", 20L);
        
        List<Map.Entry<String, Long>> logDescend = 
                new ArrayList<Map.Entry<String, Long>>(logSeqMap.entrySet());
        Collections.sort(logDescend,
                new Comparator<Map.Entry<String, Long>>() {
                    @Override
                    public int compare(
                            Entry<String, Long> o1,
                            Entry<String, Long> o2) {
                        if (o1.getValue() > o1.getValue()) {
                            return 1;
                        } else if (o1.getValue() < o1.getValue()) {
                            return -1;
                        } else {
                            return 0;
                        }
                    }
                });

        assertEquals("3", logDescend.get(0).getKey());
        assertEquals("2", logDescend.get(1).getKey());
        assertEquals("1", logDescend.get(2).getKey());
        assertEquals("0", logDescend.get(3).getKey());
        
        assertEquals(Long.valueOf(30L), logDescend.get(0).getValue());
        assertEquals(Long.valueOf(20L), logDescend.get(1).getValue());
        assertEquals(Long.valueOf(10L), logDescend.get(2).getValue());
        assertEquals(Long.valueOf(0L), logDescend.get(3).getValue());
    }
    
    public class ValueSortedMap<K extends Comparable, V extends Comparable> extends TreeMap<K, V> {
        @Override
        public Set<Entry<K, V>> entrySet() {
            Set<Entry<K, V>> originalEntries = super.entrySet();
            Set<Entry<K, V>> sortedEntry = new TreeSet<Entry<K, V>>(new Comparator<Entry<K, V>>() {
                @Override
                public int compare(Entry<K, V> entryA, Entry<K, V> entryB) {
                    int compareTo = entryA.getValue().compareTo(entryB.getValue());
                    if(compareTo == 0) {
                        compareTo = entryA.getKey().compareTo(entryB.getKey());
                    }
                    return compareTo;
                }
            });
            sortedEntry.addAll(originalEntries);
            return sortedEntry;
        }

        @Override
        public Collection<V> values() {
            Set<V> sortedValues = new TreeSet<V>(new Comparator<V>(){
                @Override
                public int compare(V vA, V vB) {
                    return vA.compareTo(vB);
                }
            });
            sortedValues.addAll(super.values());
            return sortedValues;
        }
    }

    public class EntryValueComparator implements Comparator {
        public int compare(Object o1, Object o2) {
            return compare((Map.Entry) o1, (Map.Entry) o2);
        }

        public int compare(Map.Entry e1, Map.Entry e2) {
           int cf = ((Comparable)e1.getValue()).compareTo(e2.getValue());
           if (cf == 0) {
              cf = ((Comparable)e1.getKey()).compareTo(e2.getKey());
           }
           return cf;
        }
    }

    public class Student implements Comparable {
        final String name;
        final int no;
        
        public Student(String name, int no) {
            this.name = name;
            this.no = no;
        }

        @Override
        public String toString() {
            return "[" + name + ", " + no + "]"; 
        }

        @Override
        public int compareTo(Object o) {
            if (o == null) {
                return -1;
            }
            if (!(o instanceof Student)) {
                return -1;
            }
            
            Student another = (Student)o;
            return no - another.no;
        }
    }
    
    @Test
    public void sortedMap() {
        Map<Student, String> lhm = new TreeMap<Student, String>();
        
        lhm.put(new Student("1cc", 30), "C");
        lhm.put(new Student("2bb", 20), "B");
        lhm.put(new Student("3aa", 10), "A");
        for (Map.Entry<Student, String> e : lhm.entrySet()) {
            System.out.println(e.getKey() + " " + e.getValue());
        }
        for (String s : lhm.values()) {
            System.out.println(s);
        }
    }
}
