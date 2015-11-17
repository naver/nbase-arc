package com.navercorp.nbasearc.confmaster.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobResult {

    private Map<Object, Object> values = new HashMap<Object, Object>();
    private List<Throwable> exceptions = new ArrayList<Throwable>();
    private List<String> messages = new ArrayList<String>();
    
    public void addException(Throwable e) {
        exceptions.add(e);
    }
    
    public List<Throwable> getExceptions() {
        return exceptions;
    }
    
    public void addMessage(String message) {
        messages.add(message);
    }
    
    public List<String> getMessages() {
        return messages;
    }
    
    public Object getValue(Object key) {
        return values.get(key);
    }
    
    public void putValue(final Object key, final Object value) {
        values.put(key, value);
    }
    
    public enum CommonKey {
        STATE(2),
        REQUEST(3),
        START_TIME(4),
        END_TIME(5),
        USAGE(6);
        
        private int value;
        
        private CommonKey(int value) {
            this.value = value;
        }
        
        @Override
        public String toString() {
            switch (this) {
            case STATE:
                return "STATE";
            case REQUEST:
                return "REQUEST";
            case START_TIME:
                return "START_TIME";
            case END_TIME:
                return "END_TIME";
            case USAGE:
                return "USAGE";
            default:
                return "UNKOWN";
            }
        }
    }    

}
