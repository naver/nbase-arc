package com.navercorp.nbasearc.confmaster.io;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.statistics.Statistics;

public class EventSelector {
    
    private Selector selector;
    private Map<Integer, Session> sessions = new ConcurrentHashMap<Integer, Session>();

    private long selectTimeout;
    
    public EventSelector(long selectTimeout) throws IOException {
        this.selectTimeout = selectTimeout;
        
        try {
            selector = Selector.open();
        } catch (IOException e) {
            Logger.error("Initialize selector fail.", e);
            throw e;
        }
    }

    /**
     * Shutdown a selector
     * @throws IOException 
     */
    public void shutdown() throws IOException {
        try {
            selector.close();
        } catch (IOException e) {
            Logger.error("Close nio event selector fail.", e);
            throw e;
        }
    }
    
    public class ElapsedTime {
        final long start;
        final long ioDone;
        final long end;
        
        public ElapsedTime(long start, long ioDone, long end) {
            this.start = start;
            this.ioDone = ioDone;
            this.end = end;
        }
        
        public long getIoTime() {
            return ioDone - start;
        }
        
        public long getTotalTime() {
            return ioDone - end;
        }
        
        public long getStart() {
            return start;
        }
        
        public long getIoDone() {
            return ioDone;
        }
        
        public long getEnd() {
            return end;
        }
    }
    
    /**
     * Main loop of nio 
     * 
     * @return elapsed time
     */
    public ElapsedTime process() {
        try {
            long start = System.currentTimeMillis();
            ioProcess();
            long ioDone = System.currentTimeMillis();
            loopProcess();
            long end = System.currentTimeMillis();

            return new ElapsedTime(start, ioDone, end);
        } catch (Exception e) {
            Logger.error("-ERR error occurs in hbMain", e);
        }

        return null;
    }

    /**
     * Demultiplex IO events 
     */
    public void ioProcess()
    {
        int numberOfKeys;
        try {
            numberOfKeys = selector.select(selectTimeout);
        } catch (IOException e) {
            Logger.error("Selector.select error.", e);
            return;
        }
        if (0 == numberOfKeys) {
            return;
        }
        
        Iterator<SelectionKey> keyIter = selector.selectedKeys().iterator(); 
        while (keyIter.hasNext())
        {
            SelectionKey key = keyIter.next();
            Session session = (Session) key.attachment();
            
            long timeMillis = System.currentTimeMillis();
            
            try {
                if (key.isValid() && key.isReadable()) {
                    long start = System.currentTimeMillis();
                    session.callbackRead(key, timeMillis);
                    long end = System.currentTimeMillis();
                    Statistics.updateNioRead(end - start);
                }
                
                if (key.isValid() && key.isWritable()) {
                    long start = System.currentTimeMillis();
                    session.callbackWrite(key, timeMillis);
                    long end = System.currentTimeMillis();
                    Statistics.updateNioWrite(end - start);
                }
                
                if (key.isValid() && key.isAcceptable()) {
                    session.callbackAccept(key, timeMillis);
                }
                
                if (key.isValid() && key.isConnectable()) {
                    session.callbackConnect(key, timeMillis);
                }
            } catch (CancelledKeyException e) {
                Logger.warn("Closed connection {}", session, e);
            } catch (ClosedChannelException e) {
                Logger.warn("Closed connection {}", session, e);
            } catch (IOException e) {
                Logger.warn("Closed connection {}", session, e);
            } catch (Exception e) {
                Logger.error("Exception occurs on {}", session, e);
            }
            
            keyIter.remove();
        }
    }

    /**
     * to be called on each loop to process idle routines.
     */
    public void loopProcess()
    {
        for (Session session : sessions.values())
        {
            long timeMillis = System.currentTimeMillis();
            try {
                session.callbackOnLoop(timeMillis);
            } catch (Exception e) {
                Logger.error("Exception occurs while callbackOnLoop on {}", session, e);
            }
        }
    }

    /**
     * register session to Selector in order to detect IO events.
     * @param session
     * @param ops interest IO operations, such as SelectionKey.OP_ACCEPT, SelectionKey.OP_CONNECT, SelectionKey.OP_READ, SelectionKey.OP_WRITE 
     * @throws ClosedChannelException 
     */
    public void register(Session session, int ops) throws ClosedChannelException {
        if (sessions.containsKey(session.getID())) {
            throw new IllegalStateException("Altready registered session");
        }
        
        SelectionKey selKey;
        try {
            selKey = session.getChannel().register(selector, ops, session);
        } catch (ClosedChannelException e) {
            Logger.error("Error while register channel to selector. {}", session, e);
            throw e;
        }
        session.setSelectionKey(selKey);
    }

    public Selector getSelector() {
        return selector;
    }
    
    public void addSession(Session session) {
        sessions.put(session.getID(), session);
    }
    
    public Session getSession(int sessionID) {
        return sessions.get(sessionID);
    }
    
    public void removeSession(int sessionID) {
        sessions.remove(sessionID);
    }
    
    public Integer getSessionCount() {
        return sessions.size();
    }
    
    public Collection<Session> getSessions() {
        return sessions.values();
    }
    
}
