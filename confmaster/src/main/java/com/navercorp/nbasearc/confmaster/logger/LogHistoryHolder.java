package com.navercorp.nbasearc.confmaster.logger;

import static org.apache.log4j.Level.*;

import org.apache.log4j.Level;

import com.navercorp.nbasearc.confmaster.context.ContextType;

public class LogHistoryHolder {

    private LogHistory logHistory = new LogHistory();
    
    public LogHistoryHolder() {
    }

    public LogHistory popLogHistory() {
        try {
            return logHistory;
        } finally {
            logHistory = new LogHistory();
        }
    }
    
    public LogHistory getLogHistory() {
        return logHistory;
    }

    public void pushLogHistory(LogHistory logHistory) {
        this.logHistory = logHistory;
    }

    public void log(Level level, String log) {
        try {
            logHistory.log(level, log);
        } catch (Exception e) {
            if (logHistory == null) {
                logHistory = new LogHistory();
            }
            logHistory.log(ERROR, "Exception occur while logging.", e);
        }
    }

    public void log(Level level, String log, Throwable e) {
        try {
            logHistory.log(level, log, e);
        } catch (Exception exception) {
            if (logHistory == null) {
                logHistory = new LogHistory();
            }
            logHistory.log(ERROR, "Exception occur while logging.", exception);
        }
    }
    
    public void flush(Level level) {
        try {
            logHistory.flush(level);
        } catch (Exception e) {
            if (logHistory == null) {
                logHistory = new LogHistory();
            }
            logHistory.log(ERROR, "Exception occur while flush logs.", e);
        }
    }

    public void log(LogHistory prevLogHistory) {
        logHistory.log(prevLogHistory);
    }

    public void setContextType(ContextType type) {
        logHistory.setContextType(type);
    }

}
