package com.navercorp.nbasearc.confmaster.logger;

import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.ERROR;
import static org.apache.log4j.Level.INFO;
import static org.apache.log4j.Level.WARN;

import org.apache.log4j.Level;
import org.slf4j.helpers.MessageFormatter;

import com.navercorp.nbasearc.confmaster.ThreadLocalVariableHolder;
import com.navercorp.nbasearc.confmaster.context.ContextType;

public class Logger {
    
    public static void setContextType(ContextType type) {
        ThreadLocalVariableHolder.getLogHistoryHolder().setContextType(type);
    }

    public static void error(String msg) {        
        ThreadLocalVariableHolder.getLogHistoryHolder().log(ERROR, msg);
    }

    public static void error(String format, Object arg1) {
        String msgStr = MessageFormatter.format(format, arg1);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(ERROR, msgStr);
    }

    public static void error(String format, Object arg1, Object arg2) {
        String msgStr = MessageFormatter.format(format, arg1, arg2);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(ERROR, msgStr);
    }

    public static void error(String format, Object[] argArray) {
        String msgStr = MessageFormatter.arrayFormat(format, argArray);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(ERROR, msgStr);
    }
    
    public static void error(String msg, Throwable e) {
        ThreadLocalVariableHolder.getLogHistoryHolder().log(ERROR, msg, e);
    }

    public static void error(String format, Object arg1, Throwable e) {
        String msgStr = MessageFormatter.format(format, arg1);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(ERROR, msgStr, e);
    }

    public static void error(String format, Object arg1, Object arg2, Throwable e) {
        String msgStr = MessageFormatter.format(format, arg1, arg2);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(ERROR, msgStr, e);
    }

    public static void error(String format, Object[] argArray, Throwable e) {
        String msgStr = MessageFormatter.arrayFormat(format, argArray);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(ERROR, msgStr, e);
    }

    public static void warn(String msg) {        
        ThreadLocalVariableHolder.getLogHistoryHolder().log(WARN, msg);
    }

    public static void warn(String format, Object arg1) {
        String msgStr = MessageFormatter.format(format, arg1);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(WARN, msgStr);
    }

    public static void warn(String format, Object arg1, Object arg2) {
        String msgStr = MessageFormatter.format(format, arg1, arg2);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(WARN, msgStr);
    }

    public static void warn(String format, Object[] argArray) {
        String msgStr = MessageFormatter.arrayFormat(format, argArray);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(WARN, msgStr);
    }
    
    public static void warn(String msg, Throwable e) {
        ThreadLocalVariableHolder.getLogHistoryHolder().log(WARN, msg, e);
    }

    public static void warn(String format, Object arg1, Throwable e) {
        String msgStr = MessageFormatter.format(format, arg1);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(WARN, msgStr, e);
    }

    public static void warn(String format, Object arg1, Object arg2, Throwable e) {
        String msgStr = MessageFormatter.format(format, arg1, arg2);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(WARN, msgStr, e);
    }

    public static void warn(String format, Object[] argArray, Throwable e) {
        String msgStr = MessageFormatter.arrayFormat(format, argArray);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(WARN, msgStr, e);
    }

    public static void info(String msg) {        
        ThreadLocalVariableHolder.getLogHistoryHolder().log(INFO, msg);
    }

    public static void info(String format, Object arg1) {
        String msgStr = MessageFormatter.format(format, arg1);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(INFO, msgStr);
    }

    public static void info(String format, Object arg1, Object arg2) {
        String msgStr = MessageFormatter.format(format, arg1, arg2);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(INFO, msgStr);
    }

    public static void info(String format, Object[] argArray) {
        String msgStr = MessageFormatter.arrayFormat(format, argArray);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(INFO, msgStr);
    }
    
    public static void info(String msg, Throwable e) {
        ThreadLocalVariableHolder.getLogHistoryHolder().log(INFO, msg, e);
    }

    public static void info(String format, Object arg1, Throwable e) {
        String msgStr = MessageFormatter.format(format, arg1);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(INFO, msgStr, e);
    }

    public static void info(String format, Object arg1, Object arg2, Throwable e) {
        String msgStr = MessageFormatter.format(format, arg1, arg2);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(INFO, msgStr, e);
    }

    public static void info(String format, Object[] argArray, Throwable e) {
        String msgStr = MessageFormatter.arrayFormat(format, argArray);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(INFO, msgStr, e);
    }

    public static void debug(String msg) {        
        ThreadLocalVariableHolder.getLogHistoryHolder().log(DEBUG, msg);
    }

    public static void debug(String format, Object arg1) {
        String msgStr = MessageFormatter.format(format, arg1);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(DEBUG, msgStr);
    }

    public static void debug(String format, Object arg1, Object arg2) {
        String msgStr = MessageFormatter.format(format, arg1, arg2);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(DEBUG, msgStr);
    }

    public static void debug(String format, Object[] argArray) {
        String msgStr = MessageFormatter.arrayFormat(format, argArray);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(DEBUG, msgStr);
    }
    
    public static void debug(String msg, Throwable e) {
        ThreadLocalVariableHolder.getLogHistoryHolder().log(DEBUG, msg, e);
    }

    public static void debug(String format, Object arg1, Throwable e) {
        String msgStr = MessageFormatter.format(format, arg1);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(DEBUG, msgStr, e);
    }

    public static void debug(String format, Object arg1, Object arg2, Throwable e) {
        String msgStr = MessageFormatter.format(format, arg1, arg2);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(DEBUG, msgStr, e);
    }

    public static void debug(String format, Object[] argArray, Throwable e) {
        String msgStr = MessageFormatter.arrayFormat(format, argArray);
        ThreadLocalVariableHolder.getLogHistoryHolder().log(DEBUG, msgStr, e);
    }

    public static void flush(Level level) {
        ThreadLocalVariableHolder.getLogHistoryHolder().flush(level);
    }

    public static LogHistory popLogHistory() {
        return ThreadLocalVariableHolder.getLogHistoryHolder().popLogHistory();
    }
    
    public static LogHistory getLogHistory() {
        return ThreadLocalVariableHolder.getLogHistoryHolder().getLogHistory();
    }

    public static void setLogHistory(LogHistory logHistory) {
        ThreadLocalVariableHolder.getLogHistoryHolder().pushLogHistory(logHistory);
    }

    public static void log(LogHistory prevLogHistory) {
        ThreadLocalVariableHolder.getLogHistoryHolder().log(prevLogHistory);
    }
    
}
