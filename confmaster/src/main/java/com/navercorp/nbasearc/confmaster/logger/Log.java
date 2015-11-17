package com.navercorp.nbasearc.confmaster.logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Level;

public class Log {
    
    private static final DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
    private final Level level;
    private final Date time;
    private final String msg;
    private final Throwable e;

    public Log(Level level, Date time, String msg) {
        this.level = level;
        this.time = time;
        this.msg = msg;
        this.e = null;
    }

    public Log(Level level, Date time, String msg, Throwable e) {
        this.level = level;
        this.time = time;
        this.msg = msg;
        this.e = e;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        sb.append(
            String.format("%s %s %03d %s", 
                df.format(time), getLevel().toString().substring(0, 3), 
                Thread.currentThread().getId(), msg));
        if (e != null) {
            sb.append(" ").append(getStackTrace(e));
        }
        
        return sb.toString();
    }

    public static String getStackTrace(Throwable aThrowable) {
      Writer result = new StringWriter();
      PrintWriter printWriter = new PrintWriter(result);
      aThrowable.printStackTrace(printWriter);
      return result.toString();
    }

    public Level getLevel() {
        return level;
    }
    
    public boolean hasException() {
        return e != null;
    }

}
