package com.navercorp.nbasearc.confmaster.io;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.MalformedInputException;
import java.util.ArrayList;
import java.util.List;

import com.navercorp.nbasearc.confmaster.logger.Logger;

public class LineReader {
    
    private StringBuilder stringBuffer;
    private CharsetDecoder decoder;

    public LineReader(CharsetDecoder decoder) {
        this.decoder = decoder.reset();
        this.stringBuffer = new StringBuilder();
    }

    public String[] readLines(ByteBuffer buffer) {
        try {
            List<String> lines = new ArrayList<String>();
            String line;
            while (true) {
                line = readLine(buffer);
                if (line == null) {
                    break;
                }
                
                lines.add(line);
            }
            
            return lines.toArray(new String[lines.size()]);
        } finally {
            buffer.clear();
        }
    }
    
    public String readLine(ByteBuffer buffer) {
        try {
            stringBuffer.append(decoder.decode(buffer));
        } catch (MalformedInputException e) {
            Logger.error("Exception occur.", e);
            buffer.clear();
        } catch (CharacterCodingException e) {
            throw new AssertionError(e);
        }

        // (line feed, LF, '\n', 0x0A)
        // (carriage return, CR, '\r', 0x0D)
        
        int newLine;
        int lineFeed;
        newLine = lineFeed = stringBuffer.indexOf("\n");
        if (lineFeed != -1) {
            if ((lineFeed != 0) && (stringBuffer.charAt(lineFeed - 1) == '\r')) {
                newLine -= 1;
            }
            
            String line = stringBuffer.substring(0, newLine);
            stringBuffer.delete(0, lineFeed + 1);
            return line;
        } else {
            int carriageReturn = stringBuffer.indexOf("\r");
            if (carriageReturn != -1) {
                String line = stringBuffer.substring(0, carriageReturn);
                stringBuffer.delete(0, carriageReturn + 1);
                return line;
            }
        }
        
        return null;
    }
    
    public int length() {
        return stringBuffer.length();
    }
    
    public String subString(int start, int end) {
        return stringBuffer.substring(start, end);
    }
    
}
