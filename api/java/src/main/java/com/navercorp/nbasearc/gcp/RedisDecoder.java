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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author seongjoon.ahn@navercorp.com
 * @Author seunghoo.han@navercorp.com (maintenance)
 *
 */
class RedisDecoder {
    private final static Logger log = LoggerFactory.getLogger(RedisDecoder.class);

    private final byte[] lookasideBuffer;
    private int lookasideBufferReaderIndex;
    private int lookasideBufferLength;
    
    RedisDecoder() {
        // 32 : 20(bytes of a maximum long) + 2(\r\n) + 1($ or *)
        //      and a power of 2
        this.lookasideBuffer = new byte[32];
    }

    void getFrames(ByteBuf in, List<byte[]> out) {
        while (true) {
            in.markReaderIndex();
            final int stx = in.readerIndex();
            
            if (hasFrame(in) == false) {
                in.resetReaderIndex();
                break;
            }
            
            int length = in.readerIndex() - stx;
            byte[] readBytes = new byte[length];

            in.resetReaderIndex();
            in.readBytes(readBytes);
            out.add(readBytes);
        }
    }
    
    private int readableLookasideBufferBytes() {
        return lookasideBufferLength - lookasideBufferReaderIndex;
    }

    private boolean hasFrame(ByteBuf in) {
        if (in.isReadable() == false) {
            return false;
        }
     
        lookasideBufferLength = Math.min(in.readableBytes(), lookasideBuffer.length);
        in.readBytes(lookasideBuffer, 0, lookasideBufferLength);
        lookasideBufferReaderIndex = 0;
        in.readerIndex(in.readerIndex() - lookasideBufferLength);
        
        byte b = lookasideBuffer[lookasideBufferReaderIndex++];
        in.skipBytes(1);
        if (b == MINUS_BYTE) {
            return processError(in);
        } else if (b == ASTERISK_BYTE) {
            return processMultiBulkReply(in);
        } else if (b == COLON_BYTE) {
            return processInteger(in);
        } else if (b == DOLLAR_BYTE) {
            return processBulkReply(in);
        } else if (b == PLUS_BYTE) {
            return processStatusCodeReply(in);
        } else {
            throw new JedisConnectionException("Unknown reply: " + (char) b);
        }
    }

    /**
     * Process error.
     *
     * @param is the is
     */
    private boolean processError(ByteBuf in) {
        int length = lineLength(in);
        if (length == 0) {
            return false;
        }
        
        in.skipBytes(length);
        return true;
    }
    
    /**
     * Process multi bulk reply.
     *
     * @param is the is
     * @return the list
     */
    private boolean processMultiBulkReply(ByteBuf in) {
        final int length = lineLength();
        if (length == 0) {
            return false;
        }
        in.skipBytes(length);
        
        int num = readInt(length - 2);
        if (num == -1) {
            return true;
        }
        
        for (int i = 0; i < num; i++) {
            if (hasFrame(in) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Process integer.
     *
     * @param is the is
     * @return the long
     */
    private boolean processInteger(ByteBuf in) {
        int length = lineLength();
        if (length == 0) {
            return false;
        }
        
        in.skipBytes(length);
        return true;
    }

    /**
     * Process bulk reply.
     *
     * @param is the is
     * @return the byte[]
     */
    private boolean processBulkReply(ByteBuf in) {
        int length = lineLength();
        if (length == 0) {
            return false;
        }
        in.skipBytes(length);

        int numBytes = readInt(length - 2); // 2 == \r\n
        if (numBytes == -1) {
            return true;
        }
        
        if (in.readableBytes() < numBytes + 2) {
            return false;
        }
        
        in.skipBytes(numBytes + 2);

        return true;
    }

    /**
     * Process status code reply.
     *
     * @param is the is
     * @return the byte[]
     */
    private boolean processStatusCodeReply(ByteBuf in) {
        int length = lineLength();
        if (length == 0) {
            return false;
        }
        
        in.skipBytes(length);
        return true;
    }
    
    private int readInt(int lineLength) {
        /* minus */
        boolean minus = false;
        
        if (lookasideBuffer[lookasideBufferReaderIndex] == MINUS_BYTE) {
            minus = true;
            
            lookasideBufferReaderIndex++;
            lineLength--;
        }

        if (lineLength <= 0) {
            throw new RuntimeException("Redis protocol exception; malformed number format");
        }
        
        /* get value */
        int value = 0;
        
        for (int i = 0; i < lineLength; ++i) {
            byte b = lookasideBuffer[lookasideBufferReaderIndex++];
            if (b < '0' || '9' < b) {
                throw new RuntimeException("Reids protocol exception; invalid byte in number format; byte=" + b);
            }
            
            value = value * 10 + (b - '0');
        }
        
        if (minus) {
            value = -value;
        }
        
        return value;
    }
    
    private int lineLength(ByteBuf in) {
        int readableBytes = in.readableBytes();
        if (readableBytes < 2) {
            return -1;
        }       

        /* CR */
        int length = in.bytesBefore(CR_BYTE);
        if (length < 0) {
            return -1;
        }
        
        if (readableBytes < length + 2) {
            return -1;
        }
        
        /* LF */
        byte eolLF = in.getByte(in.readerIndex() + length + 1);
        if (eolLF != LF_BYTE) {
            throw new RuntimeException("Redis protocol exception; malformed end of line; byte=" + eolLF);
        }
        
        return length + 2;
    }
    
    private int lineLength() {
        final int stx = lookasideBufferReaderIndex;
        try {
            while (readableLookasideBufferBytes() > 0) {
                if (lookasideBuffer[lookasideBufferReaderIndex++] == CR_BYTE) {
                    if (readableLookasideBufferBytes() < 1) {
                        return 0;
                    }
    
                    if (lookasideBuffer[lookasideBufferReaderIndex++] == LF_BYTE) {
                        return lookasideBufferReaderIndex - stx;
                    }
                }
            }
            
            if (lookasideBufferReaderIndex >= lookasideBuffer.length) {
                throw new RuntimeException("Redis protocol exception; malformed end of line");
            }
            
            return 0;
        } finally {
            lookasideBufferReaderIndex = stx;
        }
    }

    /**
     * The Constant DEFAULT_DATABASE.
     */
    static final int DEFAULT_DATABASE = 0;

    /**
     * The Constant CHARSET.
     */
    static final String CHARSET = "UTF-8";

    /**
     * The Constant DOLLAR_BYTE.
     */
    static final byte DOLLAR_BYTE = '$';

    /**
     * The Constant ASTERISK_BYTE.
     */
    static final byte ASTERISK_BYTE = '*';

    /**
     * The Constant PLUS_BYTE.
     */
    static final byte PLUS_BYTE = '+';

    /**
     * The Constant MINUS_BYTE.
     */
    static final byte MINUS_BYTE = '-';

    /**
     * The Constant COLON_BYTE.
     */
    static final byte COLON_BYTE = ':';
    
    static final byte CR_BYTE = '\r';
    static final byte LF_BYTE = '\n';
}
/* end of class RedisDecoder */
