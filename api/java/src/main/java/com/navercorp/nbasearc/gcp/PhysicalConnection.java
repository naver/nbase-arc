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

import static com.navercorp.nbasearc.gcp.ErrorCode.*;
import static com.navercorp.nbasearc.gcp.PhysicalConnection.State.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * 
 * One request, one callback.
 * First request, first callback. (except timeout)
 * 
 *   +---------------------------------------------unuse-------+
 *   |                    +------------------------unuse-------+
 *   |                    |                                    v
 * NONE --connect--> CONNECTING --y--> CONNECTED --unuse--> CLOSING --> UNUSED
 *                     ^     |             |  
 *                     +--n--+             |
 *                     +-------error-------+
 * 
 * @author seongjoon.ahn@navercorp.com
 * @author seunghoo.han@navercorp.com (maintenance)
 *
 */
class PhysicalConnection {

    enum State {
        NONE, CONNECTING, CONNECTED, CLOSING, UNUSED
    }

    private static final Logger log = LoggerFactory.getLogger(PhysicalConnection.class);
    private final int CLOSEJOB_INTERVAL_MILLIS = 100;

    private final Bootstrap b;
    private final Pipeline pipeline;
    private final SingleThreadEventLoop eventLoop;
    private final String ip;
    private final int port;
    private final Gateway gw;
    private final int reconnectInterval;

    private boolean pendingFlush;
    private boolean channelConnected;
    private Channel ch;
    private AtomicReference<SettableFuture<?>> closeFuture;
    private AtomicInteger referenceCount;

    private volatile AtomicReference<State> state;

    static PhysicalConnection create(String ip, int port, SingleThreadEventLoop eventLoop, Gateway gw,
            int reconnectInterval) {
        final PhysicalConnection pc = new PhysicalConnection(ip, port, eventLoop, gw, reconnectInterval);

        pc.b.group(eventLoop.getEventLoopGroup())
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_LINGER, 0)
            .option(ChannelOption.SO_SNDBUF, SOCKET_BUFFER)
            .option(ChannelOption.SO_RCVBUF, SOCKET_BUFFER)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(pc.new PhysicalConnectionHandler());
                }
            });
        
        return pc;
    }

    private PhysicalConnection(String ip, int port, SingleThreadEventLoop eventLoop, Gateway gw,
            int reconnectInterval) {
        this.ip = ip;
        this.port = port;
        this.state = new AtomicReference<State>(NONE);
        this.eventLoop = eventLoop;
        this.gw = gw;
        this.reconnectInterval = reconnectInterval;

        this.pipeline = new Pipeline(PIPELINE_SIZE, eventLoop);

        this.b = new Bootstrap();
        this.pendingFlush = false;
        this.channelConnected = false;
        this.ch = null;
        this.closeFuture = new AtomicReference<SettableFuture<?>>();
        this.referenceCount = new AtomicInteger();
    }
    
    private Runnable tryCloseJob;

    private boolean tryClose() {
        synchronized (this) {
            if (pipeline.isEmpty() && pendingFlush == false && referenceCount.get() == 0) {
                setState(UNUSED);
                close();
                
                closeFuture.get().set(null);
                
                return true;
            }
            
            return false;
        }
    }

    private void close() {
        if (channelConnected) {
            channelConnected = false;
            gw.decreaseActive();
            ch.close();
        }
    }

    SettableFuture<Boolean> connect() {
        state.set(State.CONNECTING);
        final SettableFuture<Boolean> sf = SettableFuture.create();
        b.connect(ip, port).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture cf) throws Exception {
                connectionComplete(cf);

                if (cf.cause() != null) {
                    sf.setException(cf.cause());
                } else {
                    sf.set(true);
                }
            }
        });
        return sf;
    }

    private void connectionComplete(ChannelFuture cf) {
        if (cf.cause() != null) {
            eventLoop.getEventLoopGroup().schedule(reconnectJob, reconnectInterval, TimeUnit.MILLISECONDS);
            return;
        }
        gw.increaseActive();
        ch = cf.channel();
        channelConnected = true;
        setState(CONNECTED);
    }

    private void setState(State newState) {
        log.debug("PhysicalConnection state changed. {}:{} {}->{}", new Object[] { ip, port, state.get(), newState });
        state.set(newState);
    }

    State getState() {
        return state.get();
    }
    
    int increaseReferenceCount() {
        return referenceCount.incrementAndGet();
    }
    
    int decreaseReferenceCount() {
        assert referenceCount.get() > 0 : "refCnt is " + referenceCount;
        return referenceCount.decrementAndGet();
    }
    
    int getReferenceCount() {
        return referenceCount.get();
    }

    ListenableFuture<?> unuse(final Set<VirtualConnection> vcConcurrentSet) {
        if (closeFuture.compareAndSet(null, SettableFuture.create()) == false) {
            return closeFuture.get();
        }
        
        setState(CLOSING);
        
        tryCloseJob = new Runnable() {
            @Override
            public void run() {
                try {
                    for (VirtualConnection vc : vcConcurrentSet) {
                        vc.reallocIdlePc(PhysicalConnection.this);
                    }
                    
                    if (!tryClose()) {
                        eventLoop.getEventLoopGroup().schedule(tryCloseJob, CLOSEJOB_INTERVAL_MILLIS,
                                TimeUnit.MILLISECONDS);
                    }
                } catch (Exception e) {
                    log.error("tryClose fail.", e);
                }
            }
        };
        eventLoop.getEventLoopGroup().schedule(tryCloseJob, CLOSEJOB_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        
        return closeFuture.get();
    }

    boolean isSafeToAbandon() {
        return pipeline.isEmpty();
    }

    void execute(Runnable runnable) {
        eventLoop.getEventLoopGroup().execute(runnable);
    }

    void request(Request rqst) {
        if (getState() != CONNECTED && getState() != CLOSING && pendingFlush == false) {
            assert pipeline.isEmpty() : "Pipeline is not clear.";
            rqst.getVirtualConnection().onResponse(rqst, null, CONNECTION_ERROR);
            return;
        }
        
        pipeline.put(rqst);
        if (rqst.getType() == Request.Type.USER) {
            eventLoop.addTimer(rqst);
        }

        if (pendingFlush) {
            return;
        }

        eventLoop.getEventLoopGroup().execute(writeAndFlushJob);
    }

    private final Runnable writeAndFlushJob = new Runnable() {
        @Override
        public void run() {
            /* previous write and flush not complete */
            if (pendingFlush) {
                return;
            }

            /* build buffer */
            ByteBuf out = pipeline.aggregate(ch.alloc());
            if (out == null) {
                return;
            }

            /* write and flush */
            pendingFlush = true;

            out.retain();
            ch.writeAndFlush(out).addListener(writeListener);
            out.release();
        }
    };

    private final ChannelFutureListener writeListener = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture cf) throws Exception {

            pendingFlush = false;

            if (cf.isSuccess()) {
                ch.eventLoop().execute(writeAndFlushJob);
            } else {
                log.error("Redis connection send failed", cf.cause());

                setState(CONNECTING);
                close();
                pipeline.clear(CONNECTION_ERROR);
                return;
            }
        }
    };

    private void processResponse(final byte[] result) {
        Request rqst = pipeline.pollSent();
        if (rqst == null) {
            log.error("Illegal internal pipeline state");
            return;
        }

        if (rqst.getType() == Request.Type.SYSTEM) {
            return;
        }

        if (rqst.getState() == Request.State.QUEUING && rqst.isTimeout()) {
            log.error("{} of request cannot get an any response.", rqst.getState());
            rqst.getVirtualConnection().onResponse(rqst, result, INTERNAL_ERROR);
            return;
        }

        if (rqst.getState() != Request.State.SENT) {
            log.error("Illegal reqeust state " + rqst + ", tid: " + Thread.currentThread().getId());
            rqst.getVirtualConnection().onResponse(rqst, result, INTERNAL_ERROR);
            return;
        }

        if (rqst.isTimeout()) {
            return;
        } else {
            rqst.setState(Request.State.DONE);

            eventLoop.delTimer(rqst);
            rqst.getVirtualConnection().onResponse(rqst, result, OK);
        }
    }

    void removeRequestFromPipeline(Request rqst) {
        pipeline.remove(rqst);
    }

    long busyCost() {
        Request rqst = pipeline.peekFirst();
        if (rqst == null) {
            return 0;
        }

        return System.currentTimeMillis() - rqst.getConnTimestamp();
    }

    private Runnable reconnectJob = new Runnable() {
        @Override
        public void run() {
            if (getState() == CLOSING || getState() == UNUSED) {
                log.error("Invalid state to reconnect. Connection is closed.");
                return;
            }
            log.error("Reconnect. {}", PhysicalConnection.this);
            b.connect(ip, port).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture cf) throws Exception {
                    connectionComplete(cf);
                }
            });
        }
    };

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append("ip: ").append(ip).append(", port: ").append(port).append("]");
        return sb.toString();
    }

    private class PhysicalConnectionHandler extends ChannelInboundHandlerAdapter {

        private final ByteBufAllocator ALLOCATOR = PooledByteBufAllocator.DEFAULT;
        
        private final ByteBuf buf;
        private final RedisDecoder decoder;
        
        private final List<byte[]> msgs = new ArrayList<byte[]>();

        PhysicalConnectionHandler() {
            this.buf = ALLOCATOR.ioBuffer(SOCKET_BUFFER * 2);
            this.decoder = new RedisDecoder();
        }

        @Override
        public void channelRead(ChannelHandlerContext _ctx, Object obj) {
            {
                ByteBuf in = (ByteBuf) obj;
                int inBytes = in.readableBytes();

                buf.ensureWritable(inBytes);
                buf.writeBytes(in);

                in.release();
            }

            decoder.getFrames(buf, msgs);

            buf.discardSomeReadBytes();

            for (byte[] msg : msgs) {
                try {
                    processResponse(msg);
                } catch (Exception e) {
                    log.error("processResponse fail.", e);
                }
            }

            msgs.clear();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Redis connection exception. {}", PhysicalConnection.this, cause);

            PhysicalConnection.this.setState(CONNECTING);
            PhysicalConnection.this.close();
            pipeline.clear(CONNECTION_ERROR);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.info("Redis connection inactived. {}", PhysicalConnection.this);
            try {
                PhysicalConnection.this.close();
                pipeline.clear(CONNECTION_ERROR);
                buf.release();

                if (getState() != UNUSED) {
                    setState(CONNECTING);

                    log.info("Run reconnectJob channelInactive. {}", PhysicalConnection.this);
                    eventLoop.getEventLoopGroup().schedule(reconnectJob, 1000, TimeUnit.MILLISECONDS);
                }
            } catch (Exception e) {
                log.error("Internal error", e);
            }
        }
    }
    /* end of class RedisConnectionHandler */

    /* defaults */
    static final int CONNECT_TIMEOUT = 5 * 1000;
    static final int COMMAND_TIMEOUT = 10 * 1000;

    static final int SOCKET_BUFFER = 65536;
    static final int PIPELINE_SIZE = 4096;

}
/* end of class RedisConnection */
