package com.navercorp.nbasearc.confmaster.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.navercorp.nbasearc.confmaster.logger.Logger;

public class BlockingSocket {

    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;

    private final String ip;
    private final int port;
    private final String ping;
    private final String delim;
    private final int timeout;
    private final Charset charset;
    
    public BlockingSocket(String ip, int port, int timeout, String ping,
            String delim, String charset) {
        this.socket = new Socket();
        this.ip = ip;
        this.port = port;
        this.ping = ping + delim;
        this.delim = delim;
        this.timeout = timeout;
        this.charset = Charset.forName(charset);
    }

    public BlockingSocket(String ip, int port, String ping, String delim,
            String charset) {
        this(ip, port, 0, ping, delim, charset);
    }

    public String execute(String query, int retryCount) throws IOException {
        int retry = 0;
        String result = null;

        for (int i = 0; i <= retryCount; i++) {
            try {
                result = execute(query);
            } catch (IOException e) {
                if (retry >= retryCount) {
                    throw e;
                }
                retry++;
            }
        }
        return result;
    }

    public String execute(String query) throws IOException {
        try {
            send(query);
            String reply = recvLine();
            if (reply != null) {
                return reply;
            } else {
                throw new IOException("The end of the stream has been reached. " + ip + ":" + port); 
            }
        } catch (SocketTimeoutException e) {
            Logger.info(String.format("Server %s:%d is not respond. timeout:%d", ip, port, timeout));
            Logger.debug(e.getMessage(), e);
            close();
            throw e;
        } catch (IOException e) {
            Logger.info(String.format("Server %s:%d is not respond", ip, port));
            Logger.debug(e.getMessage(), e);
            close();
            throw e;
        }
    }
    
    public List<String> executeAndMultiReply(String query, int replyCount) throws IOException {
        List<String> replyList = new ArrayList<String>();

        send(query);
        
        try {
            for (int i = 0; i < replyCount; i++) {
                String result = recvLine();
                replyList.add(result);
            }
        } catch (SocketTimeoutException e) {
            Logger.info(String.format("Server %s:%d is not respond. timeout:%d", ip, port, timeout));
            Logger.debug(e.getMessage(), e);
            close();
            throw e;
        } catch (IOException e) {
            Logger.info(String.format("Server %s:%d is not respond", ip, port));
            Logger.debug(e.getMessage(), e);
            close();
            throw e;
        }
        return replyList;
    }
    
    public void send(String query) throws IOException {
        if (!socket.isConnected() || socket.isClosed()) {
            try {
                connect();
            } catch (IOException e) {
                Logger.info(String.format("Server %s:%d is not connectable", ip, port));
                Logger.debug(e.getMessage(), e);
                close();
                throw e;
            }
        }

        out.print(ping);
        out.flush();

        try {
            if (recvLine() == null) {
                close();
                connect();
            }
        } catch (SocketTimeoutException e) {
            Logger.info(String.format("timeout, server %s:%d", ip, port));
            Logger.debug(e.getMessage(), e);
            close();
            throw e;
        } catch (IOException e) {
            try {
                close();
                connect();
            } catch (IOException e2) {
                Logger.info(String.format("Server %s:%d is not connectable", ip, port));
                Logger.debug(e.getMessage(), e2);
                close();
                throw e;
            }
        }

        out.print(query + delim);
        out.flush();
    }
    
    public String recvLine() throws IOException {
        
        
        
        return in.readLine();
    }

    private void connect() throws IOException {
        socket = new Socket();
        socket.connect(new InetSocketAddress(ip, port), timeout);
        socket.setSoTimeout(timeout);
        socket.setTcpNoDelay(true);
        in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), charset));
        out = new PrintWriter(
                new BufferedWriter(
                    new OutputStreamWriter(socket.getOutputStream(), charset)));
        Logger.debug("Connection to {}:{} was established.", ip, port);
    }

    public void close() throws IOException {
        if (in != null) in.close();
        if (out != null) out.close();
        if (socket != null) socket.close();
        Logger.debug("Connection to {}:{} was closed.", ip, port);
    }

}
