/*
 * Copyright 2015 Naver Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.nbasearc.confmaster.server.mimic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MimicRemoteServer implements Runnable {
    ServerSocket listenSock;
    Map<Client, Thread> clntMap = new HashMap<Client, Thread>();
    IMimic<String, String> mimic;
    AtomicBoolean terminate = new AtomicBoolean(false);

    public MimicRemoteServer(int port, IMimic<String, String> mimic) throws IOException, InterruptedException {
        listenSock = new ServerSocket(port);
        this.mimic = mimic;
        mimic.init();
    }
    
    @Override
    public void run() {
        try {
            while (!terminate.get()) {
                Socket clientSocket = listenSock.accept();
                Client client = new Client(clientSocket);
                Thread clientThread = new Thread(client);
                clientThread.start();
                clntMap.put(client, clientThread);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void cancel() throws IOException {
        terminate.set(true);
        
        Socket s = new Socket();
        s.connect(new InetSocketAddress("localhost", listenSock.getLocalPort()), 3000);
        s.close();
    }
    
    public void release() throws IOException, InterruptedException {
        for (Map.Entry<Client, Thread> entry : clntMap.entrySet()) {
            entry.getKey().clientSocket.close();
            entry.getValue().join();
        }
        listenSock.close();
    }

    public class Client implements Runnable {
        BufferedReader in;
        BufferedWriter out;
        Socket clientSocket;

        public Client(Socket clientSocket) throws IOException {
            this.clientSocket = clientSocket;
            out = new BufferedWriter(new OutputStreamWriter(
                    clientSocket.getOutputStream()));
            in = new BufferedReader(new InputStreamReader(
                    clientSocket.getInputStream()));
        }

        public void run() {
            String input = null;
            try {
                while ((input = in.readLine()) != null) {
                    input = input.trim();
                    String output = mimic.execute(input);
                    send(output + "\r\n");
                }
            } catch (IOException e) {
            }
        }

        public synchronized void send(String message) throws IOException {
            out.write(message);
            out.flush();
        }
    }
}
