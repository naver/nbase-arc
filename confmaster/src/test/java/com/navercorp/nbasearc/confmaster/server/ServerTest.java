package com.navercorp.nbasearc.confmaster.server;

import static com.jayway.awaitility.Awaitility.*;
import static java.util.concurrent.TimeUnit.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.io.BlockingSocket;
import com.navercorp.nbasearc.confmaster.server.ClusterContollerServer;
import com.navercorp.nbasearc.confmaster.server.command.ClusterLifeCycleCommander;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class ServerTest extends BasicSetting {

    private final Integer MAX_EXECUTOR = 10;
    private ExecutorService executor = Executors.newFixedThreadPool(MAX_EXECUTOR);
    private final String ping = "PING";

    private ClusterContollerServer server;
    
    @Autowired
    private Config config;
     
    @BeforeClass
    public static void beforeClass() throws Exception {
        BasicSetting.beforeClass();
    }
    
    @Before
    public void before() throws Exception {
        try {
            super.before();
            server = new ClusterContollerServer(context);
            server.initialize();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    @After
    public void after() throws Exception {
        try {
            server.relase();
            super.after();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    @Test
    public void requestResponse() throws Exception {
        List<Future<String>> futures = new ArrayList<Future<String>>(MAX_EXECUTOR);
        for (int i = 0; i < MAX_EXECUTOR; i++) {
            Future<String> future = executor.submit(new Callable<String>() {
                @Override
                public String call() {
                    final String[][] commandTemplate = ClusterLifeCycleCommander
                            .makeOperations(Thread.currentThread().getId());

                    // Make Socket
                    BlockingSocket socket = new BlockingSocket(config.getIp(),
                            config.getPort(), ping, config.getDelim(), config.getCharset());

                    // Send command and Check reply
                    for (int i = 0; i < commandTemplate.length; i++) {
                        String command = commandTemplate[i][0];
                        String expected = commandTemplate[i][1];
                        String message = null;
                        try {
                            // Execute command
                            String reply = socket.execute(command);
                            
                            // Parse json formatted reply.
                            JsonNode replyJson = new ObjectMapper()
                                    .readTree(new StringReader(reply));

                            JsonNode msgNode = replyJson.get("msg");
                            if (msgNode != null) {
                                message = msgNode.getTextValue();
                            } else {
                                // When json formatted reply contains no msg, 
                                // data item will be contained. 
                                JsonNode dataNode = replyJson.get("data");
                                message = dataNode.toString();
                                
                                // Convert to expectedReply in order to 
                                // match the foramt with data item. 
                                expected = new ObjectMapper().readTree(
                                        new StringReader(expected))
                                        .toString();
                            }
                            
                            // Check command has been completed successfully.
                            if (!expected.equals(message)) {
                                return "Reply incurrect. EXPECTED="
                                        + expected + ", REPLY=" + message + ", COMMAND=" + command;
                            }
                        } catch (NullPointerException e) {
                            e.printStackTrace();
                            return e.getMessage();
                        } catch (Exception e) {
                            e.printStackTrace();
                            return e.getMessage();
                        }
                    }

                    return "OK";
                }
            });
            futures.add(future);
        }
        
        for (Future<String> future : futures) {
            String ret = future.get();
            assertEquals(ret, "OK", ret);
        }
    }
    
    @Test(expected=IOException.class)
    public void clientSessionInputBufferFull() throws Exception {
        BlockingSocket socket = new BlockingSocket(config.getIp(),
                config.getPort(), ping, config.getDelim(), config.getCharset());
        
        StringBuilder buffer = new StringBuilder(config.getServerClientBufferSize());
        for (int i = 0; i < config.getServerClientBufferSize(); i++) {
            buffer.append('x');
        }

        String reply = socket.execute(buffer.toString());
        assertNull("No reply from server.", reply);
    }
    
    @Test
    public void clientSessionCount() throws Exception {
        List<Socket> sockets = new ArrayList<Socket>();
        for (int i = 1; i <= config.getServerClientMax(); i++) {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("localhost", config.getPort()));
            
            sockets.add(socket);
            await("Could not connection to server."
                    ).atMost(5, SECONDS).until(new SessionCountChecker(server, i));
        }

        for (int i = config.getServerClientMax() - 1; i >= 0; i--) {
            sockets.get(i).close();
            await("Client connection did not decreased."
                    ).atMost(5, SECONDS).until(new SessionCountChecker(server, i));
        }
    }

    /**
     * @return returns a list of connected sockets.
     */
    public List<BlockingSocket> createConnections(int connectionCount, final SocketAddress addr)
            throws InterruptedException, ExecutionException {
        List<BlockingSocket> cons = new ArrayList<BlockingSocket>();
        for (int i = 0; i < connectionCount; i++) {
            try {
                BlockingSocket socket = new BlockingSocket(config.getIp(), config.getPort(),
                        ping, config.getDelim(), config.getCharset());
    
                String ret = socket.execute("ping");
                if (ret.equals("{\"state\":\"success\",\"msg\":\"+PONG\"}")) {
                    cons.add(socket);
                } else {
                    socket.close();
                }
            } catch (Exception e) {
                // Ignore
            }
        }
        
        return cons;
    }
    
    @Test
    public void maxClientConnection() throws IOException, InterruptedException,
            ExecutionException {
        SocketAddress addr = new InetSocketAddress("localhost", config.getPort());
        
        // Under limitation
        List<BlockingSocket> ulCons = createConnections(config.getServerClientMax(), addr);
        assertEquals(
                "Not enough established connections. expected :64, established :" + ulCons.size(), 
                64, ulCons.size());
        assertTrue(
                "The number of connections mismatch. client side :" + ulCons.size()
                    + ", server side :" + server.getSessionCount(),
                ulCons.size() == server.getSessionCount());
        
        // Over limitation
        List<BlockingSocket> olCons = createConnections(config.getServerClientMax(), addr);
        assertTrue(
                "Limit connection fail. expected : less than 64, server side :" + server.getSessionCount()
                    + ", ulCons :" + ulCons.size() + ", olCons :" + olCons.size(),
                64 >= server.getSessionCount());

        System.out.println(
                "server side :" + server.getSessionCount() 
                    + ", ulCons :" + ulCons.size() 
                    + ", olCons :" + olCons.size());
    }

    public static class SessionCountChecker implements Callable<Boolean> {
        private final ClusterContollerServer server;
        private final int clientCount;
        
        public SessionCountChecker(ClusterContollerServer server, int clientCount) {
            this.server = server;
            this.clientCount = clientCount;
        }
        
        @Override
        public Boolean call() throws Exception {
            return server.getSessionCount() == clientCount;
        }
    }

    @Test
    public void multiCommands() throws InterruptedException,
            ExecutionException, TimeoutException, JsonParseException,
            JsonMappingException, IOException {
        ExecutorService executor = Executors.newFixedThreadPool(config.getServerClientMax() * 3);
        Future<List<String>> future = executor.submit(new Callable<List<String>>() {
            @Override
            public List<String> call() {
                try {
                    BlockingSocket sock = new BlockingSocket(config.getIp(),
                            config.getPort(), ping, config.getDelim(), config.getCharset());

                    sock.send("\r\n\r\n");
                    sock.send("cluster_ls\r\ncluster_ls\r\ncluster_ls\r\ncluster_ls\r\ncluster_ls\r\ncluster_ls\r\ncluster_ls\r\ncluster_ls\r\ncluster_ls\r\ncluster_ls");
                    sock.send("\r\n\r\n");
                    sock.send("pm_ls\r\npm_ls\r\npm_ls\r\npm_ls\r\npm_ls\r\npm_ls\r\npm_ls\r\npm_ls\r\npm_ls\r\npm_ls");
                    sock.send("\r\n\r\n");
                    sock.send("ping\r\nping\r\nping\r\nping\r\nping\r\nping\r\nping\r\nping\r\nping\r\nping");
                    
                    List<String> rets = new ArrayList<String>();
                    for (int i = 0; i < 30; i++) {
                        rets.add(sock.recvLine());
                    }
                    return rets;
                } catch (Exception e) {
                    return null;
                }
            }
        });
        
        List<String> rets = future.get(30000, TimeUnit.MILLISECONDS);
        assertEquals("Replies is not enough. cnt:" + rets.size(), 30, rets.size());
        
        ObjectMapper mapper = new ObjectMapper();
        for (String ret : rets) {
            Map<String, Object> map = mapper.readValue(ret,
                    new TypeReference<Map<String, Object>>() {});
            assertEquals("Invalid reply :" + ret, 
                    "success", map.get("state"));
        }
    }
    
}

