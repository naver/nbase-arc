package com.navercorp.nbasearc.confmaster.server;

import static org.apache.log4j.Level.INFO;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.context.ContextType;
import com.navercorp.nbasearc.confmaster.io.Server;
import com.navercorp.nbasearc.confmaster.io.ServerSessionHandler;
import com.navercorp.nbasearc.confmaster.io.SessionHandler;
import com.navercorp.nbasearc.confmaster.logger.Logger;

public class ClusterContollerServer {

    private ApplicationContext context;
    private Config config;

    private Server server;
    private Future<Object> serverFuture;
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    
    private volatile boolean shutdown = false;
    
    public ClusterContollerServer(ApplicationContext context) {
        this.context = context;
        this.config = context.getBean(Config.class);
    }
    
    public void initialize() throws IOException {
        try {
            server = new Server(
                        config.getIp(),
                        config.getPort(), 
                        config.getServerClientMax(),
                        config.getServerClientTimeout(), 
                        new ClientSessionFactory(context));

            SessionHandler handler = new ServerSessionHandler(
                    server.getEventSelector(), server.getSessionFactory(), server);
            server.initialize(handler);
            
            serverFuture = executor.submit(new Runner(server));
        } catch (IOException e) {
            Logger.error("Start server fail. {}", server, e);
            throw e;
        }
    }
    
    public void relase() throws InterruptedException, ExecutionException, IOException {
        shutdown = true;
        serverFuture.get();
        server.close();
    }
    
    public Server getServer() {
        return server;
    }

    public Integer getSessionCount() {
        return server.getSessionCount();
    }
    
    @Override
    public String toString() {
        return "ClusterContoolerServer[port: " + config.getPort() + "]";
    }
    
    public class Runner implements Callable<Object> {
        
        private final Server server;
        
        public Runner(Server server) {
            this.server = server;
        }
        
        @Override
        public Object call() {
            Logger.setContextType(ContextType.SV);
            Logger.info("Server start on port {}", config.getPort());
            while (!shutdown) {
                server.process();
                Logger.flush(INFO);
            }
            return null;
        }
    };

}
