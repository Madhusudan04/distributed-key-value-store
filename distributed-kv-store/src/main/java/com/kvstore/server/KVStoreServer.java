package com.kvstore.server;

import com.kvstore.cache.LRUCache;
import com.kvstore.cluster.NodeSyncManager;
import com.kvstore.cluster.ReplicationManager;
import com.kvstore.persistence.AOFWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * TCP Server for KV Store.
 * Listens on a specified port and accepts client connections.
 * Uses a thread pool to handle multiple clients concurrently.
 * Integrates with cluster replication and sync managers.
 */
@Slf4j
@Component
public class KVStoreServer {

    private final int port;
    private final int threadPoolSize;
    private final LRUCache<String, String> cache;
    private final AOFWriter aofWriter;
    private final ReplicationManager replicationManager;
    private final NodeSyncManager nodeSyncManager;

    private ServerSocket serverSocket;
    private ExecutorService executorService;
    private volatile boolean running = false;
    private Thread serverThread;

    public KVStoreServer(
            @Value("${kvstore.server.port:6379}") int port,
            @Value("${kvstore.server.thread-pool-size:10}") int threadPoolSize,
            LRUCache<String, String> cache,
            AOFWriter aofWriter,
            ReplicationManager replicationManager,
            NodeSyncManager nodeSyncManager) {
        this.port = port;
        this.threadPoolSize = threadPoolSize;
        this.cache = cache;
        this.aofWriter = aofWriter;
        this.replicationManager = replicationManager;
        this.nodeSyncManager = nodeSyncManager;
    }

    /**
     * Starts the server when application starts.
     */
    @PostConstruct
    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            executorService = Executors.newFixedThreadPool(threadPoolSize);
            running = true;

            serverThread = new Thread(this::acceptConnections);
            serverThread.setName("KVStoreServer-" + port);
            serverThread.setDaemon(false);
            serverThread.start();

            log.info("KV Store Server started on port {}", port);
            log.info("Thread pool size: {}", threadPoolSize);
        } catch (IOException e) {
            log.error("Failed to start KV Store Server on port {}", port, e);
            throw new RuntimeException("Failed to start server", e);
        }
    }

    /**
     * Accepts incoming client connections and assigns handlers.
     */
    private void acceptConnections() {
        log.info("Server accepting connections on port {}", port);

        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                Socket clientSocket = serverSocket.accept();
                log.debug("New client connection from {}:{}",
                        clientSocket.getInetAddress().getHostAddress(),
                        clientSocket.getPort());

                ConnectionHandler handler = new ConnectionHandler(clientSocket, cache, aofWriter,
                        replicationManager, nodeSyncManager);
                executorService.submit(handler);

            } catch (IOException e) {
                if (running) {
                    log.error("Error accepting client connection", e);
                } else {
                    log.debug("Server stopped accepting connections");
                    break;
                }
            }
        }

        log.info("Server stopped accepting connections");
    }

    /**
     * Gracefully shuts down the server when application stops.
     */
    @PreDestroy
    public void stop() {
        log.info("Shutting down KV Store Server...");
        running = false;

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
                log.debug("Server socket closed");
            }
        } catch (IOException e) {
            log.error("Error closing server socket", e);
        }

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Executor service did not terminate in time, forcing shutdown");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("Interrupted waiting for executor service", e);
                executorService.shutdownNow();
            }
            log.debug("Executor service shut down");
        }

        if (serverThread != null) {
            try {
                serverThread.interrupt();
                serverThread.join(2000);
            } catch (InterruptedException e) {
                log.error("Interrupted waiting for server thread", e);
            }
        }

        log.info("KV Store Server stopped");
    }

    /**
     * Returns whether the server is running.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Returns the server port.
     */
    public int getPort() {
        return port;
    }
}