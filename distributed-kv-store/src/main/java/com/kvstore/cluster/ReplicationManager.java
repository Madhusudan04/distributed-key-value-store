package com.kvstore.cluster;

import com.kvstore.cache.LRUCache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;

/**
 * Manages replication of data across cluster nodes.
 *
 * - Replicates writes to replica nodes
 * - Handles failover when primary is down
 * - Syncs data between nodes
 */
@Slf4j
@Component
public class ReplicationManager {

    private final ConsistentHash consistentHash;
    private final LRUCache<String, String> cache;
    private final int replicationFactor;
    private final int socketTimeoutMs;

    public ReplicationManager(
            ConsistentHash consistentHash,
            LRUCache<String, String> cache,
            @Value("${kvstore.cluster.replication-factor:3}") int replicationFactor,
            @Value("${kvstore.cluster.socket-timeout-ms:5000}") int socketTimeoutMs) {
        this.consistentHash = consistentHash;
        this.cache = cache;
        this.replicationFactor = replicationFactor;
        this.socketTimeoutMs = socketTimeoutMs;
    }

    /**
     * Replicates a SET operation to all responsible replica nodes.
     *
     * @param key the key
     * @param value the value
     * @param expirySeconds expiry time in seconds
     * @return number of successful replications
     */
    public int replicateSet(String key, String value, long expirySeconds) {
        List<ClusterNode> nodes = consistentHash.getResponsibleNodes(key, replicationFactor);

        if (nodes.isEmpty()) {
            log.warn("No nodes available for replication of key: {}", key);
            return 0;
        }

        int successCount = 0;

        for (ClusterNode node : nodes) {
            try {
                if (sendReplicaCommand(node, "SET", key, value, String.valueOf(expirySeconds))) {
                    successCount++;
                }
            } catch (Exception e) {
                log.warn("Failed to replicate SET to {}: {}", node.getId(), e.getMessage());
            }
        }

        log.debug("Replicated SET {} to {}/{} nodes", key, successCount, nodes.size());
        return successCount;
    }

    /**
     * Replicates a DEL operation to all responsible replica nodes.
     *
     * @param key the key
     * @return number of successful replications
     */
    public int replicateDel(String key) {
        List<ClusterNode> nodes = consistentHash.getResponsibleNodes(key, replicationFactor);

        if (nodes.isEmpty()) {
            log.warn("No nodes available for replication of key: {}", key);
            return 0;
        }

        int successCount = 0;

        for (ClusterNode node : nodes) {
            try {
                if (sendReplicaCommand(node, "DEL", key)) {
                    successCount++;
                }
            } catch (Exception e) {
                log.warn("Failed to replicate DEL to {}: {}", node.getId(), e.getMessage());
            }
        }

        log.debug("Replicated DEL {} to {}/{} nodes", key, successCount, nodes.size());
        return successCount;
    }

    /**
     * Replicates an EXPIRE operation to all responsible replica nodes.
     *
     * @param key the key
     * @param seconds expiry time in seconds
     * @return number of successful replications
     */
    public int replicateExpire(String key, long seconds) {
        List<ClusterNode> nodes = consistentHash.getResponsibleNodes(key, replicationFactor);

        if (nodes.isEmpty()) {
            log.warn("No nodes available for replication of key: {}", key);
            return 0;
        }

        int successCount = 0;

        for (ClusterNode node : nodes) {
            try {
                if (sendReplicaCommand(node, "EXPIRE", key, String.valueOf(seconds))) {
                    successCount++;
                }
            } catch (Exception e) {
                log.warn("Failed to replicate EXPIRE to {}: {}", node.getId(), e.getMessage());
            }
        }

        log.debug("Replicated EXPIRE {} to {}/{} nodes", key, successCount, nodes.size());
        return successCount;
    }

    /**
     * Sends a replication command to a remote node via TCP.
     *
     * @param node the target node
     * @param command the command (SET, DEL, EXPIRE)
     * @param args command arguments
     * @return true if replication was successful
     */
    private boolean sendReplicaCommand(ClusterNode node, String command, String... args) {
        if (!node.isHealthy()) {
            log.debug("Skipping replication to unhealthy node: {}", node.getId());
            return false;
        }

        try (Socket socket = new Socket(node.getHost(), node.getPort())) {
            socket.setSoTimeout(socketTimeoutMs);

            try (OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                // Build replica command: REPLICATE command key [value] [seconds]
                StringBuilder sb = new StringBuilder("REPLICATE ");
                sb.append(command);
                for (String arg : args) {
                    sb.append(" ").append(arg);
                }
                sb.append("\n");

                out.write(sb.toString().getBytes("UTF-8"));
                out.flush();

                // Read response (simple OK/ERR)
                byte[] buffer = new byte[256];
                int bytesRead = in.read(buffer);
                String response = new String(buffer, 0, bytesRead, "UTF-8").trim();

                if (response.startsWith("OK")) {
                    log.debug("Replica command succeeded on {}: {}", node.getId(), command);
                    return true;
                } else {
                    log.warn("Replica command failed on {}: {}", node.getId(), response);
                    node.markUnhealthy();
                    return false;
                }
            }

        } catch (SocketTimeoutException e) {
            log.warn("Replication timeout to {}: {}", node.getId(), e.getMessage());
            node.markUnhealthy();
            return false;
        } catch (IOException e) {
            log.warn("Replication error to {}: {}", node.getId(), e.getMessage());
            node.markUnhealthy();
            return false;
        }
    }

    /**
     * Gets the list of responsible nodes for a key.
     *
     * @param key the key
     * @return list of nodes
     */
    public List<ClusterNode> getResponsibleNodes(String key) {
        return consistentHash.getResponsibleNodes(key, replicationFactor);
    }

    /**
     * Gets the primary (first responsible) node for a key.
     *
     * @param key the key
     * @return the primary node
     */
    public ClusterNode getPrimaryNode(String key) {
        return consistentHash.getPrimaryNode(key);
    }
}