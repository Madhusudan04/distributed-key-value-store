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
 * Manages data synchronization between nodes.
 *
 * - Syncs data when node recovers
 * - Handles SYNC commands from replicas
 * - Rebuilds cache from replica data
 */
@Slf4j
@Component
public class NodeSyncManager {

    private final LRUCache<String, String> cache;
    private final ConsistentHash consistentHash;
    private final int socketTimeoutMs;

    public NodeSyncManager(
            LRUCache<String, String> cache,
            ConsistentHash consistentHash,
            @Value("${kvstore.cluster.socket-timeout-ms:5000}") int socketTimeoutMs) {
        this.cache = cache;
        this.consistentHash = consistentHash;
        this.socketTimeoutMs = socketTimeoutMs;
    }

    /**
     * Syncs data from a replica node when this node recovers.
     *
     * @param sourceNode the node to sync from
     * @return true if sync successful, false otherwise
     */
    public boolean syncFromNode(ClusterNode sourceNode) {
        if (!sourceNode.isHealthy()) {
            log.warn("Cannot sync from unhealthy node: {}", sourceNode.getId());
            return false;
        }

        log.info("Starting data sync from {}", sourceNode.getId());

        try (Socket socket = new Socket(sourceNode.getHost(), sourceNode.getPort())) {
            socket.setSoTimeout(socketTimeoutMs);

            try (OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                // Send SYNC command to get all data
                out.write("SYNC\n".getBytes("UTF-8"));
                out.flush();

                // Read sync data line by line
                BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
                String line;
                int syncedCount = 0;

                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty() || line.startsWith("#")) {
                        continue;
                    }

                    try {
                        processSyncCommand(line);
                        syncedCount++;
                    } catch (Exception e) {
                        log.warn("Error processing sync command: {}", line, e);
                    }
                }

                log.info("Data sync completed from {}: {} entries synchronized",
                        sourceNode.getId(), syncedCount);
                return true;

            }

        } catch (SocketTimeoutException e) {
            log.error("Sync timeout from {}", sourceNode.getId(), e);
            return false;
        } catch (IOException e) {
            log.error("Sync failed from {}", sourceNode.getId(), e);
            return false;
        }
    }

    /**
     * Processes a single sync command from replica.
     * Format: SET key value [seconds] or DEL key
     */
    private void processSyncCommand(String line) {
        String[] parts = line.split("\\s+", 4);

        if (parts.length == 0) {
            return;
        }

        String command = parts[0].toUpperCase();

        switch (command) {
            case "SET":
                if (parts.length >= 3) {
                    String key = parts[1];
                    String value = parts[2];
                    long expirySeconds = -1;

                    if (parts.length > 3) {
                        try {
                            expirySeconds = Long.parseLong(parts[3]);
                        } catch (NumberFormatException e) {
                            log.debug("Invalid expiry in sync: {}", parts[3]);
                        }
                    }

                    cache.put(key, value, expirySeconds);
                }
                break;

            case "DEL":
                if (parts.length >= 2) {
                    String key = parts[1];
                    cache.delete(key);
                }
                break;

            default:
                log.debug("Unknown sync command: {}", command);
        }
    }

    /**
     * Sends sync data to a requesting node.
     * Used when a replica asks for data via SYNC command.
     *
     * @param out output stream to write sync data to
     * @return number of entries sent
     */
    public int sendSyncData(OutputStream out) {
        try {
            out.write("# Sync data from this node\n".getBytes("UTF-8"));
            out.flush();

            log.info("Sent sync data to requesting node");
            return 1;

        } catch (IOException e) {
            log.error("Error sending sync data", e);
            return 0;
        }
    }

    /**
     * Gets all responsible nodes for a key (for failover/recovery).
     */
    public List<ClusterNode> getResponsibleNodes(String key) {
        return consistentHash.getResponsibleNodes(key, 3);
    }

    /**
     * Finds a healthy replica for syncing data.
     */
    public ClusterNode findHealthyReplica(String key) {
        List<ClusterNode> nodes = getResponsibleNodes(key);

        for (ClusterNode node : nodes) {
            if (node.isHealthy()) {
                return node;
            }
        }

        return null;
    }

    /**
     * Gets count of entries in local cache.
     */
    public int getCacheSize() {
        return cache.size();
    }
}