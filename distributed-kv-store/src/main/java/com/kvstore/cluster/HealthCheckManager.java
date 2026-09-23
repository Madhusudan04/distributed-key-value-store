package com.kvstore.cluster;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Collection;

/**
 * Manages health checks for cluster nodes.
 *
 * - Periodically pings all nodes
 * - Detects unhealthy nodes
 * - Marks nodes for recovery
 * - Updates node status in consistent hash
 */
@Slf4j
@Component
public class HealthCheckManager {

    private final ConsistentHash consistentHash;
    private final int healthCheckIntervalMs;
    private final int healthCheckTimeoutMs;
    private final int failureThreshold;

    public HealthCheckManager(
            ConsistentHash consistentHash,
            @Value("${kvstore.cluster.health-check-interval:5000}") int healthCheckIntervalMs,
            @Value("${kvstore.cluster.health-check-timeout:2000}") int healthCheckTimeoutMs,
            @Value("${kvstore.cluster.health-check-failure-threshold:3}") int failureThreshold) {
        this.consistentHash = consistentHash;
        this.healthCheckIntervalMs = healthCheckIntervalMs;
        this.healthCheckTimeoutMs = healthCheckTimeoutMs;
        this.failureThreshold = failureThreshold;
    }

    /**
     * Periodically checks health of all cluster nodes.
     * Runs every N milliseconds.
     */
    @Scheduled(fixedDelayString = "${kvstore.cluster.health-check-interval:5000}")
    public void performHealthCheck() {
        Collection<ClusterNode> nodes = consistentHash.getAllNodes();

        if (nodes.isEmpty()) {
            log.debug("No nodes to health check");
            return;
        }

        log.debug("Starting health check for {} nodes", nodes.size());

        for (ClusterNode node : nodes) {
            try {
                if (pingNode(node)) {
                    // Ping successful
                    if (!node.isHealthy()) {
                        log.info("Node recovered: {}", node.getId());
                        node.markHealthy();
                    }
                } else {
                    // Ping failed
                    if (node.isHealthy()) {
                        log.warn("Node unhealthy: {}", node.getId());
                        node.markUnhealthy();
                    }
                }
            } catch (Exception e) {
                log.warn("Health check error for {}: {}", node.getId(), e.getMessage());
                node.markUnhealthy();
            }
        }

        logClusterStatus();
    }

    /**
     * Pings a node to check if it's alive.
     *
     * @param node the node to ping
     * @return true if ping successful, false otherwise
     */
    private boolean pingNode(ClusterNode node) {
        try (Socket socket = new Socket(node.getHost(), node.getPort())) {
            socket.setSoTimeout(healthCheckTimeoutMs);

            try (OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                // Send PING command
                out.write("PING\n".getBytes("UTF-8"));
                out.flush();

                // Read response
                byte[] buffer = new byte[256];
                int bytesRead = in.read(buffer);
                String response = new String(buffer, 0, bytesRead, "UTF-8").trim();

                boolean success = response.contains("PONG");

                if (success) {
                    log.debug("Health check passed for {}", node.getId());
                } else {
                    log.debug("Health check failed for {}: {}", node.getId(), response);
                }

                return success;
            }

        } catch (SocketTimeoutException e) {
            log.debug("Health check timeout for {}", node.getId());
            return false;
        } catch (IOException e) {
            log.debug("Health check failed for {}: {}", node.getId(), e.getMessage());
            return false;
        }
    }

    /**
     * Logs the current cluster status.
     */
    private void logClusterStatus() {
        Collection<ClusterNode> nodes = consistentHash.getAllNodes();

        if (nodes.isEmpty()) {
            return;
        }

        long healthyCount = nodes.stream().filter(ClusterNode::isHealthy).count();
        long unhealthyCount = nodes.size() - healthyCount;

        log.info("Cluster status: {} healthy, {} unhealthy out of {} total",
                healthyCount, unhealthyCount, nodes.size());

        for (ClusterNode node : nodes) {
            long timeSinceHeartbeat = node.getTimeSinceLastHeartbeat();
            log.debug("  {} - status: {}, last heartbeat: {}ms ago",
                    node.getId(), node.getStatus(), timeSinceHeartbeat);
        }
    }

    /**
     * Gets the current cluster status summary.
     */
    public ClusterStatus getClusterStatus() {
        Collection<ClusterNode> nodes = consistentHash.getAllNodes();
        long healthyCount = nodes.stream().filter(ClusterNode::isHealthy).count();

        return new ClusterStatus(nodes.size(), (int) healthyCount, nodes.size() - (int) healthyCount);
    }

    /**
     * Cluster status DTO.
     */
    public static class ClusterStatus {
        public int totalNodes;
        public int healthyNodes;
        public int unhealthyNodes;

        public ClusterStatus(int totalNodes, int healthyNodes, int unhealthyNodes) {
            this.totalNodes = totalNodes;
            this.healthyNodes = healthyNodes;
            this.unhealthyNodes = unhealthyNodes;
        }

        @Override
        public String toString() {
            return String.format("ClusterStatus{total=%d, healthy=%d, unhealthy=%d}",
                    totalNodes, healthyNodes, unhealthyNodes);
        }
    }
}