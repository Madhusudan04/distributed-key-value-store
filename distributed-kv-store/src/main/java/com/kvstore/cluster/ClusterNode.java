package com.kvstore.cluster;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Represents a node in the KV Store cluster.
 *
 * Each node has:
 * - Unique ID (pod-1, pod-2, etc.)
 * - Host address
 * - Port for communication
 * - Status (HEALTHY, UNHEALTHY, RECOVERING)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClusterNode implements Serializable {

    public enum Status {
        HEALTHY,
        UNHEALTHY,
        RECOVERING
    }

    private String id;           // Node identifier (pod-1, pod-2, etc.)
    private String host;         // Host address (localhost, 10.0.0.1, etc.)
    private int port;            // Port for TCP communication
    private Status status;        // Current health status
    private long lastHeartbeat;   // Timestamp of last successful communication

    public ClusterNode(String id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.status = Status.HEALTHY;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    /**
     * Returns the network address (host:port).
     */
    public String getAddress() {
        return host + ":" + port;
    }

    /**
     * Marks node as healthy.
     */
    public void markHealthy() {
        this.status = Status.HEALTHY;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    /**
     * Marks node as unhealthy.
     */
    public void markUnhealthy() {
        this.status = Status.UNHEALTHY;
    }

    /**
     * Marks node as recovering.
     */
    public void markRecovering() {
        this.status = Status.RECOVERING;
    }

    /**
     * Checks if node is healthy.
     */
    public boolean isHealthy() {
        return status == Status.HEALTHY;
    }

    /**
     * Returns time since last heartbeat in milliseconds.
     */
    public long getTimeSinceLastHeartbeat() {
        return System.currentTimeMillis() - lastHeartbeat;
    }

    @Override
    public String toString() {
        return String.format("%s(%s:%d)[%s]", id, host, port, status);
    }
}