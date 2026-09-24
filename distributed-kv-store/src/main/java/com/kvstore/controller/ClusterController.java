package com.kvstore.controller;

import com.kvstore.cache.LRUCache;
import com.kvstore.cluster.ClusterNode;
import com.kvstore.cluster.ConsistentHash;
import com.kvstore.cluster.HealthCheckManager;
import com.kvstore.cluster.NodeSyncManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * REST API endpoints for cluster monitoring and status.
 */
@Slf4j
@RestController
@RequestMapping("/api/cluster")
public class ClusterController {

    private final ConsistentHash consistentHash;
    private final HealthCheckManager healthCheckManager;
    private final NodeSyncManager nodeSyncManager;
    private final LRUCache<String, String> cache;

    public ClusterController(ConsistentHash consistentHash,
                            HealthCheckManager healthCheckManager,
                            NodeSyncManager nodeSyncManager,
                            LRUCache<String, String> cache) {
        this.consistentHash = consistentHash;
        this.healthCheckManager = healthCheckManager;
        this.nodeSyncManager = nodeSyncManager;
        this.cache = cache;
    }

    /**
     * GET /api/cluster/status
     * Returns overall cluster status.
     */
    @GetMapping("/status")
    public ResponseEntity<ClusterStatusResponse> getClusterStatus() {
        try {
            HealthCheckManager.ClusterStatus status = healthCheckManager.getClusterStatus();
            
            ClusterStatusResponse response = new ClusterStatusResponse();
            response.setTotalNodes(status.totalNodes);
            response.setHealthyNodes(status.healthyNodes);
            response.setUnhealthyNodes(status.unhealthyNodes);
            response.setClusterHealth(getClusterHealth(status.healthyNodes, status.totalNodes));
            response.setCacheSize(cache.size());
            response.setCacheCapacity(cache.getCapacity());
            response.setTimestamp(System.currentTimeMillis());

            log.info("Cluster status requested: {}", response);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting cluster status", e);
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * GET /api/cluster/nodes
     * Returns list of all nodes in the cluster.
     */
    @GetMapping("/nodes")
    public ResponseEntity<NodesResponse> getClusterNodes() {
        try {
            Collection<ClusterNode> nodes = consistentHash.getAllNodes();
            List<NodeInfo> nodeInfos = new ArrayList<>();

            for (ClusterNode node : nodes) {
                NodeInfo info = new NodeInfo();
                info.setId(node.getId());
                info.setHost(node.getHost());
                info.setPort(node.getPort());
                info.setStatus(node.getStatus().toString());
                info.setAddress(node.getAddress());
                info.setLastHeartbeat(node.getLastHeartbeat());
                info.setTimeSinceLastHeartbeatMs(node.getTimeSinceLastHeartbeat());
                nodeInfos.add(info);
            }

            NodesResponse response = new NodesResponse();
            response.setNodes(nodeInfos);
            response.setTotalNodes(nodes.size());
            response.setTimestamp(System.currentTimeMillis());

            log.info("Cluster nodes requested: {} nodes", nodes.size());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting cluster nodes", e);
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * GET /api/cluster/node/{nodeId}
     * Returns details of a specific node.
     */
    @GetMapping("/node/{nodeId}")
    public ResponseEntity<NodeInfo> getNode(@PathVariable String nodeId) {
        try {
            ClusterNode node = consistentHash.getNode(nodeId);
            if (node == null) {
                log.warn("Node not found: {}", nodeId);
                return ResponseEntity.notFound().build();
            }

            NodeInfo info = new NodeInfo();
            info.setId(node.getId());
            info.setHost(node.getHost());
            info.setPort(node.getPort());
            info.setStatus(node.getStatus().toString());
            info.setAddress(node.getAddress());
            info.setLastHeartbeat(node.getLastHeartbeat());
            info.setTimeSinceLastHeartbeatMs(node.getTimeSinceLastHeartbeat());

            log.info("Node details requested: {}", nodeId);
            return ResponseEntity.ok(info);
        } catch (Exception e) {
            log.error("Error getting node details", e);
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * GET /api/cluster/key/{key}
     * Returns which nodes are responsible for a key.
     */
    @GetMapping("/key/{key}")
    public ResponseEntity<KeyLocationResponse> getKeyLocation(@PathVariable String key) {
        try {
            List<ClusterNode> nodes = nodeSyncManager.getResponsibleNodes(key);
            List<String> nodeIds = new ArrayList<>();

            for (ClusterNode node : nodes) {
                nodeIds.add(node.getId());
            }

            KeyLocationResponse response = new KeyLocationResponse();
            response.setKey(key);
            response.setResponsibleNodes(nodeIds);
            response.setPrimaryNode(nodeIds.isEmpty() ? null : nodeIds.get(0));
            response.setReplicationFactor(Math.min(3, nodeIds.size()));
            response.setTimestamp(System.currentTimeMillis());

            log.info("Key location requested: {} -> {}", key, nodeIds);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting key location", e);
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * GET /api/cluster/health
     * Returns detailed health information.
     */
    @GetMapping("/health")
    public ResponseEntity<HealthResponse> getClusterHealth() {
        try {
            HealthCheckManager.ClusterStatus status = healthCheckManager.getClusterStatus();
            Collection<ClusterNode> nodes = consistentHash.getAllNodes();

            HealthResponse response = new HealthResponse();
            response.setOverallHealth(getClusterHealth(status.healthyNodes, status.totalNodes));
            response.setTotalNodes(status.totalNodes);
            response.setHealthyNodes(status.healthyNodes);
            response.setUnhealthyNodes(status.unhealthyNodes);
            response.setHealthPercentage((status.totalNodes == 0) ? 0 : 
                    (100.0 * status.healthyNodes / status.totalNodes));

            List<NodeHealthInfo> nodeHealths = new ArrayList<>();
            for (ClusterNode node : nodes) {
                NodeHealthInfo nodeHealth = new NodeHealthInfo();
                nodeHealth.setNodeId(node.getId());
                nodeHealth.setStatus(node.getStatus().toString());
                nodeHealth.setHealthy(node.isHealthy());
                nodeHealth.setLastHeartbeatMs(node.getTimeSinceLastHeartbeat());
                nodeHealths.add(nodeHealth);
            }

            response.setNodeHealths(nodeHealths);
            response.setTimestamp(System.currentTimeMillis());

            log.info("Health details requested");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting cluster health", e);
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * GET /api/cluster/stats
     * Returns cluster statistics.
     */
    @GetMapping("/stats")
    public ResponseEntity<StatsResponse> getClusterStats() {
        try {
            Collection<ClusterNode> nodes = consistentHash.getAllNodes();
            
            StatsResponse response = new StatsResponse();
            response.setTotalNodes(nodes.size());
            response.setRingSize(consistentHash.getRingSize());
            response.setVirtualNodesPerPhysicalNode(
                    consistentHash.getRingSize() / Math.max(1, nodes.size()));
            response.setCacheSize(cache.size());
            response.setCacheCapacity(cache.getCapacity());
            response.setCacheUtilization((100.0 * cache.size() / cache.getCapacity()));
            response.setTimestamp(System.currentTimeMillis());

            log.info("Cluster stats requested");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting cluster stats", e);
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * Determines cluster health level.
     */
    private String getClusterHealth(int healthy, int total) {
        if (total == 0) {
            return "NO_NODES";
        }
        double percentage = (100.0 * healthy) / total;
        if (percentage == 100) {
            return "HEALTHY";
        } else if (percentage >= 66.67) {
            return "DEGRADED";
        } else {
            return "CRITICAL";
        }
    }

    // Response DTOs

    @Data
    public static class ClusterStatusResponse {
        private int totalNodes;
        private int healthyNodes;
        private int unhealthyNodes;
        private String clusterHealth;
        private int cacheSize;
        private int cacheCapacity;
        private long timestamp;
    }

    @Data
    public static class NodesResponse {
        private List<NodeInfo> nodes;
        private int totalNodes;
        private long timestamp;
    }

    @Data
    public static class NodeInfo {
        private String id;
        private String host;
        private int port;
        private String status;
        private String address;
        private long lastHeartbeat;
        private long timeSinceLastHeartbeatMs;
    }

    @Data
    public static class KeyLocationResponse {
        private String key;
        private List<String> responsibleNodes;
        private String primaryNode;
        private int replicationFactor;
        private long timestamp;
    }

    @Data
    public static class HealthResponse {
        private String overallHealth;
        private int totalNodes;
        private int healthyNodes;
        private int unhealthyNodes;
        private double healthPercentage;
        private List<NodeHealthInfo> nodeHealths;
        private long timestamp;
    }

    @Data
    public static class NodeHealthInfo {
        private String nodeId;
        private String status;
        private boolean healthy;
        private long lastHeartbeatMs;
    }

    @Data
    public static class StatsResponse {
        private int totalNodes;
        private int ringSize;
        private int virtualNodesPerPhysicalNode;
        private int cacheSize;
        private int cacheCapacity;
        private double cacheUtilization;
        private long timestamp;
    }
}