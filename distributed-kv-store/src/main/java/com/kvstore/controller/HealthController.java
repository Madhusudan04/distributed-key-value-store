package com.kvstore.controller;

import com.kvstore.cache.LRUCache;
import com.kvstore.cluster.HealthCheckManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Health check endpoints for monitoring.
 */
@Slf4j
@RestController
@RequestMapping("/api/health")
public class HealthController {

    private final HealthCheckManager healthCheckManager;
    private final LRUCache<String, String> cache;

    public HealthController(HealthCheckManager healthCheckManager,
                            LRUCache<String, String> cache) {
        this.healthCheckManager = healthCheckManager;
        this.cache = cache;
    }

    /**
     * GET /api/health/check
     * Returns application health status.
     */
    @GetMapping("/check")
    public ResponseEntity<HealthCheckResponse> healthCheck() {
        try {
            HealthCheckManager.ClusterStatus status =
                    healthCheckManager.getClusterStatus();

            HealthCheckResponse response = new HealthCheckResponse();
            response.setStatus("UP");
            response.setCacheOperational(true);
            response.setCacheSize(cache.size());
            response.setClusterHealthy(status.healthyNodes > 0);
            response.setHealthyNodes(status.healthyNodes);
            response.setTotalNodes(status.totalNodes);
            response.setTimestamp(System.currentTimeMillis());

            log.debug("Health check performed");
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error performing health check", e);

            return ResponseEntity.status(503).body(
                    new HealthCheckResponse(
                            "DOWN",
                            false,
                            0,
                            false,
                            0,
                            0,
                            System.currentTimeMillis()
                    )
            );
        }
    }

    /**
     * GET /api/health/ready
     * Returns readiness status.
     */
    @GetMapping("/ready")
    public ResponseEntity<ReadinessResponse> readinessCheck() {
        try {
            HealthCheckManager.ClusterStatus status =
                    healthCheckManager.getClusterStatus();

            boolean ready =
                    status.totalNodes > 0 && status.healthyNodes > 0;

            ReadinessResponse response = new ReadinessResponse();
            response.setReady(ready);
            response.setReason(
                    ready
                            ? "Service is ready"
                            : "No healthy nodes available"
            );
            response.setHealthyNodes(status.healthyNodes);
            response.setTotalNodes(status.totalNodes);
            response.setTimestamp(System.currentTimeMillis());

            log.debug("Readiness check performed: ready={}", ready);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error performing readiness check", e);

            return ResponseEntity.status(503).body(
                    new ReadinessResponse(
                            false,
                            "Service unavailable",
                            0,
                            0,
                            System.currentTimeMillis()
                    )
            );
        }
    }

    @Data
    @AllArgsConstructor
    public static class HealthCheckResponse {

        private String status;
        private boolean cacheOperational;
        private int cacheSize;
        private boolean clusterHealthy;
        private int healthyNodes;
        private int totalNodes;
        private long timestamp;

        public HealthCheckResponse() {
            this.timestamp = System.currentTimeMillis();
        }
    }

    @Data
    @AllArgsConstructor
    public static class ReadinessResponse {

        private boolean ready;
        private String reason;
        private int healthyNodes;
        private int totalNodes;
        private long timestamp;

        public ReadinessResponse() {
            this.timestamp = System.currentTimeMillis();
        }
    }
}

/**
 * Custom health indicator for Spring Boot Actuator.
 */
@Slf4j
@Component
class KVStoreHealthIndicator implements HealthIndicator {

    private final HealthCheckManager healthCheckManager;
    private final LRUCache<String, String> cache;

    public KVStoreHealthIndicator(
            HealthCheckManager healthCheckManager,
            LRUCache<String, String> cache) {
        this.healthCheckManager = healthCheckManager;
        this.cache = cache;
    }

    @Override
    public Health health() {
        try {
            HealthCheckManager.ClusterStatus status =
                    healthCheckManager.getClusterStatus();

            if (status.totalNodes == 0) {
                return Health.down()
                        .withDetail("reason", "No nodes in cluster")
                        .build();
            }

            if (status.healthyNodes == 0) {
                return Health.down()
                        .withDetail("reason", "No healthy nodes")
                        .withDetail("totalNodes", status.totalNodes)
                        .build();
            }

            return Health.up()
                    .withDetail("cache_size", cache.size())
                    .withDetail("cache_capacity", cache.getCapacity())
                    .withDetail("healthy_nodes", status.healthyNodes)
                    .withDetail("total_nodes", status.totalNodes)
                    .withDetail(
                            "cluster_health",
                            (100.0 * status.healthyNodes / status.totalNodes)
                                    + "%"
                    )
                    .build();

        } catch (Exception e) {
            log.error("Error in health check", e);

            return Health.down()
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }
}