package com.kvstore.cluster;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for cluster setup.
 *
 * Initializes:
 * - Consistent hash ring
 * - All cluster nodes
 * - Replication manager
 */
@Slf4j
@Configuration
public class ClusterConfig {

    @Data
    public static class NodeConfig {

        private String id;
        private String host;
        private int port;
    }

    @Value("${kvstore.cluster.nodes:}")
    private String clusterNodes;

    @Value("${kvstore.cluster.virtual-nodes:150}")
    private int virtualNodes;

    @Bean
    public ConsistentHash consistentHash() {
        ConsistentHash hash = new ConsistentHash(virtualNodes);

        // Parse nodes from config:
        // pod-1:localhost:9001,pod-2:localhost:9002,pod-3:localhost:9003
        if (clusterNodes != null && !clusterNodes.isEmpty()) {
            String[] nodeConfigs = clusterNodes.split(",");

            for (String config : nodeConfigs) {
                String[] parts = config.trim().split(":");

                if (parts.length == 3) {
                    String nodeId = parts[0].trim();
                    String host = parts[1].trim();
                    int port = Integer.parseInt(parts[2].trim());

                    ClusterNode node =
                            new ClusterNode(nodeId, host, port);

                    hash.addNode(node);
                    log.info("Added cluster node: {}", node);
                }
            }
        } else {
            log.warn("No cluster nodes configured");
        }

        log.info(
                "ConsistentHash initialized with {} nodes and {} virtual nodes per node",
                hash.getNodeCount(),
                virtualNodes
        );

        return hash;
    }

}