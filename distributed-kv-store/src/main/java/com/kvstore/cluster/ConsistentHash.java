package com.kvstore.cluster;

import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Consistent Hashing implementation for distributing keys across cluster nodes.
 *
 * Uses virtual nodes to ensure even distribution.
 * Each physical node has multiple virtual nodes on the hash ring.
 */
@Slf4j
public class ConsistentHash {

    private final int virtualNodes;
    private final TreeMap<Long, ClusterNode> ring = new TreeMap<>();
    private final Map<String, ClusterNode> nodes = new HashMap<>();

    public ConsistentHash(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    /**
     * Adds a node to the hash ring.
     */
    public void addNode(ClusterNode node) {
        if (nodes.containsKey(node.getId())) {
            log.warn("Node already exists: {}", node.getId());
            return;
        }

        nodes.put(node.getId(), node);

        // Add virtual nodes to the ring
        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(node.getId() + ":" + i);
            ring.put(hash, node);
        }

        log.info("Added node {} to hash ring with {} virtual nodes", node.getId(), virtualNodes);
    }

    /**
     * Removes a node from the hash ring.
     */
    public void removeNode(String nodeId) {
        ClusterNode node = nodes.remove(nodeId);
        if (node == null) {
            log.warn("Node not found: {}", nodeId);
            return;
        }

        // Remove virtual nodes from the ring
        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(nodeId + ":" + i);
            ring.remove(hash);
        }

        log.info("Removed node {} from hash ring", nodeId);
    }

    /**
     * Gets all nodes in the cluster.
     */
    public Collection<ClusterNode> getAllNodes() {
        return new ArrayList<>(nodes.values());
    }

    /**
     * Gets a specific node by ID.
     */
    public ClusterNode getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    /**
     * Finds the responsible nodes for a key (primary + replicas).
     *
     * @param key the key
     * @param replicationFactor number of replicas (e.g., 3 = primary + 2 replicas)
     * @return list of nodes responsible for the key
     */
    public List<ClusterNode> getResponsibleNodes(String key, int replicationFactor) {
        if (ring.isEmpty()) {
            return new ArrayList<>();
        }

        List<ClusterNode> result = new ArrayList<>();
        Set<String> nodeIds = new HashSet<>();

        long hash = hash(key);

        // Find the first node >= hash
        SortedMap<Long, ClusterNode> tailMap = ring.tailMap(hash);
        Iterator<ClusterNode> iterator;

        if (tailMap.isEmpty()) {
            // Wrap around to the beginning
            iterator = ring.values().iterator();
        } else {
            iterator = tailMap.values().iterator();
        }

        // Collect unique nodes up to replicationFactor
        while (iterator.hasNext() && result.size() < replicationFactor) {
            ClusterNode node = iterator.next();
            if (!nodeIds.contains(node.getId())) {
                result.add(node);
                nodeIds.add(node.getId());
            }
        }

        // Wrap around if needed
        if (result.size() < replicationFactor) {
            for (ClusterNode node : ring.values()) {
                if (result.size() >= replicationFactor) {
                    break;
                }
                if (!nodeIds.contains(node.getId())) {
                    result.add(node);
                    nodeIds.add(node.getId());
                }
            }
        }

        log.debug("Key {} -> nodes: {}", key, nodeIds);
        return result;
    }

    /**
     * Gets the primary (first responsible) node for a key.
     */
    public ClusterNode getPrimaryNode(String key) {
        List<ClusterNode> nodes = getResponsibleNodes(key, 1);
        return nodes.isEmpty() ? null : nodes.get(0);
    }

    /**
     * Hash function using MD5.
     */
    private long hash(String key) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(key.getBytes("UTF-8"));
            
            long result = 0;
            for (int i = 0; i < 8; i++) {
                result = (result << 8) | (hash[i] & 0xFF);
            }
            return Math.abs(result);
        } catch (Exception e) {
            log.error("Error hashing key: {}", key, e);
            return key.hashCode();
        }
    }

    /**
     * Returns the current ring size (total virtual nodes).
     */
    public int getRingSize() {
        return ring.size();
    }

    /**
     * Returns the number of unique physical nodes.
     */
    public int getNodeCount() {
        return nodes.size();
    }

    /**
     * Returns true if the ring is empty.
     */
    public boolean isEmpty() {
        return ring.isEmpty();
    }
}