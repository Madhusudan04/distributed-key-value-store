package com.kvstore.cache;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Thread-safe LRU (Least Recently Used) Cache implementation with TTL support.
 *
 * Uses a doubly-linked list for O(1) LRU tracking and HashMap for O(1) key lookup.
 * All operations (get, put, delete) are O(1) time complexity.
 * Supports time-to-live (TTL) expiration with lazy and active cleanup.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 */
@Slf4j
public class LRUCache<K, V> {

    /**
     * Node in the doubly-linked list.
     * Maintains key, value, pointers to previous and next nodes, and expiry time.
     */
    private class Node {
        K key;
        V val;
        Node next;
        Node prev;
        long expiryTimeMillis;  // -1 means no expiry

        Node(K key, V val) {
            this.key = key;
            this.val = val;
            this.expiryTimeMillis = -1;  // No expiry by default
        }
    }

    private final int capacity;
    private final Node head;  // Dummy head (most recently used side)
    private final Node tail;  // Dummy tail (least recently used side)
    private final Map<K, Node> cache = new HashMap<>();  // Key -> Node mapping

    /**
     * Initializes LRU cache with specified capacity.
     *
     * @param capacity the maximum number of entries the cache can hold
     * @throws IllegalArgumentException if capacity is <= 0
     */
    public LRUCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be greater than 0");
        }
        this.capacity = capacity;
        this.head = new Node(null, null);
        this.tail = new Node(null, null);
        this.head.next = tail;
        this.tail.prev = head;
    }

    /**
     * Retrieves the value associated with the given key.
     * Marks the key as recently used by moving it to the front.
     * Returns null if key doesn't exist or has expired.
     *
     * @param key the key to look up
     * @return the value associated with the key, or null if not found/expired
     */
    public synchronized V get(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if (!cache.containsKey(key)) {
            log.debug("Cache miss for key: {}", key);
            return null;
        }

        Node node = cache.get(key);

        // Check if key has expired (lazy expiry)
        if (isExpired(node)) {
            removeNode(node);
            cache.remove(key);
            log.debug("Key expired on access: {}", key);
            return null;
        }

        V value = node.val;

        // Move node to front (mark as recently used)
        removeNode(node);
        addToFront(node);

        log.debug("Cache hit for key: {}", key);
        return value;
    }

    /**
     * Puts a key-value pair into the cache without expiration.
     * If the key already exists, updates the value.
     * If cache is at capacity, removes the least recently used entry.
     *
     * @param key   the key
     * @param value the value
     * @throws IllegalArgumentException if key or value is null
     */
    public synchronized void put(K key, V value) {
        put(key, value, -1);  // -1 means no expiry
    }

    /**
     * Puts a key-value pair into the cache with optional expiration time.
     * If the key already exists, updates the value.
     * If cache is at capacity, removes the least recently used entry.
     *
     * @param key            the key
     * @param value          the value
     * @param expirySeconds  seconds until expiration (-1 for no expiry)
     * @throws IllegalArgumentException if key/value is null or expirySeconds is invalid
     */
    public synchronized void put(K key, V value, long expirySeconds) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }

        if (expirySeconds < -1) {
            throw new IllegalArgumentException("Expiry seconds must be >= -1");
        }

        // If key already exists, update and move to front
        if (cache.containsKey(key)) {
            Node node = cache.get(key);
            node.val = value;
            if (expirySeconds > 0) {
                node.expiryTimeMillis = System.currentTimeMillis() + (expirySeconds * 1000);
            } else if (expirySeconds == -1) {
                node.expiryTimeMillis = -1;  // Remove expiry
            }
            removeNode(node);
            addToFront(node);
            log.debug("Updated cache for key: {}, expiry: {} seconds", key, expirySeconds);
            return;
        }

        // Evict least recently used if at capacity
        if (cache.size() >= capacity) {
            Node lruNode = tail.prev;
            removeNode(lruNode);
            cache.remove(lruNode.key);
            log.debug("Evicted LRU key: {}", lruNode.key);
        }

        // Create and add new node
        Node newNode = new Node(key, value);
        if (expirySeconds > 0) {
            newNode.expiryTimeMillis = System.currentTimeMillis() + (expirySeconds * 1000);
        }
        addToFront(newNode);
        cache.put(key, newNode);
        log.debug("Added new key to cache: {}, expiry: {} seconds", key, expirySeconds);
    }

    /**
     * Sets an expiration time for an existing key.
     *
     * @param key     the key
     * @param seconds seconds until expiration
     * @return true if key exists and expiry was set, false if key doesn't exist
     * @throws IllegalArgumentException if seconds <= 0
     */
    public synchronized boolean expire(K key, long seconds) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if (seconds <= 0) {
            throw new IllegalArgumentException("Seconds must be > 0");
        }

        if (!cache.containsKey(key)) {
            log.debug("Key not found for expiry: {}", key);
            return false;
        }

        Node node = cache.get(key);
        node.expiryTimeMillis = System.currentTimeMillis() + (seconds * 1000);
        log.debug("Set expiry for key: {}, seconds: {}", key, seconds);
        return true;
    }

    /**
     * Returns the remaining time to live for a key in seconds.
     *
     * @param key the key
     * @return remaining seconds (0-N), -1 if no expiry set, -2 if key doesn't exist
     */
    public synchronized long ttl(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if (!cache.containsKey(key)) {
            return -2;  // Key doesn't exist
        }

        Node node = cache.get(key);

        if (node.expiryTimeMillis == -1) {
            return -1;  // No expiry set
        }

        long remainingMillis = node.expiryTimeMillis - System.currentTimeMillis();

        if (remainingMillis <= 0) {
            // Expired, remove it
            removeNode(node);
            cache.remove(key);
            return -2;
        }

        return remainingMillis / 1000;  // Convert to seconds
    }

    /**
     * Checks if a key exists and has not expired.
     *
     * @param key the key
     * @return true if key exists and is not expired, false otherwise
     */
    public synchronized boolean exists(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if (!cache.containsKey(key)) {
            return false;
        }

        Node node = cache.get(key);

        if (isExpired(node)) {
            removeNode(node);
            cache.remove(key);
            return false;
        }

        return true;
    }

    /**
     * Removes a key from the cache.
     *
     * @param key the key to remove
     * @return true if key existed and was removed, false otherwise
     */
    public synchronized boolean delete(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if (!cache.containsKey(key)) {
            log.debug("Key not found for deletion: {}", key);
            return false;
        }

        Node node = cache.get(key);
        removeNode(node);
        cache.remove(key);
        log.debug("Deleted key from cache: {}", key);
        return true;
    }

    /**
     * Returns the current number of entries in the cache.
     *
     * @return the size of the cache
     */
    public synchronized int size() {
        return cache.size();
    }

    /**
     * Clears all entries from the cache.
     */
    public synchronized void clear() {
        cache.clear();
        head.next = tail;
        tail.prev = head;
        log.debug("Cache cleared");
    }

    /**
     * Returns the maximum capacity of the cache.
     *
     * @return the capacity
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Periodically cleans up expired entries from the cache.
     * Runs every 30 seconds.
     */
    @Scheduled(fixedDelay = 30000)
    public synchronized void cleanupExpiredEntries() {
        List<K> keysToDelete = new ArrayList<>();

        // Find all expired keys
        for (Map.Entry<K, Node> entry : cache.entrySet()) {
            if (isExpired(entry.getValue())) {
                keysToDelete.add(entry.getKey());
            }
        }

        // Remove expired keys
        for (K key : keysToDelete) {
            Node node = cache.get(key);
            removeNode(node);
            cache.remove(key);
        }

        if (keysToDelete.size() > 0) {
            log.info("Cleanup: removed {} expired keys, cache size now: {}",
                    keysToDelete.size(), cache.size());
        }
    }

    /**
     * Checks if a node has expired.
     *
     * @param node the node to check
     * @return true if node has expired, false otherwise
     */
    private boolean isExpired(Node node) {
        if (node.expiryTimeMillis == -1) {
            return false;  // No expiry set
        }
        return System.currentTimeMillis() > node.expiryTimeMillis;
    }

    /**
     * Adds a node to the front of the list (most recently used position).
     */
    private void addToFront(Node node) {
        Node temp = head.next;
        head.next = node;
        node.prev = head;
        node.next = temp;
        temp.prev = node;
    }

    /**
     * Removes a node from the doubly-linked list.
     */
    private void removeNode(Node node) {
        Node prevNode = node.prev;
        Node nextNode = node.next;
        prevNode.next = nextNode;
        nextNode.prev = prevNode;
    }
}