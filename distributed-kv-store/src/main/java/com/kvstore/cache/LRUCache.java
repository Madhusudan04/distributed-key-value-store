package com.kvstore.cache;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * Thread-safe LRU (Least Recently Used) Cache implementation.
 *
 * Uses a doubly-linked list for O(1) LRU tracking and HashMap for O(1) key lookup.
 * All operations (get, put, delete) are O(1) time complexity.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 */
@Slf4j
public class LRUCache<K, V> {

    /**
     * Node in the doubly-linked list.
     * Maintains key, value, and pointers to previous and next nodes.
     */
    private class Node {
        K key;
        V val;
        Node next;
        Node prev;

        Node(K key, V val) {
            this.key = key;
            this.val = val;
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
     *
     * @param key the key to look up
     * @return the value associated with the key, or null if not found
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
        V value = node.val;

        // Move node to front (mark as recently used)
        removeNode(node);
        addToFront(node);

        log.debug("Cache hit for key: {}", key);
        return value;
    }

    /**
     * Puts a key-value pair into the cache.
     * If the key already exists, updates the value.
     * If cache is at capacity, removes the least recently used entry.
     *
     * @param key   the key
     * @param value the value
     * @throws IllegalArgumentException if key or value is null
     */
    public synchronized void put(K key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }

        // If key already exists, update and move to front
        if (cache.containsKey(key)) {
            Node node = cache.get(key);
            node.val = value;
            removeNode(node);
            addToFront(node);
            log.debug("Updated cache for key: {}", key);
            return;
        }

        // Evict least recently used if at capacity
        if (cache.size() >= capacity) {
            Node lruNode = tail.prev;
            removeNode(lruNode);
            cache.remove(lruNode.key);
            log.debug("Evicted LRU key: {}", lruNode.key);
        }

        // Add new node to front
        Node newNode = new Node(key, value);
        addToFront(newNode);
        cache.put(key, newNode);
        log.debug("Added new key to cache: {}", key);
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

    /**
     * Returns the maximum capacity of the cache.
     *
     * @return the capacity
     */
    public int getCapacity() {
        return capacity;
    }
}