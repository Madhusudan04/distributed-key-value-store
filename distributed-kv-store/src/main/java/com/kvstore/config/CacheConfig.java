package com.kvstore.config;

import com.kvstore.cache.LRUCache;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Configuration for LRU Cache and related components.
 */
@Configuration
@EnableScheduling  // Enable @Scheduled cleanup task
public class CacheConfig {

    @Bean
    public LRUCache<String, String> lruCache(
            @Value("${kvstore.cache.capacity:10000}") int capacity) {
        return new LRUCache<>(capacity);
    }
}