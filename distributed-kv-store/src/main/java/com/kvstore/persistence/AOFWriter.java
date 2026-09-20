package com.kvstore.persistence;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Writes all write operations to an append-only file for durability.
 *
 * Format: COMMAND key [value] [seconds]
 * Example:
 *   SET user-123 john 300
 *   SET order-456 data
 *   DEL payment-001
 *   EXPIRE user-123 60
 */
@Slf4j
@Component
public class AOFWriter {

    private final String aofFilePath;
    private final boolean fsyncEnabled;
    private final int fsyncIntervalMs;

    private BufferedWriter writer;
    private Path filePath;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private long lastFsyncTime = 0;
    private volatile boolean running = true;

    public AOFWriter(
            @Value("${kvstore.persistence.aof-file:data/appendonly.aof}") String aofFilePath,
            @Value("${kvstore.persistence.fsync-enabled:true}") boolean fsyncEnabled,
            @Value("${kvstore.persistence.fsync-interval:1000}") int fsyncIntervalMs) {
        this.aofFilePath = aofFilePath;
        this.fsyncEnabled = fsyncEnabled;
        this.fsyncIntervalMs = fsyncIntervalMs;
    }

    /**
     * Initializes the AOF writer and creates file if not exists.
     */
    @PostConstruct
    public void init() {
        try {
            filePath = Paths.get(aofFilePath);

            // Create parent directories if needed
            if (filePath.getParent() != null) {
                Files.createDirectories(filePath.getParent());
            }

            // Create file if not exists
            if (!Files.exists(filePath)) {
                Files.createFile(filePath);
                log.info("Created new AOF file: {}", aofFilePath);
            }

            // Open writer in append mode
            writer = new BufferedWriter(
                new FileWriter(filePath.toFile(), true),
                8192  // 8KB buffer
            );

            log.info("AOF Writer initialized: {}", aofFilePath);
            log.info("Fsync enabled: {}, interval: {}ms", fsyncEnabled, fsyncIntervalMs);
        } catch (IOException e) {
            log.error("Failed to initialize AOF writer", e);
            throw new RuntimeException("Failed to initialize AOF writer", e);
        }
    }

    /**
     * Appends a SET command to the log.
     * Format: SET key value [seconds]
     */
    public void writeSet(String key, String value, long expirySeconds) {
        if (!running) {
            return;
        }

        lock.readLock().lock();
        try {
            String command;
            if (expirySeconds > 0) {
                command = String.format("SET %s %s %d%n", key, value, expirySeconds);
            } else {
                command = String.format("SET %s %s%n", key, value);
            }

            writer.write(command);
            log.debug("AOF wrote: {}", command.trim());

            shouldFsync();
        } catch (IOException e) {
            log.error("Failed to write SET command to AOF", e);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Appends a DEL command to the log.
     * Format: DEL key
     */
    public void writeDel(String key) {
        if (!running) {
            return;
        }

        lock.readLock().lock();
        try {
            String command = String.format("DEL %s%n", key);
            writer.write(command);
            log.debug("AOF wrote: {}", command.trim());

            shouldFsync();
        } catch (IOException e) {
            log.error("Failed to write DEL command to AOF", e);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Appends an EXPIRE command to the log.
     * Format: EXPIRE key seconds
     */
    public void writeExpire(String key, long seconds) {
        if (!running) {
            return;
        }

        lock.readLock().lock();
        try {
            String command = String.format("EXPIRE %s %d%n", key, seconds);
            writer.write(command);
            log.debug("AOF wrote: {}", command.trim());

            shouldFsync();
        } catch (IOException e) {
            log.error("Failed to write EXPIRE command to AOF", e);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if fsync should be performed based on time interval.
     */
    private void shouldFsync() {
        if (!fsyncEnabled) {
            return;
        }

        long now = System.currentTimeMillis();
        if (now - lastFsyncTime >= fsyncIntervalMs) {
            try {
                writer.flush();
                lastFsyncTime = now;
                log.debug("AOF fsynced to disk");
            } catch (IOException e) {
                log.error("Failed to fsync AOF", e);
            }
        }
    }

    /**
     * Force flush and sync to disk immediately.
     */
    public void forceSync() {
        lock.readLock().lock();
        try {
            writer.flush();
            lastFsyncTime = System.currentTimeMillis();
            log.debug("AOF force synced to disk");
        } catch (IOException e) {
            log.error("Failed to force sync AOF", e);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Closes the AOF writer gracefully.
     */
    @PreDestroy
    public void shutdown() {
        running = false;

        lock.writeLock().lock();
        try {
            if (writer != null) {
                writer.flush();
                writer.close();
                log.info("AOF Writer shut down");
            }
        } catch (IOException e) {
            log.error("Error closing AOF writer", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the AOF file path.
     */
    public String getFilePath() {
        return aofFilePath;
    }

    /**
     * Returns the file size in bytes.
     */
    public long getFileSize() {
        try {
            if (Files.exists(filePath)) {
                return Files.size(filePath);
            }
        } catch (IOException e) {
            log.error("Error getting AOF file size", e);
        }
        return 0;
    }
}