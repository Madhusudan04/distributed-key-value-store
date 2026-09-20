package com.kvstore.persistence;

import com.kvstore.cache.LRUCache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Reads and replays append-only file on startup to recover data.
 *
 * Supported commands:
 *   SET key value [seconds]
 *   DEL key
 *   EXPIRE key seconds
 */
@Slf4j
@Component
public class AOFReader {

    private final String aofFilePath;
    private final LRUCache<String, String> cache;

    public AOFReader(
            @Value("${kvstore.persistence.aof-file:data/appendonly.aof}") String aofFilePath,
            LRUCache<String, String> cache) {
        this.aofFilePath = aofFilePath;
        this.cache = cache;
    }

    /**
     * Replays AOF on startup to recover data.
     */
    @PostConstruct
    public void replayAOF() {
        Path filePath = Paths.get(aofFilePath);

        if (!Files.exists(filePath)) {
            log.info("No AOF file found, starting with empty cache");
            return;
        }

        try {
            long startTime = System.currentTimeMillis();
            int commandCount = 0;

            log.info("Starting AOF replay from: {}", aofFilePath);

            try (BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()))) {
                String line;
                int lineNumber = 0;

                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    line = line.trim();

                    if (line.isEmpty() || line.startsWith("#")) {
                        continue;  // Skip empty lines and comments
                    }

                    try {
                        replayCommand(line);
                        commandCount++;
                    } catch (Exception e) {
                        log.error("Error replaying command at line {}: {}", lineNumber, line, e);
                    }
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            log.info("AOF replay completed: {} commands in {}ms", commandCount, duration);
            log.info("Cache now has {} entries", cache.size());

        } catch (IOException e) {
            log.error("Failed to read AOF file", e);
            throw new RuntimeException("Failed to replay AOF", e);
        }
    }

    /**
     * Replays a single command from the AOF.
     */
    private void replayCommand(String line) {
        String[] parts = line.split("\\s+", 3);  // Max 3 parts for SET command

        if (parts.length == 0) {
            return;
        }

        String command = parts[0].toUpperCase();

        switch (command) {
            case "SET":
                replaySet(parts);
                break;
            case "DEL":
                replayDel(parts);
                break;
            case "EXPIRE":
                replayExpire(parts);
                break;
            default:
                log.warn("Unknown command in AOF: {}", command);
        }
    }

    /**
     * Replays SET command: SET key value [seconds]
     */
    private void replaySet(String[] parts) {
        if (parts.length < 3) {
            log.warn("Invalid SET command: needs key and value");
            return;
        }

        String key = parts[1];
        String value = parts[2];
        long expirySeconds = -1;

        // Parse expiry if present
        if (parts.length > 3) {
            try {
                expirySeconds = Long.parseLong(parts[3]);
            } catch (NumberFormatException e) {
                log.warn("Invalid expiry seconds in SET command: {}", parts[3]);
            }
        }

        cache.put(key, value, expirySeconds);
        log.debug("Replayed SET {} = {} (expiry: {}s)", key, value, expirySeconds);
    }

    /**
     * Replays DEL command: DEL key
     */
    private void replayDel(String[] parts) {
        if (parts.length < 2) {
            log.warn("Invalid DEL command: needs key");
            return;
        }

        String key = parts[1];
        cache.delete(key);
        log.debug("Replayed DEL {}", key);
    }

    /**
     * Replays EXPIRE command: EXPIRE key seconds
     */
    private void replayExpire(String[] parts) {
        if (parts.length < 3) {
            log.warn("Invalid EXPIRE command: needs key and seconds");
            return;
        }

        String key = parts[1];
        try {
            long seconds = Long.parseLong(parts[2]);
            cache.expire(key, seconds);
            log.debug("Replayed EXPIRE {} {}s", key, seconds);
        } catch (NumberFormatException e) {
            log.warn("Invalid seconds in EXPIRE command: {}", parts[2]);
        }
    }
}