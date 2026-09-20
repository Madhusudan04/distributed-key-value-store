package com.kvstore.server;

import com.kvstore.cache.LRUCache;
import com.kvstore.persistence.AOFWriter;
import com.kvstore.protocol.*;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Socket;

/**
 * Handles a single client connection.
 * Runs in a separate thread from the ThreadPool.
 * Reads requests, executes commands, and sends responses.
 * Writes all write operations to AOF for durability.
 */
@Slf4j
public class ConnectionHandler implements Runnable {

    private final Socket socket;
    private final LRUCache<String, String> cache;
    private final AOFWriter aofWriter;
    private volatile boolean running = true;

    public ConnectionHandler(Socket socket, LRUCache<String, String> cache, AOFWriter aofWriter) {
        this.socket = socket;
        this.cache = cache;
        this.aofWriter = aofWriter;
    }

    @Override
    public void run() {
        String clientAddress = socket.getInetAddress().getHostAddress();
        int clientPort = socket.getPort();

        log.info("Client connected: {}:{}", clientAddress, clientPort);

        try (InputStream in = socket.getInputStream();
             OutputStream out = socket.getOutputStream()) {

            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    // Read request size (4 bytes)
                    byte[] sizeBytes = new byte[4];
                    int bytesRead = in.read(sizeBytes);

                    if (bytesRead == -1) {
                        // Client closed connection
                        log.info("Client disconnected: {}:{}", clientAddress, clientPort);
                        break;
                    }

                    if (bytesRead < 4) {
                        log.warn("Invalid request size header from {}:{}", clientAddress, clientPort);
                        continue;
                    }

                    int requestSize = readInt(sizeBytes);

                    if (requestSize <= 0 || requestSize > 1_000_000) {  // Max 1MB request
                        log.warn("Invalid request size {} from {}:{}", requestSize, clientAddress, clientPort);
                        sendErrorResponse(out, "Invalid request size");
                        continue;
                    }

                    // Read request data
                    byte[] requestData = new byte[requestSize];
                    int totalRead = 0;
                    while (totalRead < requestSize) {
                        int read = in.read(requestData, totalRead, requestSize - totalRead);
                        if (read == -1) {
                            log.warn("Client disconnected while reading request from {}:{}", clientAddress, clientPort);
                            break;
                        }
                        totalRead += read;
                    }

                    if (totalRead < requestSize) {
                        log.warn("Incomplete request from {}:{}", clientAddress, clientPort);
                        break;
                    }

                    // Decode and process request
                    try {
                        Request request = RequestDecoder.decode(requestData);
                        log.debug("Received command {} with {} args from {}:{}",
                                request.getCommand(), request.getArgCount(), clientAddress, clientPort);

                        Response response = processCommand(request);

                        // Encode and send response
                        byte[] responseData = ResponseEncoder.encode(response);
                        byte[] sizeHeader = writeInt(responseData.length);

                        out.write(sizeHeader);
                        out.write(responseData);
                        out.flush();

                        log.debug("Sent response with status {} to {}:{}",
                                response.getStatus(), clientAddress, clientPort);

                    } catch (IOException e) {
                        log.error("Error processing request from {}:{}", clientAddress, clientPort, e);
                        sendErrorResponse(out, "Error processing request: " + e.getMessage());
                    }

                } catch (IOException e) {
                    log.error("Connection error with {}:{}", clientAddress, clientPort, e);
                    break;
                }
            }

        } catch (IOException e) {
            log.error("Socket error for {}:{}", clientAddress, clientPort, e);
        } finally {
            try {
                socket.close();
                log.info("Connection closed: {}:{}", clientAddress, clientPort);
            } catch (IOException e) {
                log.error("Error closing socket for {}:{}", clientAddress, clientPort, e);
            }
        }
    }

    /**
     * Processes a command and returns a response.
     */
    private Response processCommand(Request request) {
        try {
            switch (request.getCommand()) {
                case SET:
                    return handleSet(request);
                case GET:
                    return handleGet(request);
                case DEL:
                    return handleDel(request);
                case EXISTS:
                    return handleExists(request);
                case EXPIRE:
                    return handleExpire(request);
                case TTL:
                    return handleTtl(request);
                case PING:
                    return Response.ok("PONG");
                case FLUSHALL:
                    return handleFlushAll(request);
                case INFO:
                    return handleInfo(request);
                default:
                    return Response.error("Unknown command: " + request.getCommand());
            }
        } catch (Exception e) {
            log.error("Error processing command {}", request.getCommand(), e);
            return Response.error("Internal error: " + e.getMessage());
        }
    }

    /**
     * Handles SET command: SET key value [seconds]
     */
    private Response handleSet(Request request) {
        if (request.getArgCount() < 2) {
            return Response.error("SET requires at least 2 arguments (key, value)");
        }

        String key = request.getArg(0);
        String value = request.getArg(1);
        long expirySeconds = -1;

        if (request.getArgCount() >= 3) {
            try {
                expirySeconds = Long.parseLong(request.getArg(2));
            } catch (NumberFormatException e) {
                return Response.error("Invalid expiry seconds: " + request.getArg(2));
            }
        }

        try {
            cache.put(key, value, expirySeconds);

            // ← ADDED: Write to AOF
            aofWriter.writeSet(key, value, expirySeconds);

            log.debug("SET {} = {} (expiry: {} seconds)", key, value, expirySeconds);
            return Response.ok();
        } catch (Exception e) {
            return Response.error("Failed to set key: " + e.getMessage());
        }
    }

    /**
     * Handles GET command: GET key
     */
    private Response handleGet(Request request) {
        if (request.getArgCount() < 1) {
            return Response.error("GET requires 1 argument (key)");
        }

        String key = request.getArg(0);

        try {
            String value = cache.get(key);
            if (value == null) {
                log.debug("GET {} = nil", key);
                return Response.nil();
            }
            log.debug("GET {} = {}", key, value);
            return Response.bulkString(value);
        } catch (Exception e) {
            return Response.error("Failed to get key: " + e.getMessage());
        }
    }

    /**
     * Handles DEL command: DEL key
     */
    private Response handleDel(Request request) {
        if (request.getArgCount() < 1) {
            return Response.error("DEL requires 1 argument (key)");
        }

        String key = request.getArg(0);

        try {
            boolean deleted = cache.delete(key);

            // ← ADDED: Write to AOF only if key was deleted
            if (deleted) {
                aofWriter.writeDel(key);
            }

            log.debug("DEL {} = {}", key, deleted ? 1 : 0);
            return Response.integer(deleted ? 1 : 0);
        } catch (Exception e) {
            return Response.error("Failed to delete key: " + e.getMessage());
        }
    }

    /**
     * Handles EXISTS command: EXISTS key
     */
    private Response handleExists(Request request) {
        if (request.getArgCount() < 1) {
            return Response.error("EXISTS requires 1 argument (key)");
        }

        String key = request.getArg(0);

        try {
            boolean exists = cache.exists(key);
            log.debug("EXISTS {} = {}", key, exists ? 1 : 0);
            return Response.integer(exists ? 1 : 0);
        } catch (Exception e) {
            return Response.error("Failed to check key existence: " + e.getMessage());
        }
    }

    /**
     * Handles EXPIRE command: EXPIRE key seconds
     */
    private Response handleExpire(Request request) {
        if (request.getArgCount() < 2) {
            return Response.error("EXPIRE requires 2 arguments (key, seconds)");
        }

        String key = request.getArg(0);
        long seconds;

        try {
            seconds = Long.parseLong(request.getArg(1));
        } catch (NumberFormatException e) {
            return Response.error("Invalid seconds: " + request.getArg(1));
        }

        try {
            boolean success = cache.expire(key, seconds);

            // ← ADDED: Write to AOF only if expire was successful
            if (success) {
                aofWriter.writeExpire(key, seconds);
            }

            log.debug("EXPIRE {} {} = {}", key, seconds, success ? 1 : 0);
            return Response.integer(success ? 1 : 0);
        } catch (Exception e) {
            return Response.error("Failed to set expiry: " + e.getMessage());
        }
    }

    /**
     * Handles TTL command: TTL key
     */
    private Response handleTtl(Request request) {
        if (request.getArgCount() < 1) {
            return Response.error("TTL requires 1 argument (key)");
        }

        String key = request.getArg(0);

        try {
            long ttl = cache.ttl(key);
            log.debug("TTL {} = {}", key, ttl);
            return Response.integer(ttl);
        } catch (Exception e) {
            return Response.error("Failed to get TTL: " + e.getMessage());
        }
    }

    /**
     * Handles FLUSHALL command: FLUSHALL
     */
    private Response handleFlushAll(Request request) {
        try {
            cache.clear();
            log.info("FLUSHALL executed");
            return Response.ok();
        } catch (Exception e) {
            return Response.error("Failed to flush cache: " + e.getMessage());
        }
    }

    /**
     * Handles INFO command: INFO
     */
    private Response handleInfo(Request request) {
        try {
            String info = String.format(
                    "# KV Store Info\r\n" +
                            "cache_size=%d\r\n" +
                            "cache_capacity=%d\r\n" +
                            "aof_file_size=%d\r\n" +
                            "timestamp=%d\r\n",
                    cache.size(),
                    cache.getCapacity(),
                    aofWriter.getFileSize(),
                    System.currentTimeMillis()
            );
            log.debug("INFO command executed");
            return Response.bulkString(info);
        } catch (Exception e) {
            return Response.error("Failed to get info: " + e.getMessage());
        }
    }

    /**
     * Sends an error response to the client.
     */
    private void sendErrorResponse(OutputStream out, String message) {
        try {
            Response errorResponse = Response.error(message);
            byte[] responseData = ResponseEncoder.encode(errorResponse);
            byte[] sizeHeader = writeInt(responseData.length);

            out.write(sizeHeader);
            out.write(responseData);
            out.flush();
        } catch (IOException e) {
            log.error("Error sending error response", e);
        }
    }

    /**
     * Writes an integer as 4 bytes (big-endian).
     */
    private byte[] writeInt(int value) {
        return new byte[]{
                (byte) ((value >> 24) & 0xFF),
                (byte) ((value >> 16) & 0xFF),
                (byte) ((value >> 8) & 0xFF),
                (byte) (value & 0xFF)
        };
    }

    /**
     * Reads an integer from 4 bytes (big-endian).
     */
    private int readInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                (bytes[3] & 0xFF);
    }

    /**
     * Gracefully shutdown the handler.
     */
    public void shutdown() {
        running = false;
        try {
            socket.close();
        } catch (IOException e) {
            log.error("Error closing socket during shutdown", e);
        }
    }
}