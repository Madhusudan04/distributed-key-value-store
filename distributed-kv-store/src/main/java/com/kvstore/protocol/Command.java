package com.kvstore.protocol;

/**
 * Enum representing all supported cache commands.
 */
public enum Command {
    SET(1, "SET key value [seconds]"),
    GET(2, "GET key"),
    DEL(3, "DEL key"),
    EXISTS(4, "EXISTS key"),
    EXPIRE(5, "EXPIRE key seconds"),
    TTL(6, "TTL key"),
    PING(7, "PING"),
    FLUSHALL(8, "FLUSHALL"),
    INFO(9, "INFO"),
    SYNC(10,"SYNC");
    public final int code;
    public final String description;

    Command(int code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     * Get command by code.
     */
    public static Command fromCode(int code) {
        for (Command cmd : values()) {
            if (cmd.code == code) {
                return cmd;
            }
        }
        throw new IllegalArgumentException("Unknown command code: " + code);
    }
}