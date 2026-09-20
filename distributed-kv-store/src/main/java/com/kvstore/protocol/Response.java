package com.kvstore.protocol;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a server response to a client request.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Response {
    public enum Status {
        OK(1, "OK"),
        ERROR(2, "ERROR"),
        NIL(3, "NIL"),
        INTEGER(4, "INTEGER"),
        BULK_STRING(5, "BULK_STRING");

        public final int code;
        public final String description;

        Status(int code, String description) {
            this.code = code;
            this.description = description;
        }

        public static Status fromCode(int code) {
            for (Status status : values()) {
                if (status.code == code) {
                    return status;
                }
            }
            throw new IllegalArgumentException("Unknown status code: " + code);
        }
    }

    private Status status;
    private String data;  // Can be error message or response data
    private long intValue;  // For integer responses (TTL, size, etc.)

    public static Response ok(String data) {
        return new Response(Status.OK, data, 0);
    }

    public static Response ok() {
        return new Response(Status.OK, "OK", 0);
    }

    public static Response error(String message) {
        return new Response(Status.ERROR, message, 0);
    }

    public static Response nil() {
        return new Response(Status.NIL, null, 0);
    }

    public static Response integer(long value) {
        return new Response(Status.INTEGER, null, value);
    }

    public static Response bulkString(String data) {
        return new Response(Status.BULK_STRING, data, 0);
    }
}