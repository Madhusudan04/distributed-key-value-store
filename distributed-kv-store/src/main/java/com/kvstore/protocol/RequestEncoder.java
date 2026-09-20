package com.kvstore.protocol;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Encodes Request objects into binary format for transmission.
 *
 * Binary Format:
 * [command_code:1 byte][arg_count:1 byte][arg1_length:2 bytes][arg1_data][arg2_length:2 bytes][arg2_data]...
 */
@Slf4j
public class RequestEncoder {

    /**
     * Encodes a request to binary format.
     */
    public static byte[] encode(Request request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Write command code
        dos.writeByte(request.getCommand().code);

        // Write argument count
        dos.writeByte(request.getArgCount());

        // Write each argument
        for (String arg : request.getArgs()) {
            if (arg == null) {
                dos.writeShort(-1);  // Null marker
            } else {
                byte[] argBytes = arg.getBytes("UTF-8");
                dos.writeShort(argBytes.length);
                dos.write(argBytes);
            }
        }

        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Creates a SET request.
     */
    public static Request createSet(String key, String value, long expireSeconds) {
        Request req = new Request();
        req.setCommand(Command.SET);
        req.getArgs().add(key);
        req.getArgs().add(value);
        if (expireSeconds > 0) {
            req.getArgs().add(String.valueOf(expireSeconds));
        }
        return req;
    }

    /**
     * Creates a GET request.
     */
    public static Request createGet(String key) {
        Request req = new Request();
        req.setCommand(Command.GET);
        req.getArgs().add(key);
        return req;
    }

    /**
     * Creates a DEL request.
     */
    public static Request createDel(String key) {
        Request req = new Request();
        req.setCommand(Command.DEL);
        req.getArgs().add(key);
        return req;
    }
}