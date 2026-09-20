package com.kvstore.protocol;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Decodes binary format into Request objects.
 */
@Slf4j
public class RequestDecoder {

    /**
     * Decodes binary data into a Request.
     */
    public static Request decode(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

        // Read command code
        int commandCode = dis.readByte();
        Command command = Command.fromCode(commandCode);

        // Read argument count
        int argCount = dis.readByte();

        Request request = new Request();
        request.setCommand(command);

        // Read each argument
        for (int i = 0; i < argCount; i++) {
            short argLength = dis.readShort();
            if (argLength == -1) {
                request.getArgs().add(null);
            } else {
                byte[] argBytes = new byte[argLength];
                dis.readFully(argBytes);
                String arg = new String(argBytes, "UTF-8");
                request.getArgs().add(arg);
            }
        }

        return request;
    }
}