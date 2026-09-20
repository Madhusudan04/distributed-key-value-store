package com.kvstore.protocol;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Encodes Response objects into binary format.
 *
 * Binary Format:
 * [status_code:1 byte][data_length:2 bytes][data]...[int_value:8 bytes]
 */
@Slf4j
public class ResponseEncoder {

    /**
     * Encodes a response to binary format.
     */
    public static byte[] encode(Response response) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Write status code
        dos.writeByte(response.getStatus().code);

        // Write data (if present)
        if (response.getData() != null) {
            byte[] dataBytes = response.getData().getBytes("UTF-8");
            dos.writeShort(dataBytes.length);
            dos.write(dataBytes);
        } else {
            dos.writeShort(0);
        }

        // Write integer value
        dos.writeLong(response.getIntValue());

        dos.flush();
        return baos.toByteArray();
    }
}