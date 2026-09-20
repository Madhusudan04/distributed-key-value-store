package com.kvstore.protocol;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Decodes binary format into Response objects.
 */
@Slf4j
public class ResponseDecoder {

    /**
     * Decodes binary data into a Response.
     */
    public static Response decode(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

        // Read status code
        int statusCode = dis.readByte();
        Response.Status status = Response.Status.fromCode(statusCode);

        // Read data
        short dataLength = dis.readShort();
        String responseData = null;
        if (dataLength > 0) {
            byte[] dataBytes = new byte[dataLength];
            dis.readFully(dataBytes);
            responseData = new String(dataBytes, "UTF-8");
        }

        // Read integer value
        long intValue = dis.readLong();

        Response response = new Response();
        response.setStatus(status);
        response.setData(responseData);
        response.setIntValue(intValue);

        return response;
    }
}