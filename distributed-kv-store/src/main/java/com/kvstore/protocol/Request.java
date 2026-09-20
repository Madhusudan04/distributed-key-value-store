package com.kvstore.protocol;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a parsed client request.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Request {
    private Command command;
    private List<String> args = new ArrayList<>();

    /**
     * Gets argument at index.
     */
    public String getArg(int index) {
        if (index >= args.size()) {
            return null;
        }
        return args.get(index);
    }

    /**
     * Gets argument count.
     */
    public int getArgCount() {
        return args.size();
    }
}