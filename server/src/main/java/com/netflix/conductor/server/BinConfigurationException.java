package com.netflix.conductor.server;

/**
 * Exception thrown when an invalid bin configuration is provided.
 */
public class BinConfigurationException extends Exception {
    public BinConfigurationException(String message) {
        super(message);
    }

    public BinConfigurationException(Throwable cause) {
        super(cause);
    }

    public BinConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
