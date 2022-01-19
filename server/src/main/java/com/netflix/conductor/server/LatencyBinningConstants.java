package com.netflix.conductor.server;

/**
 * Class containing constants for latency binning.
 */
public final class LatencyBinningConstants {
    public static final String LATENCY_BIN_LOW_KEY = "latencySloBinLow";
    public static final String LATENCY_BIN_HIGH_KEY = "latencySloBinHigh";
    public static final String LATENCY_BIN_STEP_KEY = "latencySloBinStep";

    public static final int MAX_BINS = 16;

    private LatencyBinningConstants() {}
}
