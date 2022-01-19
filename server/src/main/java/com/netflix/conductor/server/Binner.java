package com.netflix.conductor.server;

import java.util.HashSet;
import java.util.Set;

/**
 * Utility class for binning metrics.
 *
 * Copied (with modifications) from the {@code Binner} class in the Redfin monitoring-core/statsd library. Since this
 * utility is pretty slim, we copy it so we don't need to add a dependency on the Redfin library here.
 */
public class Binner {
    private final BinConfig config;

    /**
     * Constructor
     * @param config configuration
     */
    public Binner(BinConfig config) {
        this.config = config;
    }

    /**
     * This helper returns all the bins that a given value belongs in.
     * Possible bin values start with {@code config.low} incrementing every {@code config.step} until
     * it reaches {@code config.high}
     * The value belongs in all bins where it's value is lower than the value of the bin.
     *
     * Each bin name is prefixed by {@code "cumulative."}
     *
     * eg:
     * config = {
     *   low = 100
     *   high = 300
     *   step = 100
     *   value = 231
     * }
     *
     * Possible bins = ["cumulative.100", "cumulative.200", "cumulative.300"]
     *
     * return value will be ["cumulative.300"] because this is the only bin that the value falls below
     *
     * Note: if {@code value} is greater than {@code high} we will return ["cumulative.hi"]
     *
     * @param value the metric value for which to determine bins
     * @return set of bins the metrics fits within
     */
    public Set<String> calculateBins(long value) {
        Set<String> bins = new HashSet<>();

        if (value > config.getHigh()) {
            bins.add("cumulative.hi");
            return bins;
        }

        // Add all bins that the value falls within
        for (long i = config.getLow(); i <= config.getHigh(); i += config.getStep()) {
            if (value <= i) {
                bins.add("cumulative." + i);
            }
        }

        return bins;
    }
}
