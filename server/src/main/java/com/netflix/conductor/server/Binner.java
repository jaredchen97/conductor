package com.netflix.conductor.server;

import java.util.HashSet;
import java.util.Set;

/**
 * Copied (with modifications) from the {@code Binner} class in the Redfin monitoring-core/statsd library. Since this
 * utility is pretty slim, we copy it so we don't need to add a dependency on the Redfin library here.
 */
public class Binner {
    private final BinConfig config;

    public Binner(BinConfig config) {
        this.config = config;
    }

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
