package com.netflix.conductor.server;

/**
 * Data class describing a binning configuration.
 */
public class BinConfig {
    private final long low;
    private final long high;
    private final long step;

    public BinConfig(
            long low,
            long high,
            long step) throws BinConfigurationException {
        this.low = low;
        this.high = high;
        this.step = step;

        // we limit the number of bins for the health of our stats stack
        if ((high - low) / step > LatencyBinningConstants.MAX_BINS) {
            throw new BinConfigurationException("too many bins");
        }

        // the range must be equally divisible by the step or we can't create equal sized bins
        if ((high - low) % step > 0) {
            throw new BinConfigurationException("unequal bins");
        }
    }

    public long getLow() {
        return low;
    }

    public long getHigh() {
        return high;
    }

    public long getStep() {
        return step;
    }
}
