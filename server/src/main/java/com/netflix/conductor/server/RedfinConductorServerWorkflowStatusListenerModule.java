package com.netflix.conductor.server;

import com.google.inject.AbstractModule;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowStatusListener;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Module to bind {@link RedfinConductorServerWorkflowStatusListener}.
 */
public class RedfinConductorServerWorkflowStatusListenerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(WorkflowStatusListener.class).to(RedfinConductorServerWorkflowStatusListener.class);
    }

    /**
     * Custom workflow status listener for Redfin deployment of Conductor server.
     * Adds a few metrics to support workflow SLOs.
     */
    public static class RedfinConductorServerWorkflowStatusListener implements WorkflowStatusListener {

        private static final Logger LOGGER = LoggerFactory.getLogger(RedfinConductorServerWorkflowStatusListener.class);

        @Override
        public void onWorkflowCompleted(Workflow workflow) {
            // NO-OP
        }

        @Override
        public void onWorkflowTerminated(Workflow workflow) {
            // NO-OP
        }

        @Override
        public void onWorkflowFinalized(Workflow workflow) {
            try {
                recordMetrics(workflow);
            } catch (Exception e) {
                // Suppress any exceptions thrown; we don't want errors in our custom metrics code to cause issues
                // for workflow execution.
                LOGGER.error("Exception thrown in onWorkflowFinalized listener", e);
            }
        }

        private void recordMetrics(Workflow workflow) {
            Monitors.recordExecutionAttempt(workflow);
            if (workflow.getStatus().isSuccessful()) {
                Monitors.recordExecutionSuccess(workflow);
            }
            recordLatencyBins(workflow);
        }

        private void recordLatencyBins(Workflow workflow) {
            long workflowExecutionTime = workflow.getEndTime() - workflow.getStartTime();
            try {
                getLatencyBinConfig(workflow).ifPresent(binConfig ->
                        new Binner(binConfig).calculateBins(workflowExecutionTime).forEach((bin) ->
                                Monitors.counter("redfin.latency_bins",
                                        "workflowName", workflow.getWorkflowName(),
                                        "bin", bin
                                )));
            } catch (BinConfigurationException e) {
                LOGGER.warn("Invalid bin configuration", e);
            }
        }

        private Optional<BinConfig> getLatencyBinConfig(Workflow workflow) throws BinConfigurationException {
            try {
                Map<String, Object> workflowInput = workflow.getInput();

                Long latencyBinLow = extractLong(workflowInput, LatencyBinConstants.LATENCY_BIN_LOW_KEY).orElse(null);
                Long latencyBinHigh = extractLong(workflowInput, LatencyBinConstants.LATENCY_BIN_HIGH_KEY).orElse(null);
                Long latencyBinStep = extractLong(workflowInput, LatencyBinConstants.LATENCY_BIN_STEP_KEY).orElse(null);

                if (latencyBinLow == null || latencyBinHigh == null || latencyBinStep == null) {
                    return Optional.empty();
                }

                return Optional.of(new BinConfig(latencyBinLow, latencyBinHigh, latencyBinStep));
            } catch (NumberFormatException e) {
                throw new BinConfigurationException(e);
            }
        }

        private Optional<Long> extractLong(Map<String, Object> workflowInput, String key) throws NumberFormatException {
            return Optional.of(workflowInput)
                    .map(i -> i.get(key))
                    .map(String::valueOf)
                    .map(Long::valueOf);
        }
    }

    //region Latency Binning

    /**
     * Copied + modified from monitoring-core/statsd, to avoid adding dependencies on a Redfin library to Conductor
     * server.
     */
    static class Binner {
        private final BinConfig config;

        Binner(BinConfig config) {
            this.config = config;
        }

        Set<String> calculateBins(long value) {
            Set<String> bins = new HashSet<>();

            if (value > config.high) {
                bins.add("cumulative.hi");
                return bins;
            }

            // Add all bins that the value falls within
            for (long i = config.low; i <= config.high; i += config.step) {
                if (value <= i) {
                    bins.add("cumulative." + i);
                }
            }

            return bins;
        }
    }

    static class LatencyBinConstants {
        static final String LATENCY_BIN_LOW_KEY = "latencySloBinLow";
        static final String LATENCY_BIN_HIGH_KEY = "latencySloBinHigh";
        static final String LATENCY_BIN_STEP_KEY = "latencySloBinStep";
        static final int MAX_BINS = 16;
    }

    static class BinConfig {
        long low;
        long high;
        long step;

        BinConfig(
                long low,
                long high,
                long step) throws BinConfigurationException {
            this.low = low;
            this.high = high;
            this.step = step;

            // we limit the number of bins for the health of our stats stack
            if ((high - low) / step > LatencyBinConstants.MAX_BINS) {
                throw new BinConfigurationException("too many bins");
            }

            // the range must be equally divisible by the step or we can't create equal sized bins
            if ((high - low) % step > 0) {
                throw new BinConfigurationException("unequal bins");
            }
        }
    }

    static class BinConfigurationException extends Exception {
        BinConfigurationException(String message) {
            super(message);
        }

        BinConfigurationException(Throwable cause) {
            super(cause);
        }
    }
    //endregion

    //region Monitors

    /**
     * Copied + modified from {@link com.netflix.conductor.metrics.Monitors}. Unfortunately, methods in that class
     * that we want are private, so we duplicate (and modify) them here instead.
     */
    static class Monitors {
        private static final Registry registry = Spectator.globalRegistry();
        private static final Map<String, Map<Map<String, String>, Counter>> counters = new ConcurrentHashMap<>();

        static void recordExecutionAttempt(Workflow workflow) {
            workflowCounter("redfin.execution_attempt", workflow);
        }

        static void recordExecutionSuccess(Workflow workflow) {
            workflowCounter("redfin.execution_success", workflow);
        }

        static void workflowCounter(String name, Workflow workflow) {
            counter(name,
                    "workflowName", workflow.getWorkflowName()
            );
        }

        static void counter(String name, String... additionalTags) {
            getCounter(name, additionalTags).increment();
        }

        private static Counter getCounter(String name, String... additionalTags) {
            Map<String, String> tags = toMap(additionalTags);

            return counters.computeIfAbsent(name, s -> new ConcurrentHashMap<>()).computeIfAbsent(tags, t -> {
                Id id = registry.createId(name, tags);
                return registry.counter(id);
            });
        }

        private static Map<String, String> toMap(String... additionalTags) {
            Map<String, String> tags = new HashMap<>();
            for (int j = 0; j < additionalTags.length - 1; j++) {
                String tk = additionalTags[j];
                String tv = "" + additionalTags[j + 1];
                if (!tv.isEmpty()) {
                    tags.put(tk, tv);
                }
                j++;
            }
            return tags;
        }
    }
    //endregion
}
