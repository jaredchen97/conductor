package com.netflix.conductor.server;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * Custom workflow status listener for Redfin deployment of Conductor server.
 * Adds metrics to support workflow SLOs.
 */
public class RedfinConductorServerWorkflowStatusListener implements WorkflowStatusListener {

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
        RedfinMonitors.recordExecutionAttempt(workflow);
        if (workflow.getStatus().isSuccessful()) {
            RedfinMonitors.recordExecutionSuccess(workflow);
        }
        recordLatencyBins(workflow);
    }

    private void recordLatencyBins(Workflow workflow) {
        long workflowExecutionTime = workflow.getEndTime() - workflow.getStartTime();
        try {
            getLatencyBinConfig(workflow).ifPresent(binConfig ->
                    new Binner(binConfig).calculateBins(workflowExecutionTime).forEach((bin) ->
                            RedfinMonitors.counter("redfin.latency_bins",
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

            Long latencyBinLow = extractLong(workflowInput, LatencyBinningConstants.LATENCY_BIN_LOW_KEY).orElse(null);
            Long latencyBinHigh = extractLong(workflowInput, LatencyBinningConstants.LATENCY_BIN_HIGH_KEY).orElse(null);
            Long latencyBinStep = extractLong(workflowInput, LatencyBinningConstants.LATENCY_BIN_STEP_KEY).orElse(null);

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
