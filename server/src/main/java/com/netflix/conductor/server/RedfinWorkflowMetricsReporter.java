package com.netflix.conductor.server;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Class to report various additional workflow metrics that we care about.
 */
public class RedfinWorkflowMetricsReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedfinWorkflowMetricsReporter.class);

    /**
     * Records additional metrics for a workflow after it is finalized
     * @param workflow workflow
     */
    public static void recordMetrics(Workflow workflow) {
        recordWorkflowMetrics(workflow);
        recordTaskMetrics(workflow);
    }

    /**
     * Records additional metrics that are scoped to the overall workflow, including:
     * - workflow success/fail/attempt counts
     * - latency bins
     *
     * @param workflow workflow
     */
    private static void recordWorkflowMetrics(Workflow workflow) {
        RedfinMonitors.recordExecutionAttempt(workflow);
        if (workflow.getStatus().isSuccessful()) {
            RedfinMonitors.recordExecutionSuccess(workflow);
            recordLatencyBins(workflow);
        } else {
            RedfinMonitors.recordExecutionFailure(workflow);
        }
    }

    /**
     * Records additional metrics for each task in a workflow, including:
     * - task success/fail/attempt counts
     * - latency
     *
     * @param workflow workflow
     */
    private static void recordTaskMetrics(Workflow workflow) {
        List<Task> tasks = workflow.getTasks();
        tasks.forEach((task) -> {
            RedfinMonitors.recordExecutionAttempt(task);
            if (task.getStatus().isSuccessful()) {
                RedfinMonitors.recordExecutionSuccess(task);
                recordTaskLatency(task, workflow);
            } else {
                RedfinMonitors.recordExecutionFailure(task);
            }
        });
    }

    /**
     * Record task latency, defined as the time between the workflow start time and the end time of task execution.
     *
     * @param task task
     * @param workflow workflow that the task ran within
     */
    private static void recordTaskLatency(Task task, Workflow workflow) {
        long taskEndTime = task.getEndTime();
        if (taskEndTime > 0) {
            long workflowStartTime = workflow.getStartTime();
            long executionTimeFromStart = taskEndTime - workflowStartTime;
            RedfinMonitors.recordTaskLatency(task, executionTimeFromStart);
        }
    }

    /**
     * Records workflow execution time, binned by user-defined buckets
     * @param workflow workflow
     */
    private static void recordLatencyBins(Workflow workflow) {
        long workflowExecutionTime = workflow.getEndTime() - workflow.getStartTime();
        try {
            getLatencyBinConfig(workflow).ifPresent(binConfig ->
                    new Binner(binConfig).calculateBins(workflowExecutionTime).forEach((bin) ->
                            RedfinMonitors.counter("redfin.wf_latency_bins",
                                    "workflowName", workflow.getWorkflowName(),
                                    "bin", bin
                            )));
        } catch (BinConfigurationException e) {
            LOGGER.warn("Invalid bin configuration", e);
        }
    }

    private static Optional<BinConfig> getLatencyBinConfig(Workflow workflow) throws BinConfigurationException {
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

    private static Optional<Long> extractLong(Map<String, Object> workflowInput, String key) throws NumberFormatException {
        return Optional.of(workflowInput)
                .map(i -> i.get(key))
                .map(String::valueOf)
                .map(Long::valueOf);
    }

    private RedfinWorkflowMetricsReporter() {}
}
