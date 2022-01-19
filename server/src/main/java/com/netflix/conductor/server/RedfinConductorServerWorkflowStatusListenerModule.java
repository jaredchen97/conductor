package com.netflix.conductor.server;

import com.google.inject.AbstractModule;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.Workflow.WorkflowStatus;
import com.netflix.conductor.core.execution.WorkflowStatusListener;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Module to inject Redfin custom workflow status listener for Conductor server.
 */
public class RedfinConductorServerWorkflowStatusListenerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(WorkflowStatusListener.class).to(RedfinConductorServerWorkflowStatusListener.class);
    }

    /**
     * Redfin custom workflow status listener for Conductor server. Handles firing metrics to support SLOs.
     */
    public static class RedfinConductorServerWorkflowStatusListener implements WorkflowStatusListener {
        private static final String LATENCY_SLO_CUTOFF_KEY = "latencySloCutoffMs";

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
                // Suppress any exceptions in this workflow listener; we don't want this to cause any issues for
                // workflow execution.
                LOGGER.warn("Exception thrown in onWorkflowFinalized listener", e);
            }
        }

        private void recordMetrics(Workflow workflow) {
            Monitors.recordExecutionAttempt(workflow);
            WorkflowStatus status = workflow.getStatus();
            if (status.isSuccessful()) {
                Monitors.recordExecutionSuccess(workflow);
            }
            recordLatencySloMetrics(workflow);
        }

        private void recordLatencySloMetrics(Workflow workflow) {
            Long latencySloCutoffMs = null;
            try {
                Map<String, Object> workflowInput = workflow.getInput();
                latencySloCutoffMs = Optional.of(workflowInput)
                        .map(i -> i.get(LATENCY_SLO_CUTOFF_KEY))
                        .map(String::valueOf)
                        .map(Long::valueOf)
                        .orElse(null);
            } catch (NumberFormatException e) {
                LOGGER.warn("Failed to cast latencySloCutoffMs to a long; no latency metrics reported.", e);
            }
            if (latencySloCutoffMs == null) {
                return;
            }
            long workflowExecutionTime = workflow.getEndTime() - workflow.getStartTime();
            if (workflowExecutionTime <= latencySloCutoffMs) {
                Monitors.recordLatencyGood(workflow);
            }
            Monitors.recordLatencyValid(workflow);
        }
    }

    /**
     * Copied + modified from {@link com.netflix.conductor.metrics.Monitors}. Unfortunately, methods in that class
     * that we want are private, so we duplicate (and modify) them here instead.
     */
    static class Monitors {
        private static final Registry registry = Spectator.globalRegistry();
        private static final Map<String, Map<Map<String, String>, Counter>> counters = new ConcurrentHashMap<>();

        public static void recordExecutionAttempt(Workflow workflow) {
            workflowCounter("redfin.execution_attempt", workflow);
        }

        public static void recordExecutionSuccess(Workflow workflow) {
            workflowCounter("redfin.execution_success", workflow);
        }

        public static void recordLatencyGood(Workflow workflow) {
            workflowCounter("redfin.latency_good", workflow);
        }

        public static void recordLatencyValid(Workflow workflow) {
            workflowCounter("redfin.latency_valid", workflow);
        }

        public static void workflowCounter(String name, Workflow workflow) {
            counter(name,
                    "workflowName", workflow.getWorkflowName()
            );
        }

        public static void counter(String name, String... additionalTags) {
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
                if(!tv.isEmpty()) {
                    tags.put(tk, tv);
                }
                j++;
            }
            return tags;
        }
    }
}
