package com.netflix.conductor.server;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            RedfinWorkflowMetricsReporter.recordMetrics(workflow);
        } catch (Exception e) {
            // Suppress any exceptions thrown; we don't want errors in our custom metrics code to cause issues
            // for workflow execution.
            LOGGER.error("Exception thrown in onWorkflowFinalized listener", e);
        }
    }
}
