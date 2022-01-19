package com.netflix.conductor.server;

import com.google.inject.AbstractModule;
import com.netflix.conductor.core.execution.WorkflowStatusListener;

/**
 * Module to bind {@link RedfinConductorServerWorkflowStatusListener}.
 */
public class RedfinConductorServerWorkflowStatusListenerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(WorkflowStatusListener.class).to(RedfinConductorServerWorkflowStatusListener.class);
    }
}
