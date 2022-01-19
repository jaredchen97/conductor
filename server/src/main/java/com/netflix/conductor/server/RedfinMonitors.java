package com.netflix.conductor.server;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copied + modified from {@link com.netflix.conductor.metrics.Monitors} for use in Redfin code. Unfortunately,
 * the methods in the conductor-core Monitors class that we'd want to use are private, so we duplicate them here
 * instead (with some modifications).
 */
public class RedfinMonitors {
    private static final Registry registry = Spectator.globalRegistry();
    private static final Map<String, Map<Map<String, String>, Counter>> counters = new ConcurrentHashMap<>();

    public static void recordExecutionAttempt(Workflow workflow) {
        workflowCounter("redfin.execution_attempt", workflow);
    }

    public static void recordExecutionSuccess(Workflow workflow) {
        workflowCounter("redfin.execution_success", workflow);
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
            if (!tv.isEmpty()) {
                tags.put(tk, tv);
            }
            j++;
        }
        return tags;
    }
}
