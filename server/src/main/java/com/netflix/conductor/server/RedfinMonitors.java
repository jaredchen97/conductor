package com.netflix.conductor.server;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.histogram.PercentileTimer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Copied + modified from {@link com.netflix.conductor.metrics.Monitors} for use in Redfin code. Unfortunately,
 * the methods in the conductor-core Monitors class that we'd want to use are private, so we duplicate them here
 * instead (with some modifications).
 */
public class RedfinMonitors {
    private static final Registry registry = Spectator.globalRegistry();
    private static final Map<String, Map<Map<String, String>, Counter>> counters = new ConcurrentHashMap<>();
    private static final Map<String, Map<Map<String, String>, PercentileTimer>> timers = new ConcurrentHashMap<>();

    //region Methods for specific metrics

    public static void recordExecutionAttempt(Workflow workflow) {
        workflowCounter("redfin.wf_execution_attempt", workflow);
    }

    public static void recordExecutionSuccess(Workflow workflow) {
        workflowCounter("redfin.wf_execution_success", workflow);
    }

    public static void recordExecutionFailure(Workflow workflow) {
        workflowCounter("redfin.wf_execution_failure", workflow);
    }

    public static void recordExecutionAttempt(Task task) {
        taskCounter("redfin.task_execution_attempt", task);
    }

    public static void recordExecutionSuccess(Task task) {
        taskCounter("redfin.task_execution_success", task);
    }

    public static void recordExecutionFailure(Task task) {
        taskCounter("redfin.task_execution_failure", task);
    }

    public static void recordTaskLatency(Task task, long durationMs) {
        taskTimer("redfin.task_latency", durationMs, task);
    }

    //endregion

    // region General public methods
    public static void workflowCounter(String name, Workflow workflow) {
        counter(name,
                "workflowName", workflow.getWorkflowName()
        );
    }

    public static void taskCounter(String name, Task task) {
        counter(name,
                "taskName", task.getTaskDefName()
        );
    }

    public static void taskTimer(String name, long durationMs, Task task) {
        recordDuration(name, durationMs,
                "taskName", task.getTaskDefName()
        );
    }

    public static void counter(String name, String... additionalTags) {
        getCounter(name, additionalTags).increment();
    }

    public static void recordDuration(String name, long durationMs, String... additionalTags) {
        getTimer(name, additionalTags).record(durationMs, TimeUnit.MILLISECONDS);
    }

    //endregion

    private static Counter getCounter(String name, String... additionalTags) {
        Map<String, String> tags = toMap(additionalTags);

        return counters.computeIfAbsent(name, s -> new ConcurrentHashMap<>()).computeIfAbsent(tags, t -> {
            Id id = registry.createId(name, tags);
            return registry.counter(id);
        });
    }

    private static Timer getTimer(String name, String... additionalTags) {
        Map<String, String> tags = toMap(additionalTags);
        return timers.computeIfAbsent(name, s -> new ConcurrentHashMap<>()).computeIfAbsent(tags, t -> {
            Id id = registry.createId(name, tags);
            return PercentileTimer.get(registry, id);
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
