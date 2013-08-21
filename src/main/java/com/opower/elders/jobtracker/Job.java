package com.opower.elders.jobtracker;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Exposes what we need from a RunningJob. To do that, it also extracts the counters as a map.
 *
 * @author alexandre.normand
 */
public class Job implements Serializable {
    private static final String COUNTER_KEY_FORMAT = "%s:%s";

    private String name;

    @JsonIgnore
    private Map<String, Long> counters = new HashMap<String, Long>();

    private float mapProgress;
    private float reduceProgress;

    public Job() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Long> getCounters() {
        return counters;
    }

    public void setCounters(Map<String, Long> counters) {
        this.counters = counters;
    }

    public float getMapProgress() {
        return mapProgress;
    }

    public void setMapProgress(float mapProgress) {
        this.mapProgress = mapProgress;
    }

    public float getReduceProgress() {
        return reduceProgress;
    }

    public void setReduceProgress(float reduceProgress) {
        this.reduceProgress = reduceProgress;
    }

    public Long getInputRecords() {
        Long inputRecords = this.getCounters().get("org.apache.hadoop.mapreduce.TaskCounter:MAP_INPUT_RECORDS");
        return inputRecords != null ? inputRecords : 0L;
    }

    public Long getOutputRecords() {
        Long output = this.getCounters().get("org.apache.hadoop.mapreduce.TaskCounter:REDUCE_OUTPUT_RECORDS");
        return output != null ? output : 0L;
    }

    public static Job fromRunningJobAndJobStatus(JobStatus jobStatus, RunningJob runningJob) throws IOException {
        Map<String, Long> counters = new HashMap<String, Long>();
        Collection<String> groupNames = runningJob.getCounters().getGroupNames();
        for (String group : groupNames) {
            Iterator<Counters.Counter> iterator = runningJob.getCounters().getGroup(group).iterator();
            while (iterator.hasNext()) {
                Counters.Counter counter = iterator.next();
                String counterKey = String.format(COUNTER_KEY_FORMAT, group, counter.getName());
                counters.put(counterKey, counter.getValue());
            }
        }

        Job job = new Job();
        job.setCounters(counters);
        job.setName(runningJob.getJobName());
        job.setMapProgress(jobStatus.mapProgress() * 100);
        job.setReduceProgress(jobStatus.reduceProgress() * 100);

        return job;
    }
}
