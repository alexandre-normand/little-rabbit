package com.opower.elders.jobtracker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Interface to the jobTracker with the responsibility of dealing with the JobTracker api to get the job statuses.
 *
 * @author alexandre.normand
 */
public class JobTracker {
    public static final int LOOKBACK_IN_HOURS = 24;
    private Configuration configuration;

    public JobTracker(Configuration configuration) {
        this.configuration = configuration;
    }

    public Map<String, Job> getAllJobs(Pattern jobNameFilter) {
        List<Job> jobs = pollForJobs();

        Map<String, Job> filteredJobs = newHashMap();
        for (Job job : jobs) {
            if (jobNameFilter.matcher(job.getName()).matches()) {
                filteredJobs.put(job.getName(), job);
            }
        }
        return filteredJobs;
    }

    private List<Job> pollForJobs() {
        List<Job> allJobs = new ArrayList<Job>();

        try {
            JobClient client = new JobClient(new JobConf(this.configuration));
            JobStatus[] jobStatuses = client.getAllJobs();

            if (jobStatuses != null) {
                Calendar lowerBound = Calendar.getInstance();
                lowerBound.add(Calendar.HOUR, -1 * LOOKBACK_IN_HOURS);

                for (JobStatus jobStatus : jobStatuses) {
                    if (jobStatus.getStartTime() >= lowerBound.getTimeInMillis()) {
                        Job job = Job.fromRunningJobAndJobStatus(jobStatus, client.getJob(jobStatus.getJobID()));
                        allJobs.add(job);
                    }
                }
            }

            return allJobs;
        }
        catch (IOException e) {
            throw new RuntimeException("Error fetching jobs from job tracker.", e);
        }
    }
}
