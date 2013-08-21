package com.opower.elders;

import akka.actor.ActorSystem;
import akka.util.Duration;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;
import com.google.common.base.Throwables;
import com.opower.elders.jobtracker.Job;
import com.opower.elders.jobtracker.JobTracker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.codehaus.jackson.annotate.JsonAnyGetter;
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.String.format;

/**
 * Command-line tool that polls the job tracker periodically to get data from the jobs that are running.
 * This is meant to be consumed by the d3 animation render tool.
 *
 * @author alexandre.normand
 */
public final class JobTrackerDataCollector {
    private static final Log LOG = LogFactory.getLog(com.opower.elders.JobTrackerDataCollector.class);

    @Parameter(names = { "-interval"}, description = "Poll interval in seconds (default: 30)")
    Integer interval = 30;

    @Parameter(names = "-output", description = "Output file", required = true, converter = FileConverter.class)
    File outputFile;

    @Parameter(names = "-jobNameFilter", description = "Regex to filter jobs to report on (only matching jobs are tracked)")
    String jobNameFilter = ".*";

    public static void main(String []argv) {
        com.opower.elders.JobTrackerDataCollector ronin = new com.opower.elders.JobTrackerDataCollector();
        JCommander jCommander = new JCommander(ronin);
        try {
            jCommander.parse(argv);
        }
        catch (ParameterException e) {
            jCommander.usage();
            System.exit(1);
        }

        try {
            JobTrackerDataPoller jobTrackerDataPoller = new JobTrackerDataPoller(ronin.interval, ronin.jobNameFilter,
                ronin.outputFile);
            jobTrackerDataPoller.run();
        }
        catch (Throwable e) {
            LOG.fatal("Unexpected error, aborting.", e);
            System.exit(1);
        }


    }

    /**
     * This polls the job tracker and collects data in-memory. A shutdown hook takes care of
     * writing the data out to the {@link com.opower.elders.JobTrackerDataCollector#outputFile} on exit. It is therefore important
     * that the process is stopped gracefully: either with ctrl-c if running in foreground
     * or kill -2 <pid> if running in background.
     *
     * @author alexandre.normand
     */
    public static class JobTrackerDataPoller implements Runnable {

        public static final ActorSystem ACTOR_SYSTEM = ActorSystem.create();
        public static final String MAP_PROGRESS = "mapProgress";
        public static final String REDUCE_PROGRESS = "reduceProgress";
        public static final String INPUT_RECORDS = "inputRecord";
        public static final String OUTPUT_RECORDS = "outputRecords";
        private JobTracker jobTracker;
        private int pollInterval;
        private Pattern jobNameFilter;
        private ObjectMapper objectMapper = new ObjectMapper();
        private final ObjectWriter writer = this.objectMapper.writerWithDefaultPrettyPrinter();
        private final List<NamedData> data = newArrayList();
        private final Map<String, NamedData> lookupTable = newHashMap();

        public JobTrackerDataPoller(int pollInterval, String jobNameFilter, final File outputFile) {
            this.jobTracker = new JobTracker(HBaseConfiguration.create());
            this.pollInterval = pollInterval;
            this.jobNameFilter = Pattern.compile(jobNameFilter);

            // We output the json on shutdown. It's therefore better to do a graceful shutdown by issuing
            // a ctrl-c (if running in foreground) or a kill -2 <pid> if running in background
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        FileWriter fileWriter = new FileWriter(outputFile);
                        objectMapper.writeValue(fileWriter, data);

                    }
                    catch (IOException e) {
                        Throwables.propagate(e);
                    }
                }
            });
        }

        private NamedData initializeJobMetrics(String name) {
            HashMap<String, List<Number[]>> metrics = newHashMap();
            ArrayList<Number[]> mapProgress = newArrayList();
            metrics.put(MAP_PROGRESS, mapProgress);

            ArrayList<Number[]> reduceProgress = newArrayList();
            metrics.put(REDUCE_PROGRESS, reduceProgress);

            ArrayList<Number[]> inputRecords = newArrayList();
            metrics.put(INPUT_RECORDS, inputRecords);

            ArrayList<Number[]> outputRecords = newArrayList();
            metrics.put(OUTPUT_RECORDS, outputRecords);

            NamedData namedData = new NamedData(name, metrics);
            return namedData;
        }

        public void run() {
            Map<String, Job> jobs = this.jobTracker.getAllJobs(this.jobNameFilter);
            try {
                Map<Long, Map<String, Job>> pollResult = newHashMap();
                long currentTime = new Date().getTime();
                pollResult.put(currentTime, jobs);
                String content = writer.writeValueAsString(pollResult);
                System.out.println(content);

                // append data
                appendData(currentTime, jobs);
            }
            catch (IOException e) {
                LOG.error("Failed to serialize output", e);
            }
            LOG.debug("Jobtracker refreshed.");
            LOG.debug(format("Scheduling next refresh in %d seconds...", this.pollInterval));

            ACTOR_SYSTEM.scheduler().scheduleOnce(
                    Duration.create(this.pollInterval, TimeUnit.SECONDS),
                    this);
        }

        private void appendData(Long timestamp, Map<String, Job> jobs) {
            for (String job : jobs.keySet()) {
                Job lastResults = jobs.get(job);
                NamedData jobData = lookupTable.get(job);

                if (jobData == null) {
                    jobData = initializeJobMetrics(job);
                    data.add(jobData);
                    lookupTable.put(job, jobData);
                }

                appendMetricData(timestamp, lastResults, jobData.getData());
            }
        }

        private void appendMetricData(Long timestamp, Job lastResults, Map<String, List<Number[]>> jobData) {
            Long inputRecords = 0L;
            Long outputRecords = 0L;
            float mapProgress = 0;
            float reduceProgress = 0;
            if (lastResults != null) {
                inputRecords = lastResults.getInputRecords();
                outputRecords = lastResults.getOutputRecords();
                mapProgress = lastResults.getMapProgress();
                reduceProgress = lastResults.getReduceProgress();
            }

            addTimestampedValue(jobData.get(INPUT_RECORDS), timestamp, inputRecords);
            addTimestampedValue(jobData.get(OUTPUT_RECORDS), timestamp, outputRecords);
            addTimestampedValue(jobData.get(MAP_PROGRESS), timestamp, mapProgress);
            addTimestampedValue(jobData.get(REDUCE_PROGRESS), timestamp, reduceProgress);
        }

        private void addTimestampedValue(List<Number[]> metricValues, Long timestamp, Number value) {
            Number []values = new Number[2];
            values[0] = timestamp;
            values[1] = zeroIfNull(value);
            metricValues.add(values);
        }

        public static Number zeroIfNull(Number value) {
            return value == null ? 0 : value;
        }
    }

    /**
     * Named object to match expected json model.
     *
     * @author alexandre.normand
     */
    public static class NamedData {
        private String name;
        Map<String, List<Number[]>> data;

        public NamedData(String name, Map<String, List<Number[]>> data) {
            this.name = name;
            this.data = data;
        }

        public String getName() {
            return name;
        }

        Map<String, List<Number[]>> getData() {
            return data;
        }

        @JsonAnyGetter
        public Map<String, List<Number[]>> any() {
            return data;
        }

        @JsonAnySetter
        public void set(String name, List<Number[]> value) {
            this.data.put(name, value);
        }
    }
}
