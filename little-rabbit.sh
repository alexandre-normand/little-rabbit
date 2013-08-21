#!/bin/sh
java -Xmx3G -cp ./target/little-rabbit-1.0-jar-with-dependencies.jar:`hbase classpath` com.opower.elders.JobTrackerDataCollector -interval 60 -output ./little-rabbits.json
