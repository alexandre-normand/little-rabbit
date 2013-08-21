little-rabbit
=============

little-rabbit is a data collector of hadoop job data.
This command-line tool does one thing: it collects data on running hadoop jobs
from the jobtracker API and, on shutdown, writes it out to a json file. 
The usual story is that this data is then fed to the 
[bloom-harvester](https://github.com/opower/bloom-harvester)
for maximum enjoyment.

To see a demo of how the data from this tool can be used, see
[The Story of the Big Data Elders](http://opower.github.io/2013/07/07/the-story-of-the-big-data-elders/)
and [The Big Data Elders, Archeology Hour](http://opower.github.io/link-tbd).

See also
------------

* [giant-squash](https://github.com/opower/giant-squash): The companion tool of little-rabbit. This
one collects hbase table sizes from ``hdfs``.
* [bloom-harvester](https://github.com/opower/bloom-harvester): This is the tool that will eat your
squash and rabbits and produce a spectacle for your eyes.

Quick start
-----------

* ``mvn clean install``
* ``java -Xmx3G -cp ./target/little-rabbit-1.0-jar-with-dependencies.jar:`hbase classpath` com.opower.elders.JobTrackerDataCollector -output <little-rabbits.json> -interval <poll_interval_in_seconds> -jobNameFilter <regex on the job name to collect data for>``
* ``Ctrl-C when done`` or ``kill -2 <pid`` if you run it with ``nohup`` in the background.
