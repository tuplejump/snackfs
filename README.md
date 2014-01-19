#SnackFS

SnackFS is our bite-sized, lightweight HDFS compatible FileSystem built over Cassandra.
With it's unique fat driver design it requires no additional SysOps or setup on the Cassanndra Cluster. All you have to do is point to your Cassandra cluster and you are ready to go.

As SnackFS was written as a dropin replacement for HDFS, your existing HDFS backed applications not only run as-is on SnackFS, but they also run faster!
SnackFS cluster is also more resilient than a HDFS cluster as there is no SPOF like the NameNode.

##Prerequisites

1. SBT : It can be set up from the instructions [here](http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html#installing-sbt).

2. Cassandra(v1.2.12) : Instructions can be found [here](http://wiki.apache.org/cassandra/GettingStarted). An easier alternative would be using [CCM](https://github.com/pcmanus/ccm)

##Using SnackFS

###Use the binary

* You can download the SnackFS distribution from here -
####TODO

* To add SnackFS to your SBT project use,
####TODO

* To add SnackFS to your Maven project use,
####TODO

###Build from Source

1. Checkout the source from http://github.com/tuplejump/snackfs or the_grand_central branch in http://githube.com/tuplejump/calliope

2. To build SnackFS distribution run sbt's dist command i nthe project directory
```
[snackfs]$ sbt dist
```

   This will result in a "snackfs-{version}.zip" file in the "target" directory of "snackfs".
   Extract "snackfs-{version}.zip" at desired location and grant user permissions
   to read, write and execute the script "snackfs" located in bin directory

3. Start Cassandra (default setup for snackfs assumes its a cluster with 3 nodes)

4. It is possible to configure the file system by updating core-site.xml.
   The following properties can be added.
   * snackfs.cassandra.host (default 127.0.0.1)
   * snackfs.cassandra.port (default 9160)
   * snackfs.consistencyLevel.write (default QUORUM)
   * snackfs.consistencyLevel.read (default QUORUM)
   * snackfs.keyspace (default snackfs)
   * snackfs.subblock.size (default 8 MB (8 * 1024 * 1024))
   * snackfs.block.size (default 128 MB (128 * 1024 * 1024))
   * snackfs.replicationFactor (default 3)
   * snackfs.replicationStrategy (default org.apache.cassandra.locator.SimpleStrategy)

5. SnackFS Shell provides the fs commands similar to Hadoop Shell. For example to create a directory,

```
[Snackfs(extracted)]$bin/snackfs -mkdir snackfs:///random
```

###To build and use with Hadoop

1. Setup Apache Hadoop v1.0.4.(http://hadoop.apache.org/#Getting+Started).
   The base directory will be referred as 'hadoop-1.0.4' in the following steps.

2. Execute the following commands in the snackfs project directory.

```
[snackfs]$ sbt package
```

   This will result in a "snackfs_2.9.3-0.1-SNAPSHOT.jar" file in the "target/scala-2.9.3" directory of "snackfs".
   Copy the jar to 'hadoop-1.0.4/lib'.

3. Copy all the jars in snackfs/lib_managed and scala-library-2.9.3.jar
   (located at '~/.ivy2/cache/org.scala-lang/scala-library/jars') to 'hadoop-1.0.4/lib'.

4. Copy snackfs/src/main/resources/core-site.xml to 'hadoop-1.0.4/conf'

5. Start Cassandra (default setup for snackfs assumes its a cluster with 3 nodes)

6. Hadoop fs commands can now be run using snackfs. For example,

```
[hadoop-1.0.4]$ bin/hadoop fs -mkdir snackfs:///random
```

###To configure logging,
If you want your logs in a File, update LogConfiguration.config like below

```scala

val config = new LoggerFactory("", Option(Level.ALL), List(FileHandler("logs")), true)

```

The arguments for LoggerFactory are

1. node - Name of the logging node. ("") is the top-level logger.
2. level - Log level for this node. Leaving it None implies the parent logger's level.
3. handlers - Where to send log messages.
4. useParents - indicates if log messages are passed up to parent nodes.To stop at this node level, set it to false

Additional logging configuration details can be found [here](https://github.com/twitter/util/tree/master/util-logging#configuring)

