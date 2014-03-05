[SnackFS @ Calliope](http://tuplejump.github.io/calliope/snackfs.html)


# SnackFS

SnackFS is our bite-sized, lightweight HDFS compatible FileSystem built over Cassandra.
With it's unique fat driver design it requires no additional SysOps or setup on the Cassanndra Cluster. All you have to do is point to your Cassandra cluster and you are ready to go.

As SnackFS was written as a dropin replacement for HDFS, your existing HDFS backed applications not only run as-is on SnackFS, but they also run faster!
SnackFS cluster is also more resilient than a HDFS cluster as there is no SPOF like the NameNode.

## Prerequisites

1. SBT : It can be set up from the instructions [here](http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html#installing-sbt).

2. Cassandra (v1.2.12 or v2.0.5) : Instructions can be found [here](http://wiki.apache.org/cassandra/GettingStarted). An easier alternative would be using [CCM](https://github.com/pcmanus/ccm)

## Using SnackFS

### Use the binary

* You can download the SnackFS distribution built with for the appropriate version of Scala and Cassandra
    - [Scala 2.9.x and Cassandra 1.2.x](http://bit.ly/1kZGAEL)
    - [Scala 2.10.x and Cassandra 1.2.x](http://bit.ly/1f6AvRH)
    - [Scala 2.9.x and Cassandra 2.0.x](http://bit.ly/1nbJiZ2)
    - [Scala 2.10.x and Cassandra 2.0.x](http://bit.ly/1f6BjGj)

* To add SnackFS to your SBT project use,

For SBT
```scala
"com.tuplejump" %% "snackfs" % "[SNACKFS_VERSION]"
```

* To add SnackFS to your Maven project use this snippet with the appropriate Scala and SnackFS version

```xml
<dependency>
  <groupId>com.tuplejump</groupId>
  <artifactId>snackfs_[scala_version]</artifactId>
  <version>[SNACKFS_VERSION]</version>
</dependency>
```

Where SnackFS version is **0.6.2-EA** for use with *Cassandra 1.2.x*
And **0.6.2-C2-EA** for use with *Cassandra 2.0.x*

The Scala version is **2.9.3** for *Scala 2.9.x* and **2.10** for use with *Scala 2.10.x*

### Build from Source

1. Checkout the source from http://github.com/tuplejump/snackfs

2. To build SnackFS distribution run sbt's dist command in the project directory
```
[snackfs]$ sbt dist
```

   This will result in a "snackfs-{version}.tgz" file in the "target" directory of "snackfs".
   Extract "snackfs-{version}.tgz" to the desired location.

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

#### Running Tests Locally
Prerequisites:
A three node Cassandra cluster running at 127.0.0.1:9160.
Cassandra version should be same as that in SnackFSBuild.scala.

To modify these for any test, you can specify the value in the specific test.
You can refer to ThriftStoreSpec for example. All the properties mentioned in previous section can be set.

Tests can be run from SBT console, using the `test` or `test-only` commands.
The integration tests:

* FSShellSpec - tests to check SnackFS compatibility with Hadoop-v1.0.4
* SnackFSShellSpec - tests to check SnackFS

can be run using the `it` command.

FSShellSpec requires `HADOOP_HOME` to be defined in the system environment.
Additionally, `projectHome` in FSShellSpec file should be set to point to the path where SnackFS
has been cloned.

Similarly, SnackFSShellSpec requires `SNACKFS_HOME` to be defined in the system environment.
This should point to the location where the tar file generated from `dist` command was extracted.

###To build and use with Hadoop

1. Setup Apache Hadoop v1.0.4.(http://hadoop.apache.org/#Getting+Started). The base directory will be referred as 'hadoop-1.0.4' in the following steps.

2. Execute the following commands in the snackfs project directory.
```
[snackfs]$ sbt package
```

   This will result in a "snackfs_&lt;scala_version&gt;-&lt;version&gt;.jar" file in the "target/scala-&lt;scala_version&gt;" directory of "snackfs".
   Copy the jar to 'hadoop-1.0.4/lib'.

3. Copy all the jars in snackfs/lib_managed and scala-library-&lt;scala_version&gt;.jar
   (located at '~/.ivy2/cache/org.scala-lang/scala-library/jars') to 'hadoop-1.0.4/lib'.

4. Copy snackfs/src/main/resources/core-site.xml to 'hadoop-1.0.4/conf'

5. Start Cassandra (default setup for snackfs assumes its a cluster with 3 nodes)

6. Hadoop fs commands can now be run using snackfs. For example,
```
[hadoop-1.0.4]$ bin/hadoop fs -mkdir snackfs:///random
```

###To configure logging,

#### In System Environment

Set SNACKFS_LOG_LEVEL in the Shell to one of the following Values

* DEBUG
* INFO
* ERROR
* ALL
* OFF

Default value if not set if ERROR

####In code (for further control/tuning)
If you want your logs in a File, update LogConfiguration.scala like below

```scala
val config = new LoggerFactory("", Option(Level.ALL), List(FileHandler("logs")), true)
```

The arguments for LoggerFactory are

1. node - Name of the logging node. ("") is the top-level logger.
2. level - Log level for this node. Leaving it None implies the parent logger's level.
3. handlers - Where to send log messages.
4. useParents - indicates if log messages are passed up to parent nodes.To stop at this node level, set it to false

Additional logging configuration details can be found [here](https://github.com/twitter/util/tree/master/util-logging#configuring)

