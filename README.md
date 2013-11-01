###Prerequisites

1. SBT : It can be set up from the instructions [here](http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html#installing-sbt).

2. Cassandra(v1.2.9) : Instructions can be found [here](http://wiki.apache.org/cassandra/GettingStarted).
An easier alternative would be using [this project](https://github.com/pcmanus/ccm)

###Setup
#####To run the project,

1. Execute the following command in the project directory

    [snackfs]$ sbt dist

   This will result in a "snackfs.tar.gz" file in the "target" directory of "snackfs".
   Extract "snackfs.tar.gz" at desired location and grant user permissions
   to read, write and execute the script "snackfs" located in bin directory

2. start Cassandra v1.2.9 (default setup for snackfs assumes its a cluster with 3 nodes)
3. It is possible to configure the file system by updating core-site.xml.
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
4. Hadoop like fs commands can now be run from the extracted Snackfs directory. For example,

    [Snackfs(extracted)]$bin/snackfs fs -mkdir snackfs:///random


#####To build the project,

1. Setup Apache Hadoop v1.0.4.(http://hadoop.apache.org/#Getting+Started).
   The base directory will be referred as 'hadoop-1.0.4' in the following steps.
2. Execute the following commands in the snackfs project directory.

    [snackfs]$ sbt package

   This will result in a "snackfs_2.9.3-0.1-SNAPSHOT.jar" file in the "target/scala-2.9.3" directory of "snackfs".
   Copy the jar to 'hadoop-1.0.4/lib'.
3. Copy all the jars in snackfs/lib_managed and scala-library-2.9.3.jar
   (located at '~/.ivy2/cache/org.scala-lang/scala-library/jars') to 'hadoop-1.0.4/lib'.
4. Copy snackfs/src/main/resources/core-site.xml to 'hadoop-1.0.4/conf'
5. start Cassandra v1.2.9 (default setup for snackfs assumes its a cluster with 3 nodes)
6. Hadoop fs commands can now be run using snackfs. For example,

    [hadoop-1.0.4]$ bin/hadoop fs -mkdir snackfs:///random


