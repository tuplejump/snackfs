/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.tuplejump.snackfs.model

import org.apache.hadoop.conf.Configuration
import org.apache.cassandra.locator.SimpleStrategy
import org.apache.cassandra.thrift.ConsistencyLevel
import scala.concurrent.duration._

case class SnackFSConfiguration(CassandraHost: String, CassandraPort: Int,
                                readConsistencyLevel: ConsistencyLevel, writeConsistencyLevel: ConsistencyLevel,
                                keySpace: String, blockSize: Long, subBlockSize: Long, atMost: FiniteDuration,
                                replicationFactor: Int, replicationStrategy: String) {
}

object SnackFSConfiguration {
  private val CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM
  private val REPLICATION_STRATEGY = classOf[SimpleStrategy].getCanonicalName
  private val KEYSPACE = "snackfs"
  private val HOST = "127.0.0.1"
  private val PORT: Int = 9160
  private val AT_MOST: Long = 10 * 1000
  private val SUB_BLOCK_SIZE: Long =  8 * 1024 * 1024 //8 MB
  private val BLOCK_SIZE: Long = 128 * 1024 * 1024 //128MB
  private val REPLICATION_FACTOR: Int = 3

  def get(userConf: Configuration): SnackFSConfiguration = {
    val cassandraHost = userConf.get("snackfs.cassandra.host")
    val host = optIfNull(cassandraHost, HOST)

    val port = userConf.getInt("snackfs.cassandra.port", PORT)

    val consistencyLevelWrite = userConf.get("snackfs.consistencyLevel.write")
    val writeLevel = getConsistencyLevel(consistencyLevelWrite)

    val consistencyLevelRead = userConf.get("snackfs.consistencyLevel.read")
    val readLevel = getConsistencyLevel(consistencyLevelRead)

    val keyspaceName: String = userConf.get("snackfs.keyspace")
    val keyspace = optIfNull(keyspaceName, KEYSPACE)

    val replicationFactor = userConf.getInt("snackfs.replicationFactor", REPLICATION_FACTOR)

    val strategy: String = userConf.get("snackfs.replicationStrategy")
    val replicationStrategy = optIfNull(strategy, REPLICATION_STRATEGY)

    val subBlockSize = userConf.getLong("snackfs.subblock.size", SUB_BLOCK_SIZE)
    val blockSize = userConf.getLong("snackfs.block.size", BLOCK_SIZE)

    val maxWaitDuration = userConf.getLong("snackfs.waitInterval", AT_MOST)
    val waitDuration = FiniteDuration(maxWaitDuration, MILLISECONDS)

    SnackFSConfiguration(host, port, readLevel, writeLevel, keyspace, blockSize,
      subBlockSize, waitDuration, replicationFactor, replicationStrategy)
  }

  private def getConsistencyLevel(level: String): ConsistencyLevel = {
    if (level != null) {
      ConsistencyLevel.valueOf(level)
    } else {
      CONSISTENCY_LEVEL
    }
  }

  private def optIfNull(valueToCheck: String, alternativeOption: String): String = {
    if (valueToCheck == null) {
      alternativeOption
    } else {
      valueToCheck
    }
  }

}
