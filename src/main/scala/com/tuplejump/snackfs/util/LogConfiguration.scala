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
package com.tuplejump.snackfs.util

import com.twitter.logging.{FileHandler, Level, LoggerFactory}
import java.io.File
import scala.util.{Failure, Success, Try}

object LogConfiguration {

  lazy val level: Level = System.getenv("SNACKFS_LOG_LEVEL") match {
    case "DEBUG" => Level.DEBUG
    case "INFO" => Level.INFO
    case "ERROR" => Level.ERROR
    case "ALL" => Level.ALL
    case "OFF" => Level.OFF
    case _ => Level.ERROR
  }

  lazy val logFile: String = {
    var possibleLocs = List("/var/log/snackfs", "/tmp/log/snackfs", "snackfs.log")
    var location: String = null

    while (possibleLocs.size > 0 && location == null) {
      val testLoc = possibleLocs.head
      possibleLocs = possibleLocs.tail
      if (isWritable(testLoc)) {
        location = testLoc
      }
    }
    println(s"Snackfs will write its log to ${location}. The log level is ${level}.")

    location
  }

  val config = new LoggerFactory("", Some(level), List(FileHandler(logFile)), true)

  private def isWritable(loc: String): Boolean = {
    val file = new File(loc)

    val fileCreated = Try {
      file.getParentFile.mkdirs()
      file.createNewFile()
      file
    }

    fileCreated match {
      case Success(f) =>
        f.canWrite
      case Failure(ex) =>
        false
    }
  }
}
