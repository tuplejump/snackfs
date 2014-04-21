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
package com.tuplejump.snackfs.api.model

import scala.util.{Failure, Success, Try}
import scala.concurrent.Await
import java.io.IOException
import org.apache.hadoop.fs.{Path, FSDataInputStream}
import com.tuplejump.snackfs.fs.stream.FileSystemInputStream
import scala.concurrent.duration.FiniteDuration
import com.twitter.logging.Logger
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.fs.model.INode
import com.tuplejump.snackfs.api.partial.Command

object OpenFileCommand extends Command {
  private lazy val log = Logger.get(getClass)

  //TODO check how bufferSize is applied
  def apply(store: FileSystemStore,
            filePath: Path,
            bufferSize: Int,
            atMost: FiniteDuration): FSDataInputStream = {

    val mayBeFile = store.retrieveINode(filePath)

    mayBeFile match {
      case Success(file: INode) =>
        if (file.isDirectory) {
          val ex = new IOException("Path %s is a directory.".format(filePath))
          log.error(ex, "Failed to open file %s as a directory exists at that path", filePath)
          throw ex

        } else {
          log.debug("opening file %s", filePath)
          val fileStream = new FSDataInputStream(FileSystemInputStream(store, filePath))
          fileStream
        }

      case Failure(e: Exception) =>
        val ex = new IOException("No such file.")
        log.error(ex, "Failed to open file %s as it doesnt exist", filePath)
        throw ex
    }
  }
}
