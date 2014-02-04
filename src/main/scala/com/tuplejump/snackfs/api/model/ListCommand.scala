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
 */
package com.tuplejump.snackfs.api.model

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}
import scala.concurrent.Await
import com.tuplejump.snackfs.fs.model.INode
import java.io.FileNotFoundException
import org.apache.hadoop.fs.{FileStatus, Path}
import com.twitter.logging.Logger
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore

object ListCommand {
  private lazy val log = Logger.get(getClass)

  def apply(store: FileSystemStore,
            path: Path,
            atMost: FiniteDuration): Array[FileStatus] = {

    var result: Array[FileStatus] = Array()
    val absolutePath = path
    val mayBeFile = Try(Await.result(store.retrieveINode(absolutePath), atMost))

    mayBeFile match {
      case Success(file: INode) =>
        if (file.isFile) {
          log.debug("fetching file status for %s")
          val fileStatus = SnackFileStatus(file, absolutePath)
          result = Array(fileStatus)

        } else {
          log.debug("fetching status for %s")
          val subPaths = Await.result(store.fetchSubPaths(absolutePath, isDeepFetch = false), atMost)
          result = subPaths.map(p => FileStatusCommand(store, p, atMost)).toArray
        }

      case Failure(e) =>
        val ex = new FileNotFoundException("No such file exists")
        log.error(ex, "Failed to list status of %s as it doesn't exist", path)
        throw ex
    }
    result
  }
}
