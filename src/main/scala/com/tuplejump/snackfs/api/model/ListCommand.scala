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
import scala.util.{Failure, Success}
import com.tuplejump.snackfs.fs.model.INode
import java.io.FileNotFoundException
import org.apache.hadoop.fs.{FileStatus, Path}
import com.twitter.logging.Logger
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.api.partial.Command
import com.tuplejump.snackfs.util.TryHelper
import com.tuplejump.snackfs.SnackFS

object ListCommand extends Command {
  private lazy val log = Logger.get(getClass)

  def apply(store: FileSystemStore,
            path: Path,
            atMost: FiniteDuration, fs: SnackFS): Array[FileStatus] = {

    var result: Array[FileStatus] = Array()
    val absolutePath = path
    val mayBeFile = store.retrieveINode(absolutePath)

    mayBeFile match {
      case Success(file: INode) =>
        if (file.isFile) {
          log.debug("fetching file status for %s")
          val fileStatus = SnackFileStatus(file, absolutePath, fs)
          result = Array(fileStatus)

        } else {
          log.debug("fetching status for %s")
          val subPaths = TryHelper.handleFailure[(Path, Boolean), Set[Path]]((store.fetchSubPaths _).tupled, (absolutePath, false)).get
          result = subPaths.map(p => FileStatusCommand(store, p, atMost, fs)).toArray
        }

      case Failure(e) =>
        val ex = new FileNotFoundException(s"File not found: $path")
        log.error(ex, "Failed to list status of %s as it doesn't exist", path)
        throw ex
    }
    result
  }
}
