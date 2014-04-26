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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import scala.util.{Failure, Success}
import com.tuplejump.snackfs.fs.model.{FileType, INode}
import com.twitter.logging.Logger
import scala.concurrent.duration.FiniteDuration
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.api.partial.Command
import com.tuplejump.snackfs.util.TryHelper
import com.tuplejump.snackfs.cassandra.model.GenericOpSuccess

object MakeDirectoryCommand extends Command {
  private lazy val log = Logger.get(getClass)

  private def mkdir(store: FileSystemStore,
                    filePath: Path,
                    filePermission: FsPermission,
                    atMost: FiniteDuration): Boolean = {

    val mayBeFile = store.retrieveINode(filePath)
    var result = true

    mayBeFile match {
      case Success(file: INode) =>
        if (file.isFile) {
          log.debug("Failed to make a directory for path %s since its a file", filePath)
          result = false
        }

      case Failure(e: Throwable) =>
        val user = System.getProperty("user.name")
        val timestamp = System.currentTimeMillis()
        val iNode = INode(user, user, filePermission, FileType.DIRECTORY, null, timestamp)
        log.debug("Creating directory for path %s", filePath)
        TryHelper.handleFailure[(Path, INode), GenericOpSuccess]((store.storeINode _).tupled, (filePath, iNode))
    }
    result
  }

  def apply(store: FileSystemStore,
            filePath: Path,
            filePermission: FsPermission,
            atMost: FiniteDuration) = {

    var absolutePath = filePath
    var paths = List[Path]()
    var result = true

    while (absolutePath != null) {
      paths = paths :+ absolutePath
      absolutePath = absolutePath.getParent
    }

    log.debug("Creating directories for path %s", filePath)
    result = paths.map(path => mkdir(store, path, filePermission, atMost)).reduce(_ && _)
    result
  }

}
