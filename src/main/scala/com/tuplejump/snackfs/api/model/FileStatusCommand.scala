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

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}
import scala.concurrent.Await
import com.tuplejump.snackfs.fs.model.INode
import java.io.FileNotFoundException
import com.twitter.logging.Logger
import org.apache.hadoop.fs.{FileStatus, Path}
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.api.partial.Command

object FileStatusCommand extends Command {
  private lazy val log = Logger.get(getClass)

  def apply(store: FileSystemStore,
            filePath: Path,
            atMost: FiniteDuration): FileStatus = {

    log.debug("getting status for %s", filePath)
    val maybeFile = Try(Await.result(store.retrieveINode(filePath), atMost))

    maybeFile match {
      case Success(file: INode) => SnackFileStatus(file, filePath)
      case Failure(e) =>
        val ex = new FileNotFoundException("No such file exists")
        log.error(ex, "Failed to get status for %s as it doesn't exist", filePath)
        throw ex
    }
  }
}
