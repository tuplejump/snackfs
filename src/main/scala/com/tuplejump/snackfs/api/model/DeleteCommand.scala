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

import scala.util.{Failure, Success}
import com.tuplejump.snackfs.fs.model.INode
import java.io.IOException
import scala.concurrent.duration.FiniteDuration
import org.apache.hadoop.fs.Path
import com.twitter.logging.Logger
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.api.partial.Command
import com.tuplejump.snackfs.util.TryHelper
import com.tuplejump.snackfs.cassandra.model.GenericOpSuccess


object DeleteCommand extends Command {
  private lazy val log = Logger.get(getClass)

  def apply(store: FileSystemStore,
            srcPath: Path,
            isRecursive: Boolean,
            atMost: FiniteDuration): Boolean = {

    val absolutePath = srcPath
    val mayBeSrc = store.retrieveINode(absolutePath)
    var result = true

    mayBeSrc match {

      case Success(src: INode) =>
        if (src.isFile) {
          log.debug("deleting file %s", srcPath)
          TryHelper.handleFailure[(Path), GenericOpSuccess](store.deleteINode, absolutePath)
          TryHelper.handleFailure[(INode), GenericOpSuccess](store.deleteBlocks, src)

        } else {
          val contents = ListCommand(store, srcPath, atMost)

          if (contents.length == 0) {
            log.debug("deleting directory %s", srcPath)
            TryHelper.handleFailure[(Path), GenericOpSuccess](store.deleteINode, absolutePath)

          } else if (!isRecursive) {
            val ex = new IOException("Directory is not empty")
            log.error(ex, "Failed to delete directory %s as it is not empty", srcPath)
            throw ex

          } else {
            log.debug("deleting directory %s and all its contents", srcPath)
            result = contents.map(p => DeleteCommand(store, p.getPath, isRecursive, atMost)).reduce(_ && _)
            TryHelper.handleFailure[(Path), GenericOpSuccess](store.deleteINode, absolutePath)
          }
        }

      case Failure(e) =>
        log.debug("failed to delete %s, as it doesn't exist", srcPath)
        result = false
    }
    result
  }

}

