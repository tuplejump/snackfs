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
import com.tuplejump.snackfs.fs.model.INode
import scala.concurrent.Await
import scala.util.{Success, Failure, Try}
import java.io.IOException
import com.twitter.logging.Logger
import scala.concurrent.duration.FiniteDuration
import org.apache.hadoop.fs.permission.FsPermission
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.api.partial.Command

object RenameCommand extends Command {
  private lazy val log = Logger.get(getClass)

  private def renameINode(store: FileSystemStore, originalPath: Path, updatedPath: Path, iNode: INode, atMost: FiniteDuration) = {
    log.debug("deleting existing iNode %s", originalPath)
    Await.ready(store.deleteINode(originalPath), atMost)
    log.debug("storing iNode %s", updatedPath)
    Await.ready(store.storeINode(updatedPath, iNode), atMost)
  }

  private def renameDir(store: FileSystemStore, src: Path, dst: Path, atMost: FiniteDuration) = {
    MakeDirectoryCommand(store, dst, FsPermission.getDefault, atMost)
    val contents = Await.result(store.fetchSubPaths(src, isDeepFetch = true), atMost)
    if (contents.size > 0) {
      log.debug("renaming all child nodes %s", contents)
      val srcPathString = src.toUri.getPath
      val dstPathString = dst.toUri.getPath
      contents.map(path => {
        val actualINode = Await.result(store.retrieveINode(path), atMost)
        val oldPathString = path.toUri.getPath
        val changedPathString = oldPathString.replaceFirst(srcPathString, dstPathString)
        val changedPath = new Path(changedPathString)
        log.debug("renaming child node %s to %s", path, changedPath)
        MakeDirectoryCommand(store, changedPath.getParent, FsPermission.getDefault, atMost)
        renameINode(store, path, changedPath, actualINode, atMost)
      })
    }
  }

  //TODO refactor this
  def apply(store: FileSystemStore, srcPath: Path, dstPath: Path, atMost: FiniteDuration): Boolean = {
    if (srcPath != dstPath) {
      val mayBeSrc = Try(Await.result(store.retrieveINode(srcPath), atMost))
      mayBeSrc match {
        case Failure(e1) =>
          val ex = new IOException("No such file or directory.%s".format(srcPath))
          log.error(ex, "Failed to rename %s as it doesnt exist", srcPath)
          throw ex
        case Success(src: INode) =>
          val mayBeDst = Try(Await.result(store.retrieveINode(dstPath), atMost))
          mayBeDst match {
            case Failure(e) =>
              log.debug("%s does not exist. checking if %s exists", dstPath, dstPath.getParent)
              val maybeDstParent = Try(Await.result(store.retrieveINode(dstPath.getParent), atMost))
              maybeDstParent match {
                case Failure(e2) =>
                  val ex = new IOException("Destination %s directory does not exist.".format(dstPath.getParent))
                  log.error(ex, "Failed to rename %s as destination %s doesn't exist", srcPath, dstPath.getParent)
                  throw ex
                case Success(dstParent: INode) =>
                  if (dstParent.isFile) {
                    val ex = new IOException("A file exists with parent of destination.")
                    log.error(ex, "Failed to rename directory %s as given destination's parent %s is a file", srcPath, dstPath.getParent)
                    throw ex
                  }
                  if (src.isDirectory) {
                    log.debug("renaming directory %s to %s", srcPath, dstPath)
                    renameDir(store, srcPath, dstPath, atMost)
                  }
                  renameINode(store, srcPath, dstPath, src, atMost)
              }
            case Success(dst: INode) =>
              if (dst.isFile) {
                val ex = new IOException("A file %s already exists".format(dstPath))
                log.error(ex, "Failed to rename %s as given destination %s is a file", srcPath, dstPath)
                throw ex
              }
              else {
                var dstPathString = dstPath.toUri.getPath
                if (!dstPathString.endsWith("/")) {
                  dstPathString = dstPathString + "/"
                }
                val fileName = srcPath.getName
                val updatedPath = new Path(dstPathString + fileName)

                val mayBeExistingFile = Try(Await.result(store.retrieveINode(updatedPath), atMost))

                mayBeExistingFile match {
                  case Failure(e) =>
                    if (src.isFile) {
                      log.debug("renaming file %s to %s", srcPath, dstPath)
                      renameINode(store, srcPath, updatedPath, src, atMost)
                    } else {
                      log.debug("renaming directory %s to %s", srcPath, dstPath)
                      renameDir(store, srcPath, updatedPath, atMost)
                    }
                  case Success(existingFile: INode) =>
                    if (existingFile.isFile) {
                      if (src.isFile) {
                        renameINode(store, srcPath, updatedPath, src, atMost)
                      } else {
                        val ex = new IOException("cannot overwrite non-directory with a directory")
                        log.error(ex, "Failed to rename directory %s as given destination %s is a file", srcPath, dstPath)
                        throw ex
                      }
                    } else {
                      if (src.isFile) {
                        val ex = new IOException("cannot overwrite directory with a non-directory")
                        log.error(ex, "Failed to rename file %s as given destination %s is a directory", srcPath, dstPath)
                        throw ex
                      }
                      else {
                        val contents = Await.result(store.fetchSubPaths(updatedPath, isDeepFetch = false), atMost)
                        if (contents.size > 0) {
                          val ex = new IOException("cannot move %s to %s - directory not empty".format(srcPath, dstPath))
                          log.error(ex, "Failed to rename %s as given destination %s is not empty", srcPath, dstPath)
                          throw ex
                        }
                        else {
                          log.debug("renaming %s to %s", srcPath, updatedPath)
                          renameDir(store, srcPath, updatedPath, atMost)
                        }
                      }
                    }
                }
              }
          }
      }
    }
    true
  }
}
