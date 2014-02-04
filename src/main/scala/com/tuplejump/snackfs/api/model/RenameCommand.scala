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

object RenameCommand {
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

  def apply(store: FileSystemStore, src: Path, dst: Path, atMost: FiniteDuration): Boolean = {
    if (src != dst) {
      val mayBeSrcINode = Try(Await.result(store.retrieveINode(src), atMost))
      mayBeSrcINode match {
        case Failure(e1) => {
          val ex = new IOException("No such file or directory.%s".format(src))
          log.error(ex, "Failed to rename %s as it doesnt exist", src)
          throw ex
        }
        case Success(iNode: INode) =>
          val mayBeDstINode = Try(Await.result(store.retrieveINode(dst), atMost))
          mayBeDstINode match {
            case Failure(e) => {
              log.debug("%s does not exist. checking if %s exists", dst, dst.getParent)
              val maybeDstParent = Try(Await.result(store.retrieveINode(dst.getParent), atMost))
              maybeDstParent match {
                case Failure(e2) => {
                  val ex = new IOException("Destination %s directory does not exist.".format(dst.getParent))
                  log.error(ex, "Failed to rename %s as destination %s doesn't exist", src, dst.getParent)
                  throw ex
                }
                case Success(dstParentINode: INode) => {
                  if (dstParentINode.isFile) {
                    val ex = new IOException("A file exists with parent of destination.")
                    log.error(ex, "Failed to rename directory %s as given destination's parent %s is a file", src, dst.getParent)
                    throw ex
                  }
                  if (iNode.isDirectory) {
                    log.debug("renaming directory %s to %s", src, dst)
                    renameDir(store, src, dst, atMost)
                  }
                  renameINode(store, src, dst, iNode, atMost)
                }
              }
            }
            case Success(dstINode: INode) =>
              if (dstINode.isFile) {
                val ex = new IOException("A file %s already exists".format(dst))
                log.error(ex, "Failed to rename %s as given destination %s is a file", src, dst)
                throw ex
              }
              else {
                var dstPathString = dst.toUri.getPath
                if (!dstPathString.endsWith("/")) {
                  dstPathString = dstPathString + "/"
                }
                val fileName = src.getName
                val updatedPath = new Path(dstPathString + fileName)

                val mayBeExistingINode = Try(Await.result(store.retrieveINode(updatedPath), atMost))

                mayBeExistingINode match {
                  case Failure(e) =>
                    if (iNode.isFile) {
                      log.debug("renaming file %s to %s", src, dst)
                      renameINode(store, src, updatedPath, iNode, atMost)
                    } else {
                      log.debug("renaming directory %s to %s", src, dst)
                      renameDir(store, src, updatedPath, atMost)
                    }
                  case Success(existingINode: INode) =>
                    if (existingINode.isFile) {
                      if (iNode.isFile) {
                        renameINode(store, src, updatedPath, iNode, atMost)
                      } else {
                        val ex = new IOException("cannot overwrite non-directory with a directory")
                        log.error(ex, "Failed to rename directory %s as given destination %s is a file", src, dst)
                        throw ex
                      }
                    } else {
                      if (iNode.isFile) {
                        val ex = new IOException("cannot overwrite directory with a non-directory")
                        log.error(ex, "Failed to rename file %s as given destination %s is a directory", src, dst)
                        throw ex
                      }
                      else {
                        val contents = Await.result(store.fetchSubPaths(updatedPath, isDeepFetch = false), atMost)
                        if (contents.size > 0) {
                          val ex = new IOException("cannot move %s to %s - directory not empty".format(src, dst))
                          log.error(ex, "Failed to rename %s as given destination %s is not empty", src, dst)
                          throw ex
                        }
                        else {
                          log.debug("renaming %s to %s", src, updatedPath)
                          renameDir(store, src, updatedPath, atMost)
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
