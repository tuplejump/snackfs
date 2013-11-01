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
package com.tuplejump.fs

import java.net.URI
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.conf.Configuration
import java.io.{FileNotFoundException, IOException}
import scala.concurrent.Await
import scala.concurrent.duration._

import scala.util.{Failure, Success, Try}
import com.tuplejump.model.{SnackFSConfiguration, FileType, INode}
import org.apache.hadoop.fs._
import com.twitter.logging.Logger

case class SnackFS() extends FileSystem {

  private val log = Logger.get(getClass)

  private var systemURI: URI = null
  private var currentDirectory: Path = null
  private var subBlockSize: Long = 0L

  private var atMost: FiniteDuration = null
  private var store: FileSystemStore = null
  private var customConfiguration: SnackFSConfiguration = _

  override def initialize(uri: URI, configuration: Configuration) = {
    log.debug("Initializing SnackFs")
    super.initialize(uri, configuration)
    setConf(configuration)

    systemURI = URI.create(uri.getScheme + "://" + uri.getAuthority)

    val directory = new Path("/user", System.getProperty("user.name"))
    currentDirectory = makeQualified(directory)

    log.debug("generating required configuration")
    customConfiguration = SnackFSConfiguration.get(configuration)

    store = new ThriftStore(customConfiguration)
    atMost = customConfiguration.atMost
    Await.ready(store.createKeyspace, atMost)
    store.init

    subBlockSize = customConfiguration.subBlockSize
  }

  private def makeAbsolute(path: Path): Path = {
    if (path.isAbsolute) path else new Path(currentDirectory, path)
  }

  def getUri: URI = systemURI

  def setWorkingDirectory(newDir: Path) = {
    currentDirectory = makeAbsolute(newDir)
  }

  def getWorkingDirectory: Path = currentDirectory

  def open(path: Path, bufferSize: Int): FSDataInputStream = {
    val mayBeiNode = Try(Await.result(store.retrieveINode(path), atMost))

    mayBeiNode match {
      case Success(inode) => {
        if (inode.isDirectory) {
          val ex = new IOException("Path %s is a directory.".format(path))
          log.error(ex, "Failed to open file %s as a directory exists at that path", path.toUri.toString)
          throw ex
        }
        else {
          log.debug("opening file %s", path.toUri.toString)
          val fileStream = new FSDataInputStream(FileSystemInputStream(store, path))
          fileStream
        }
      }
      case Failure(e) => {
        val ex = new IOException("No such file.")
        log.error(ex, "Failed to open file %s as it doesnt exist", path.toUri.toString)
        throw ex
      }
    }
  }

  private def mkdir(path: Path, permission: FsPermission): Boolean = {
    val mayBeiNode = Try(Await.result(store.retrieveINode(path), atMost))

    var result = true
    mayBeiNode match {
      case Success(inode) =>
        if (inode.isFile) {
          log.debug("Failed to make a directory for path %s since its a file", path.toUri.toString)
          result = false
        }
      case Failure(e) =>
        val user = System.getProperty("user.name")
        val timestamp = System.currentTimeMillis()
        val iNode = INode(user, user, permission, FileType.DIRECTORY, null, timestamp)
        log.debug("Creating directory for path %s", path.toUri.toString)
        Await.ready(store.storeINode(path, iNode), atMost)
    }
    result
  }

  def mkdirs(path: Path, permission: FsPermission): Boolean = {
    var absolutePath = makeAbsolute(path)
    var paths = List[Path]()
    var result = true
    while (absolutePath != null) {
      paths = paths :+ absolutePath
      absolutePath = absolutePath.getParent
    }
    result = paths.map(p => mkdir(p, permission)).reduce(_ && _)
    log.debug("Creating directories for path %s", path.toUri.toString)
    result
  }

  def create(filePath: Path, permission: FsPermission, overwrite: Boolean,
             bufferSize: Int, replication: Short, blockSize: Long,
             progress: Progressable): FSDataOutputStream = {

    val mayBeiNode = Try(Await.result(store.retrieveINode(filePath), atMost))
    mayBeiNode match {
      case Success(p) => {
        if (p.isFile && !overwrite) {
          val ex = new IOException("File exists and cannot be overwritten")
          log.error(ex, "Failed to create file %s as it exists and cannot be overwritten", filePath.toUri.toString)
          throw ex
        }
      }
      case Failure(e) =>
        val parentPath = filePath.getParent
        if (parentPath != null) {
          mkdirs(parentPath)
        }
    }
    log.debug("creating file %s", filePath.toUri.toString)
    val fileStream = new FileSystemOutputStream(store, filePath, blockSize, subBlockSize, bufferSize, atMost)
    val fileDataStream = new FSDataOutputStream(fileStream, statistics)
    fileDataStream
  }

  override def getDefaultBlockSize: Long = {
    customConfiguration.blockSize
  }

  def append(path: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    val ex = new IOException("Appending to existing file is not supported.")
    log.error(ex, "Failed to append to file %s as it is not supported", path.toUri.toString)
    throw ex
  }

  private def length(iNode: INode): Long = {
    var result = 0L
    if (iNode.isFile) {
      result = iNode.blocks.map(_.length).sum
    }
    result
  }

  private def blockSize(iNode: INode): Long = {
    var result = 0L
    if (iNode.blocks != null && iNode.blocks.length > 0) {
      result = iNode.blocks(0).length
    }
    result
  }

  private case class SnackFileStatus(iNode: INode, path: Path) extends FileStatus(length(iNode), iNode.isDirectory, 0,
    blockSize(iNode), iNode.timestamp, 0, iNode.permission, iNode.user, iNode.group, path: Path) {
  }

  def getFileStatus(path: Path): FileStatus = {
    log.debug("getting status for %s", path.toUri.toString)
    val maybeInode = Try(Await.result(store.retrieveINode(path), atMost))
    maybeInode match {
      case Success(iNode: INode) => SnackFileStatus(iNode, path)
      case Failure(e) => {
        val ex = new FileNotFoundException("No such file exists")
        log.error(ex, "Failed to get status for %s as it doesn't exist", path.toUri.toString)
        throw ex
      }
    }
  }

  def delete(path: Path, isRecursive: Boolean): Boolean = {
    val absolutePath = makeAbsolute(path)
    val mayBeiNode = Try(Await.result(store.retrieveINode(absolutePath), atMost))
    var result = true
    mayBeiNode match {
      case Success(iNode: INode) =>
        if (iNode.isFile) {
          log.debug("deleting file %s",path.toUri.toString)
          Await.ready(store.deleteINode(absolutePath), atMost)
          Await.ready(store.deleteBlocks(iNode), atMost)
        }
        else {
          val contents = listStatus(path)
          if (contents.length == 0)  {
            log.debug("deleting directory %s",path.toUri.toString)
            Await.ready(store.deleteINode(absolutePath), atMost)
          }
          else if (!isRecursive) {
            val ex = new IOException("Directory is not empty")
            log.error(ex, "Failed to delete directory %s as it is not empty", path.toUri.toString)
            throw ex
          }
          else {
            log.debug("deleting directory %s and all its contents",path.toUri.toString)
            result = contents.map(p => delete(p.getPath, isRecursive)).reduce(_ && _)
            Await.ready(store.deleteINode(absolutePath), atMost)
          }
        }
      case Failure(e) => {
        log.debug("failed to delete %s, as it doesn't exist", path.toUri.toString)
        result = false
      }
    }
    result
  }

  def rename(src: Path, dst: Path): Boolean = {
    if (src != dst) {
      val srcPath = makeAbsolute(src)
      val mayBeSrcINode = Try(Await.result(store.retrieveINode(srcPath), atMost))
      mayBeSrcINode match {
        case Failure(e1) => throw new IOException("No such file or directory.%s".format(srcPath))
        case Success(iNode: INode) =>
          val dstPath = makeAbsolute(dst)
          val mayBeDstINode = Try(Await.result(store.retrieveINode(dstPath), atMost))
          mayBeDstINode match {
            case Failure(e) => {
              val maybeDstParent = Try(Await.result(store.retrieveINode(dst.getParent), atMost))
              maybeDstParent match {
                case Failure(e2) =>
                  throw new IOException("Destination %s directory does not exist.".format(dst.getParent))
                case Success(dstParentINode: INode) => {
                  if (dstParentINode.isFile) {
                    throw new IOException("A file exists with parent of destination.")
                  }
                  if (iNode.isDirectory) {
                    renameDir(src, dst)
                  }
                  renameINode(srcPath, dstPath, iNode)
                }
              }
            }
            case Success(dstINode: INode) =>
              if (dstINode.isFile) {
                throw new IOException("A file %s already exists".format(dstPath))
              }
              else {
                var dstPathString = dstPath.toUri.getPath
                if (!dstPathString.endsWith("/")) {
                  dstPathString = dstPathString + "/"
                }
                val fileName = src.getName
                val updatedPath = new Path(dstPathString + fileName)

                val mayBeExistingINode = Try(Await.result(store.retrieveINode(updatedPath), atMost))

                mayBeExistingINode match {
                  case Failure(e) =>
                    if (iNode.isFile) {
                      renameINode(srcPath, updatedPath, iNode)
                    } else {
                      renameDir(srcPath, updatedPath)
                    }
                  case Success(existingINode: INode) =>
                    if (existingINode.isFile) {
                      if (iNode.isFile) {
                        renameINode(srcPath, updatedPath, iNode)
                      } else
                        throw new IOException("cannot overwrite non-directory with a directory")
                    } else {
                      if (iNode.isFile)
                        throw new IOException("cannot overwrite directory with a non-directory")
                      else {
                        val contents = Await.result(store.fetchSubPaths(updatedPath, isDeepFetch = false), atMost)
                        if (contents.size > 0)
                          throw new IOException("cannot move %s to %s - directory not empty".format(src, dst))
                        else
                          renameDir(srcPath, updatedPath)
                      }
                    }
                }
              }
          }
      }
    }
    true
  }

  def renameINode(originalPath: Path, updatedPath: Path, iNode: INode) = {
    Await.ready(store.deleteINode(originalPath), atMost)
    Await.ready(store.storeINode(updatedPath, iNode), atMost)
  }

  def renameDir(src: Path, dst: Path) = {
    val srcPath = makeAbsolute(src)
    mkdirs(dst)
    val contents = Await.result(store.fetchSubPaths(srcPath, isDeepFetch = true), atMost)
    if (contents.size > 0) {
      val srcPathString = src.toUri.getPath
      val dstPathString = dst.toUri.getPath
      contents.map(path => {
        val actualINode = Await.result(store.retrieveINode(makeQualified(path)), atMost)
        val oldPathString = path.toUri.getPath
        val changedPathString = oldPathString.replaceFirst(srcPathString, dstPathString)
        val changedPath = new Path(changedPathString)
        mkdirs(changedPath.getParent)
        renameINode(makeQualified(path), changedPath, actualINode)
      })
    }
  }

  def listStatus(path: Path): Array[FileStatus] = {
    var result: Array[FileStatus] = Array()
    val absolutePath = makeAbsolute(path)
    val mayBeiNode = Try(Await.result(store.retrieveINode(absolutePath), atMost))
    mayBeiNode match {
      case Success(iNode: INode) =>
        if (iNode.isFile) {
          val fileStatus = SnackFileStatus(iNode, absolutePath)
          result = Array(fileStatus)
        } else {
          val subPaths = Await.result(store.fetchSubPaths(absolutePath, isDeepFetch = false), atMost)
          result = subPaths.map(p => getFileStatus(makeQualified(p))).toArray
        }
      case Failure(e) => throw new FileNotFoundException("No such file exists")
    }
    result
  }

  def delete(p1: Path): Boolean = delete(p1, isRecursive = false)

  def getFileBlockLocations(path: Path, start: Long, len: Long): Array[BlockLocation] = {
    val blocks = Await.result(store.getBlockLocations(path), atMost)
    val locs = blocks.filterNot(x => x._1.offset + x._1.length < start)
    val locsMap = locs.map {
      case (b, ips) =>
        val bl = new BlockLocation()
        bl.setHosts(ips.toArray)
        bl.setNames(ips.map(i => "%s:%s".format(i, customConfiguration.CassandraPort)).toArray)
        bl.setOffset(b.offset)
        bl.setLength(b.length)
        bl
    }
    locsMap.toArray
  }

  override def getFileBlockLocations(file: FileStatus, start: Long, len: Long): Array[BlockLocation] = {
    getFileBlockLocations(file.getPath, start, len)
  }
}
