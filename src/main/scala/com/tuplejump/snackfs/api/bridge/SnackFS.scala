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
package com.tuplejump.snackfs.api.bridge

import java.net.URI
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.conf.Configuration
import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.hadoop.fs._
import com.twitter.logging.Logger
import java.util.UUID
import com.tuplejump.snackfs.api.model._
import com.tuplejump.snackfs.fs.model.BlockMeta
import com.tuplejump.snackfs.cassandra.store.ThriftStore
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration

case class SnackFS() extends FileSystem {

  private lazy val log = Logger.get(getClass)

  private var systemURI: URI = null
  private var currentDirectory: Path = null
  private var subBlockSize: Long = 0L

  private var atMost: FiniteDuration = null
  private var store: FileSystemStore = null
  private var customConfiguration: SnackFSConfiguration = _

  val processId = UUID.randomUUID()

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

    log.debug("creating base directory")
    mkdirs(new Path("/"))

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
    OpenFileCommand(store, path, bufferSize, atMost)
  }

  def mkdirs(path: Path, permission: FsPermission): Boolean = {
    val absolutePath = makeAbsolute(path)
    MakeDirectoryCommand(store, absolutePath, permission, atMost)
  }

  def create(filePath: Path, permission: FsPermission, overwrite: Boolean,
             bufferSize: Int, replication: Short, blockSize: Long,
             progress: Progressable): FSDataOutputStream = {

    CreateFileCommand(store, filePath, permission, overwrite, bufferSize, replication,
      blockSize, progress, processId, statistics, subBlockSize, atMost)
  }

  override def getDefaultBlockSize: Long = {
    customConfiguration.blockSize
  }

  def append(path: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    AppendFileCommand(store, path, bufferSize, progress, atMost)
  }

  def getFileStatus(path: Path): FileStatus = {
    FileStatusCommand(store, path, atMost)
  }

  def delete(path: Path, isRecursive: Boolean): Boolean = {
    val absolutePath = makeAbsolute(path)
    DeleteCommand(store, absolutePath, isRecursive, atMost)
  }

  def rename(src: Path, dst: Path): Boolean = {
    val srcPath = makeAbsolute(src)
    val dstPath = makeAbsolute(dst)
    RenameCommand(store, srcPath, dstPath, atMost)
  }


  def listStatus(path: Path): Array[FileStatus] = {
    val absolutePath = makeAbsolute(path)
    ListCommand(store, absolutePath, atMost)
  }

  def delete(p1: Path): Boolean = delete(p1, isRecursive = false)

  def getFileBlockLocations(path: Path, start: Long, len: Long): Array[BlockLocation] = {
    log.debug("fetching block locations for %s", path)
    val blocks: Map[BlockMeta, List[String]] = Await.result(store.getBlockLocations(path), atMost)
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
